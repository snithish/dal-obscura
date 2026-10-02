from __future__ import annotations

import pytest
from sqlalchemy import inspect, text

from dal_obscura.storage.database.db import (
    ConfigStoreMigrationRequired,
    check_config_store_schema,
    create_engine_from_url,
    migrate_config_store,
)
from dal_obscura.storage.database.orm import Base

REVISION = "20261002_0003"


def test_check_config_store_schema_fails_without_mutating_empty_database() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")

    with pytest.raises(ConfigStoreMigrationRequired, match="dal-obscura-migrate upgrade"):
        check_config_store_schema(engine)

    assert inspect(engine).get_table_names() == []


def test_baseline_creates_only_the_current_live_schema() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    assert set(inspect(engine).get_table_names()) == set(Base.metadata.tables) | {"alembic_version"}
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == REVISION


def test_migration_and_schema_check_are_idempotent() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")

    migrate_config_store(engine)
    migrate_config_store(engine)

    check_config_store_schema(engine)
    with engine.connect() as connection:
        version = connection.scalar(text("SELECT version_num FROM alembic_version"))
    assert version == REVISION


def test_explicit_downgrade_drops_the_config_store() -> None:
    from alembic import command

    from dal_obscura.storage.database.db import _alembic_config

    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    config = _alembic_config(engine)

    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.downgrade(config, "base")

    assert set(inspect(engine).get_table_names()) == {"alembic_version"}


def test_migration_accepts_percent_encoded_database_url(tmp_path):
    from dal_obscura.storage.database.db import create_engine_from_url, migrate_config_store

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path}/config%25.db")
    try:
        migrate_config_store(engine)
    finally:
        engine.dispose()


def test_database_enforces_singleton_workspace_and_runtime():
    import pytest
    from sqlalchemy.exc import IntegrityError

    from dal_obscura.storage.database.db import (
        create_engine_from_url,
        migrate_config_store,
        session_factory,
    )
    from dal_obscura.storage.database.orm import RuntimeSettingsRecord, WorkspaceRecord

    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    with session_factory(engine)() as session:
        session.add(WorkspaceRecord(id=2))
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()
        session.add(RuntimeSettingsRecord(id=2, ticket_ttl_seconds=300, max_tickets=1))
        with pytest.raises(IntegrityError):
            session.flush()


def test_baseline_matches_orm_columns_constraints_and_indexes():
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    from dal_obscura.storage.database.db import create_engine_from_url, migrate_config_store

    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    with engine.connect() as connection:
        assert compare_metadata(MigrationContext.configure(connection), Base.metadata) == []


@pytest.mark.integration
def test_postgres_preserves_complete_nested_schema_types():
    from uuid import uuid4

    from dal_obscura.storage.database.orm import (
        AssetRecord,
        AssetSchemaFieldRecord,
        CatalogRecord,
    )
    from tests.support.postgres import isolated_postgres_sessions

    nested_type = "struct<" + ", ".join(f"field_{i}: large_string" for i in range(40)) + ">"
    with isolated_postgres_sessions() as factory:
        catalog_id, asset_id, field_id = uuid4(), uuid4(), uuid4()
        with factory() as session:
            session.add(CatalogRecord(id=catalog_id, name="nested", plugin_id="iceberg.sql"))
            session.flush()
            session.add(
                AssetRecord(id=asset_id, catalog_id=catalog_id, target="nested", backend="iceberg")
            )
            session.flush()
            session.add(
                AssetSchemaFieldRecord(
                    id=field_id,
                    asset_id=asset_id,
                    ordinal=0,
                    name="profile",
                    field_id="1",
                    path_json=["profile"],
                    type=nested_type,
                    nullable=True,
                )
            )
            session.commit()
        with factory() as session:
            stored = session.get(AssetSchemaFieldRecord, field_id)
            assert stored is not None
            assert stored.type == nested_type


def test_core_cutover_preserves_configuration_and_audit_but_invalidates_old_tickets():
    from uuid import uuid4

    from sqlalchemy import select
    from sqlalchemy.orm import Session

    from dal_obscura.storage.database.orm import (
        AssetOwnerRecord,
        AssetRecord,
        AuditEventRecord,
        AuthProviderRecord,
        CatalogRecord,
        DataPlaneTicketRecord,
        PolicyRuleRecord,
        utcnow,
    )
    from tests.support.live_config import seed_live_config

    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine, "20260930_0002")
    with Session(engine) as session:
        (asset_id,) = seed_live_config(session)
        provider = session.scalar(select(AuthProviderRecord))
        assert provider is not None
        provider.module = (
            "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks."
            "OidcJwksIdentityProvider"
        )
        session.add(
            AssetOwnerRecord(
                id=uuid4(), asset_id=asset_id, ordinal=0, principal="group:asset-owners"
            )
        )
        audit_id = uuid4()
        session.add(
            AuditEventRecord(
                id=audit_id,
                actor_principal="owner",
                action="policy.update",
                resource_type="asset",
                resource_id=str(asset_id),
                outcome="success",
                details_json={"revision": 4},
                created_at=utcnow(),
            )
        )
        session.add(
            DataPlaneTicketRecord(
                ticket_id=uuid4(),
                asset_id=asset_id,
                catalog="analytics",
                target="default.users",
                principal_id="user1",
                policy_version=4,
                expires_at=9999999999,
                max_exchanges=1,
                exchange_count=0,
                payload_json={"old": "pickle"},
                payload_hash="old",
            )
        )
        session.commit()
        before_asset = session.get(AssetRecord, asset_id)
        assert before_asset is not None
        preserved_asset = (before_asset.revision, before_asset.policy_revision, before_asset.target)
        before_catalog = session.scalar(select(CatalogRecord))
        assert before_catalog is not None
        preserved_catalog = (before_catalog.revision, before_catalog.options_json)
        before_rule = session.scalar(select(PolicyRuleRecord))
        assert before_rule is not None
        preserved_policy = (
            before_rule.id,
            before_rule.principals_json,
            before_rule.columns_json,
            before_rule.masks_json,
        )
    migrate_config_store(engine)
    with Session(engine) as session:
        audit = session.get(AuditEventRecord, audit_id)
        assert audit is not None and audit.details_json == {"revision": 4}
        provider = session.scalar(select(AuthProviderRecord))
        assert (
            provider is not None
            and provider.module == "dal_obscura.identity.oidc.OidcJwksIdentityProvider"
        )
        assert session.scalar(select(DataPlaneTicketRecord)) is None
        asset = session.get(AssetRecord, asset_id)
        catalog = session.scalar(select(CatalogRecord))
        rule = session.scalar(select(PolicyRuleRecord))
        owner = session.scalar(select(AssetOwnerRecord))
        assert (
            asset is not None
            and (asset.revision, asset.policy_revision, asset.target) == preserved_asset
        )
        assert catalog is not None and (catalog.revision, catalog.options_json) == preserved_catalog
        assert (
            rule is not None
            and (rule.id, rule.principals_json, rule.columns_json, rule.masks_json)
            == preserved_policy
        )
        assert owner is not None and owner.principal == "group:asset-owners"
