from __future__ import annotations

from uuid import uuid4

from sqlalchemy.orm import Session

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
)
from dal_obscura.common.config_store.identity_migration import (
    IdentityMigrationError,
    apply_identity_key_migration,
    inspect_identity_keys,
)
from dal_obscura.common.config_store.orm import (
    AssetOwnerRecord,
    AuthProviderRecord,
    PolicyRuleRecord,
)
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def _session() -> Session:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    return Session(engine)


def test_identity_migration_previews_and_applies_known_issuer_keys() -> None:
    with _session() as session:
        store = PublicationStore(session)
        context = store.ensure_default_workspace_context()
        store.upsert_catalog(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            name="analytics",
            module="iceberg",
            options={},
        )
        asset_id = store.upsert_asset(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            catalog="analytics",
            target="users",
            backend="iceberg",
            table_identifier="users",
            options={},
        )
        session.add(
            AuthProviderRecord(
                id=uuid4(),
                cell_id=context.cell_id,
                ordinal=1,
                module="oidc",
                args_json={"issuer": "https://issuer.example/realm/"},
                enabled=True,
            )
        )
        session.add(
            AssetOwnerRecord(
                id=uuid4(),
                asset_id=asset_id,
                ordinal=1,
                principal="https://issuer.example/realm|alice|one",
            )
        )
        session.add(
            PolicyRuleRecord(
                id=uuid4(),
                asset_id=asset_id,
                ordinal=10,
                effect="allow",
                principals_json=["https://issuer.example/realm|group:analysts|east"],
                when_json={},
                columns_json=["id"],
                masks_json={},
                row_filter_sql=None,
            )
        )
        session.commit()

        report = inspect_identity_keys(session)
        assert report.safe_to_apply
        assert report.converted == 2
        applied = apply_identity_key_migration(session)
        assert applied.converted == 2
        session.commit()

        owner = session.query(AssetOwnerRecord).first()
        policy_rule = session.query(PolicyRuleRecord).first()
        assert owner is not None
        assert policy_rule is not None
        assert owner.principal == ("https://issuer.example/realm/|u|alice%7Cone")
        assert policy_rule.principals_json == [
            "https://issuer.example/realm/|g|analysts%7Ceast"
        ]


def test_identity_migration_refuses_unknown_keys_without_mutation() -> None:
    with _session() as session:
        store = PublicationStore(session)
        context = store.ensure_default_workspace_context()
        store.upsert_catalog(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            name="analytics",
            module="iceberg",
            options={},
        )
        asset_id = store.upsert_asset(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            catalog="analytics",
            target="users",
            backend="iceberg",
            table_identifier="users",
            options={},
        )
        session.add(
            AssetOwnerRecord(
                id=uuid4(),
                asset_id=asset_id,
                ordinal=1,
                principal="unknown|legacy",
            )
        )
        session.commit()

        report = inspect_identity_keys(session)
        assert report.unresolved == ("unknown|legacy",)
        try:
            apply_identity_key_migration(session)
        except IdentityMigrationError:
            pass
        else:
            raise AssertionError("unknown identity keys must block migration")
        owner = session.query(AssetOwnerRecord).first()
        assert owner is not None
        assert owner.principal == "unknown|legacy"


def test_identity_migration_leaves_local_identity_keys_unchanged() -> None:
    with _session() as session:
        store = PublicationStore(session)
        context = store.ensure_default_workspace_context()
        store.upsert_catalog(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            name="analytics",
            module="iceberg",
            options={},
        )
        asset_id = store.upsert_asset(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            catalog="analytics",
            target="users",
            backend="iceberg",
            table_identifier="users",
            options={},
        )
        session.add_all(
            [
                AssetOwnerRecord(
                    id=uuid4(), asset_id=asset_id, ordinal=1, principal="local|operator"
                ),
                AssetOwnerRecord(
                    id=uuid4(), asset_id=asset_id, ordinal=2, principal="group:local|admins"
                ),
            ]
        )
        session.commit()

        report = inspect_identity_keys(session)
        assert report.safe_to_apply
        assert report.converted == 0


def test_identity_migration_is_idempotent_after_provider_removal() -> None:
    with _session() as session:
        store = PublicationStore(session)
        context = store.ensure_default_workspace_context()
        store.upsert_catalog(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            name="analytics",
            module="iceberg",
            options={},
        )
        asset_id = store.upsert_asset(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            catalog="analytics",
            target="users",
            backend="iceberg",
            table_identifier="users",
            options={},
        )
        session.add(
            AssetOwnerRecord(
                id=uuid4(),
                asset_id=asset_id,
                ordinal=1,
                principal="https://removed.example|u|alice",
            )
        )
        session.commit()

        report = inspect_identity_keys(session)
        assert report.safe_to_apply
        assert report.converted == 0


def test_identity_migration_converts_exact_issuer_escapes_and_rejects_legacy_escapes() -> None:
    with _session() as session:
        store = PublicationStore(session)
        context = store.ensure_default_workspace_context()
        store.upsert_catalog(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            name="analytics",
            module="iceberg",
            options={},
        )
        asset_id = store.upsert_asset(
            cell_id=context.cell_id,
            tenant_id=context.tenant_id,
            catalog="analytics",
            target="users",
            backend="iceberg",
            table_identifier="users",
            options={},
        )
        session.add(
            AuthProviderRecord(
                id=uuid4(),
                cell_id=context.cell_id,
                ordinal=1,
                module="oidc",
                args_json={"issuer": "https://issuer.example/realm/"},
                enabled=True,
            )
        )
        session.add_all(
            [
                AssetOwnerRecord(
                    id=uuid4(),
                    asset_id=asset_id,
                    ordinal=1,
                    principal="https://issuer.example/realm/|alice%7Cone",
                ),
                AssetOwnerRecord(
                    id=uuid4(),
                    asset_id=asset_id,
                    ordinal=2,
                    principal="https://issuer.example/realm|alice%7Cone",
                ),
            ]
        )
        session.commit()

        report = inspect_identity_keys(session)
        assert report.converted == 1
        assert report.ambiguous == ("https://issuer.example/realm|alice%7Cone",)
