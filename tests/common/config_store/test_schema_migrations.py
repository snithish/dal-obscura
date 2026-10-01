from __future__ import annotations

import pytest
from sqlalchemy import inspect, text

from dal_obscura.common.config_store.db import (
    ConfigStoreMigrationRequired,
    check_config_store_schema,
    create_engine_from_url,
    migrate_config_store,
)
from dal_obscura.common.config_store.orm import Base

REVISION = "20260930_0002"


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

    from dal_obscura.common.config_store.db import _alembic_config

    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    config = _alembic_config(engine)

    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.downgrade(config, "base")

    assert set(inspect(engine).get_table_names()) == {"alembic_version"}


def test_migration_accepts_percent_encoded_database_url(tmp_path):
    from dal_obscura.common.config_store.db import create_engine_from_url, migrate_config_store

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path}/config%25.db")
    try:
        migrate_config_store(engine)
    finally:
        engine.dispose()


def test_database_enforces_singleton_workspace_and_runtime():
    import pytest
    from sqlalchemy.exc import IntegrityError

    from dal_obscura.common.config_store.db import (
        create_engine_from_url,
        migrate_config_store,
        session_factory,
    )
    from dal_obscura.common.config_store.orm import RuntimeSettingsRecord, WorkspaceRecord

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

    from dal_obscura.common.config_store.db import create_engine_from_url, migrate_config_store

    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    with engine.connect() as connection:
        assert compare_metadata(MigrationContext.configure(connection), Base.metadata) == []


@pytest.mark.integration
def test_postgres_preserves_complete_nested_schema_types():
    from uuid import uuid4

    from dal_obscura.common.config_store.orm import (
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
