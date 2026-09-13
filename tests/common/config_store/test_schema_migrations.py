from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import inspect, text

from dal_obscura.common.config_store.db import (
    ConfigStoreMigrationRequired,
    check_config_store_schema,
    create_engine_from_url,
    migrate_config_store,
)

MIGRATION_FILE = (
    Path(__file__).parents[3]
    / "src"
    / "dal_obscura"
    / "common"
    / "config_store"
    / "migrations"
    / "versions"
    / "20260626_0001_initial_config_store.py"
)


def test_initial_config_store_revision_uses_frozen_ddl() -> None:
    migration_source = MIGRATION_FILE.read_text()

    assert "common.config_store.orm" not in migration_source
    assert ".metadata.create_all" not in migration_source
    assert ".metadata.drop_all" not in migration_source


def test_check_config_store_schema_fails_without_mutating_empty_database() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")

    with pytest.raises(ConfigStoreMigrationRequired, match="dal-obscura-migrate upgrade"):
        check_config_store_schema(engine)

    assert inspect(engine).get_table_names() == []


def test_migrate_config_store_creates_current_schema_from_empty_database() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")

    migrate_config_store(engine)

    inspector = inspect(engine)
    assert "alembic_version" in inspector.get_table_names()
    assert "data_plane_tickets" in inspector.get_table_names()
    runtime_columns = {column["name"] for column in inspector.get_columns("cell_runtime_settings")}
    assert "max_ticket_exchanges" in runtime_columns
    catalog_columns = {column["name"] for column in inspector.get_columns("catalogs")}
    assert "revision" in catalog_columns


def test_check_config_store_schema_passes_after_explicit_migration() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)

    check_config_store_schema(engine)


def test_migrate_config_store_is_idempotent() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")

    migrate_config_store(engine)
    migrate_config_store(engine)

    with engine.connect() as connection:
        version = connection.scalar(text("SELECT version_num FROM alembic_version"))

    assert version == "20260913_0017"


def test_migrate_config_store_upgrades_legacy_runtime_settings_column() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE tenants (id CHAR(32) PRIMARY KEY)"))
        connection.execute(
            text(
                "CREATE TABLE cells ("
                "id CHAR(32) PRIMARY KEY, "
                "name VARCHAR(120) NOT NULL, "
                "region VARCHAR(64) NOT NULL, "
                "status VARCHAR(24) NOT NULL"
                ")"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE cell_runtime_settings ("
                "cell_id CHAR(32) PRIMARY KEY, "
                "ticket_ttl_seconds INTEGER NOT NULL, "
                "max_tickets INTEGER NOT NULL, "
                "path_rules_json JSON NOT NULL"
                ")"
            )
        )
        connection.execute(
            text(
                "INSERT INTO cell_runtime_settings "
                "(cell_id, ticket_ttl_seconds, max_tickets, path_rules_json) "
                "VALUES ('00000000000000000000000000000001', 900, 64, '[]')"
            )
        )

    migrate_config_store(engine)

    inspector = inspect(engine)
    runtime_columns = {column["name"] for column in inspector.get_columns("cell_runtime_settings")}
    assert "max_ticket_exchanges" in runtime_columns
    with engine.connect() as connection:
        value = connection.scalar(text("SELECT max_ticket_exchanges FROM cell_runtime_settings"))
    assert value == 1


def test_schema_identity_migration_backfills_legacy_rows() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine, "20260912_0010")
    tenant = "00000000-0000-0000-0000-000000000001"
    cell = "00000000-0000-0000-0000-000000000002"
    catalog = "00000000-0000-0000-0000-000000000003"
    asset = "00000000-0000-0000-0000-000000000004"
    field = "00000000-0000-0000-0000-000000000005"
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO tenants (id, slug, display_name, status) "
                "VALUES (:id, 'tenant', 'Tenant', 'active')"
            ),
            {"id": tenant},
        )
        connection.execute(
            text(
                "INSERT INTO cells (id, name, region, status) "
                "VALUES (:id, 'cell', 'local', 'active')"
            ),
            {"id": cell},
        )
        connection.execute(
            text(
                "INSERT INTO catalogs (id, cell_id, tenant_id, name, module, options_json) "
                "VALUES (:id, :cell, :tenant, 'analytics', 'IcebergCatalog', '{}')"
            ),
            {"id": catalog, "cell": cell, "tenant": tenant},
        )
        connection.execute(
            text(
                "INSERT INTO assets (id, cell_id, tenant_id, catalog_id, target, backend, "
                "table_identifier, options_json) VALUES (:id, :cell, :tenant, :catalog, "
                "'users', 'iceberg', 'prod.users', '{}')"
            ),
            {"id": asset, "cell": cell, "tenant": tenant, "catalog": catalog},
        )
        connection.execute(
            text(
                "INSERT INTO asset_schema_fields (id, asset_id, ordinal, name, type, nullable) "
                "VALUES (:id, :asset, 1, 'profile.email', 'string', 1)"
            ),
            {"id": field, "asset": asset},
        )

    migrate_config_store(engine)

    with engine.connect() as connection:
        row = connection.execute(
            text("SELECT field_id, path_json FROM asset_schema_fields WHERE id = :id"),
            {"id": field},
        ).mappings().one()
    assert json.loads(row["path_json"]) == ["profile.email"]
    assert str(row["field_id"]).startswith("legacy:")
