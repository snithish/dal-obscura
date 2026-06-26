from __future__ import annotations

from sqlalchemy import inspect, text

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    ensure_config_store_schema,
)


def test_schema_migrations_create_current_schema_from_empty_database() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")

    ensure_config_store_schema(engine)

    inspector = inspect(engine)
    assert "alembic_version" in inspector.get_table_names()
    assert "data_plane_tickets" in inspector.get_table_names()
    runtime_columns = {column["name"] for column in inspector.get_columns("cell_runtime_settings")}
    assert "max_ticket_exchanges" in runtime_columns


def test_schema_migrations_are_idempotent() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")

    ensure_config_store_schema(engine)
    ensure_config_store_schema(engine)

    with engine.connect() as connection:
        version = connection.scalar(text("SELECT version_num FROM alembic_version"))

    assert version == "20260626_0001"


def test_schema_migrations_upgrade_legacy_runtime_settings_column() -> None:
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

    ensure_config_store_schema(engine)

    inspector = inspect(engine)
    runtime_columns = {column["name"] for column in inspector.get_columns("cell_runtime_settings")}
    assert "max_ticket_exchanges" in runtime_columns
    with engine.connect() as connection:
        value = connection.scalar(text("SELECT max_ticket_exchanges FROM cell_runtime_settings"))
    assert value == 1
