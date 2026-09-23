from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import inspect, text

from dal_obscura.common.config_store.db import (
    ConfigStoreMigrationRequired,
    check_config_store_schema,
    create_engine_from_url,
    migrate_config_store,
)
from dal_obscura.common.config_store.orm import Base

MIGRATIONS_DIR = (
    Path(__file__).parents[3]
    / "src"
    / "dal_obscura"
    / "common"
    / "config_store"
    / "migrations"
    / "versions"
)
MIGRATION_FILE = MIGRATIONS_DIR / "20260923_0001_live_configuration.py"
REVISION = "20260923_0001"


def test_history_is_a_single_frozen_baseline() -> None:
    migration_source = MIGRATION_FILE.read_text()
    revisions = {path.name for path in MIGRATIONS_DIR.glob("*.py")} - {"__init__.py"}

    assert revisions == {MIGRATION_FILE.name}
    assert f'revision = "{REVISION}"' in migration_source
    assert "down_revision = None" in migration_source
    assert "common.config_store.orm" not in migration_source
    assert ".metadata.create_all" not in migration_source
    assert ".metadata.drop_all" not in migration_source
    assert "asset_policy_drafts" not in migration_source
    assert "config_publications" not in migration_source
    assert "published_assets" not in migration_source


def test_check_config_store_schema_fails_without_mutating_empty_database() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")

    with pytest.raises(ConfigStoreMigrationRequired, match="dal-obscura-migrate upgrade"):
        check_config_store_schema(engine)

    assert inspect(engine).get_table_names() == []


def test_baseline_creates_only_the_current_live_schema() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")

    migrate_config_store(engine)

    tables = set(inspect(engine).get_table_names())
    assert tables == set(Base.metadata.tables) | {"alembic_version"}
    assert not {
        "asset_policy_drafts",
        "publication_operations",
        "config_publications",
        "active_publications",
        "published_cell_runtime",
        "published_catalogs",
        "published_assets",
    }.intersection(tables)

    inspector = inspect(engine)
    assert {column["name"] for column in inspector.get_columns("data_plane_tickets")} >= {
        "asset_id",
        "revoked_at",
    }
    assert "revision" in {column["name"] for column in inspector.get_columns("catalogs")}
    assert "policy_revision" in {column["name"] for column in inspector.get_columns("assets")}
    with engine.connect() as connection:
        version = connection.scalar(text("SELECT version_num FROM alembic_version"))
    assert version == REVISION


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
