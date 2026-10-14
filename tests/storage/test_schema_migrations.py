from __future__ import annotations

import pytest
from sqlalchemy import inspect, text

from dal_obscura.storage.database.db import (
    ConfigStoreMigrationRequired,
    check_config_store_schema,
    migrate_config_store,
)
from dal_obscura.storage.database.orm import Base

REVISION = "20261011_0001"


def test_check_config_store_schema_fails_without_mutating_empty_database(engine_factory) -> None:
    engine = engine_factory("sqlite+pysqlite:///:memory:")

    with pytest.raises(ConfigStoreMigrationRequired, match="dal-obscura-migrate upgrade"):
        check_config_store_schema(engine)

    assert inspect(engine).get_table_names() == []


def test_baseline_creates_only_the_current_live_schema(engine_factory) -> None:
    engine = engine_factory("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    assert set(inspect(engine).get_table_names()) == set(Base.metadata.tables) | {"alembic_version"}
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == REVISION


def test_migration_and_schema_check_are_idempotent(engine_factory) -> None:
    engine = engine_factory("sqlite+pysqlite:///:memory:")

    migrate_config_store(engine)
    migrate_config_store(engine)

    check_config_store_schema(engine)
    with engine.connect() as connection:
        version = connection.scalar(text("SELECT version_num FROM alembic_version"))
    assert version == REVISION


def test_explicit_downgrade_drops_the_config_store(engine_factory) -> None:
    from alembic import command

    from dal_obscura.storage.database.db import _alembic_config

    engine = engine_factory("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    config = _alembic_config(engine)

    with engine.begin() as connection:
        config.attributes["connection"] = connection
        command.downgrade(config, "base")

    assert set(inspect(engine).get_table_names()) == {"alembic_version"}


def test_migration_accepts_percent_encoded_database_url(engine_factory, tmp_path):
    from dal_obscura.storage.database.db import migrate_config_store

    engine = engine_factory(f"sqlite+pysqlite:///{tmp_path}/config%25.db")
    try:
        migrate_config_store(engine)
    finally:
        engine.dispose()


def test_database_enforces_singleton_workspace_and_runtime(engine_factory):
    import pytest
    from sqlalchemy.exc import IntegrityError

    from dal_obscura.storage.database.db import (
        migrate_config_store,
        session_factory,
    )
    from dal_obscura.storage.database.orm import RuntimeSettingsRecord, WorkspaceRecord

    engine = engine_factory("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    with session_factory(engine)() as session:
        session.add(WorkspaceRecord(id=2))
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()
        session.add(RuntimeSettingsRecord(id=2, ticket_ttl_seconds=300, max_tickets=1))
        with pytest.raises(IntegrityError):
            session.flush()


def test_baseline_matches_orm_columns_constraints_and_indexes(engine_factory):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    from dal_obscura.storage.database.db import migrate_config_store

    engine = engine_factory("sqlite+pysqlite:///:memory:")
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
    from tests.support.storage.postgres import isolated_postgres_sessions

    nested_type = "struct<" + ", ".join(f"field_{i}: large_string" for i in range(40)) + ">"
    with isolated_postgres_sessions() as factory:
        catalog_id, asset_id, field_id = uuid4(), uuid4(), uuid4()
        with factory() as session:
            session.add(CatalogRecord(id=catalog_id, name="nested", plugin_id="iceberg.catalog"))
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


def test_release_has_one_create_only_baseline(engine_factory):
    from alembic.script import ScriptDirectory

    from dal_obscura.storage.database.db import _alembic_config

    engine = engine_factory("sqlite+pysqlite:///:memory:")
    revisions = list(ScriptDirectory.from_config(_alembic_config(engine)).walk_revisions())
    assert [revision.revision for revision in revisions] == [REVISION]
    assert revisions[0].down_revision is None


@pytest.mark.integration
def test_postgres_admits_long_incompressible_names_and_distinct_leaf_paths():
    import random
    import string

    from fastapi.testclient import TestClient

    from dal_obscura.interfaces.http.app import create_app
    from tests.support.control.workspace import ADMIN_HEADERS, _provision_asset
    from tests.support.storage.postgres import isolated_postgres_sessions

    name = "".join(random.Random(71).choices(string.ascii_letters + string.digits, k=16384))
    fields = [
        {"name": name, "field_id": "1", "path": [name], "type": "string", "nullable": True},
        {
            "name": "value",
            "field_id": "2",
            "path": ["left", "value"],
            "type": "string",
            "nullable": True,
        },
        {
            "name": "value",
            "field_id": "3",
            "path": ["right", "value"],
            "type": "string",
            "nullable": True,
        },
    ]
    with (
        isolated_postgres_sessions() as factory,
        TestClient(create_app(factory, admin_token="test-admin")) as client,
    ):
        asset = _provision_asset(client)
        saved = client.put(
            f"/v1/assets/{asset['id']}/schema-fields",
            headers=ADMIN_HEADERS,
            json={"fields": fields, "expected_revision": 0},
        )
        assert saved.status_code == 200, saved.text
        detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()
        assert detail["schema_fields"] == [
            {**field, "field_id": f"iceberg:{field['field_id']}"} for field in fields
        ]


def test_earlier_baseline_requires_fresh_database_without_mutating_it(engine_factory):
    engine = engine_factory("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    with engine.begin() as connection:
        connection.execute(text("UPDATE alembic_version SET version_num = '20261003_0001'"))
    tables = inspect(engine).get_table_names()
    with pytest.raises(
        ConfigStoreMigrationRequired, match="Earlier database baselines are unsupported"
    ):
        check_config_store_schema(engine)
    assert inspect(engine).get_table_names() == tables
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "20261003_0001"
