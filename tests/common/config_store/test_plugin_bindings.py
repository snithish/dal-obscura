from __future__ import annotations

import json
from uuid import uuid4

from dal_obscura.common.config_store.cli import run
from dal_obscura.common.config_store.db import create_engine_from_url, migrate_config_store
from dal_obscura.common.config_store.orm import PublishedAssetRecord, PublishedCatalogRecord


def test_plugin_binding_migration_dry_run_and_apply_are_explicit(tmp_path, capsys) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'config.db'}"
    engine = create_engine_from_url(database_url)
    migrate_config_store(engine)
    publication_id = uuid4()
    tenant_id = uuid4()
    with engine.begin() as connection:
        from sqlalchemy.orm import Session

        session = Session(bind=connection)
        session.add(
            PublishedCatalogRecord(
                publication_id=publication_id,
                tenant_id=tenant_id,
                catalog="analytics",
                plugin_id=None,
                plugin_revision=None,
                config_json={
                    "module": (
                        "dal_obscura.data_plane.infrastructure.adapters.catalog_registry."
                        "IcebergCatalog"
                    )
                },
            )
        )
        session.add(
            PublishedAssetRecord(
                publication_id=publication_id,
                tenant_id=tenant_id,
                catalog="analytics",
                target="default.users:partition",
                backend="iceberg",
                catalog_plugin_id=None,
                format_plugin_id=None,
                plugin_revision=None,
                compiled_config_json={
                    "catalog": {
                        "module": (
                            "dal_obscura.data_plane.infrastructure.adapters.catalog_registry."
                            "IcebergCatalog"
                        )
                    }
                },
                policy_version=1,
            )
        )
        session.add(
            PublishedAssetRecord(
                publication_id=publication_id,
                tenant_id=tenant_id,
                catalog="analytics",
                target="unknown",
                backend="other",
                catalog_plugin_id=None,
                format_plugin_id=None,
                plugin_revision=None,
                compiled_config_json={},
                policy_version=1,
            )
        )
        session.commit()

    assert run(["plugin-bindings", "--database-url", database_url]) == 0
    dry_run = json.loads(capsys.readouterr().out)
    assert dry_run["migratable"] == 2
    assert dry_run["unsupported"] == 1
    assert dry_run["applied"] == 0

    assert run(["plugin-bindings", "--database-url", database_url, "--apply"]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert applied["applied"] == 2

    with engine.connect() as connection:
        from sqlalchemy.orm import Session

        session = Session(bind=connection)
        rows = session.query(PublishedAssetRecord).all()
        assert {row.target: row.format_plugin_id for row in rows} == {
            "default.users:partition": "iceberg",
            "unknown": None,
        }

        catalog = session.get(
            PublishedCatalogRecord,
            {
                "publication_id": publication_id,
                "tenant_id": tenant_id,
                "catalog": "analytics",
            },
        )
        assert catalog is not None
        assert catalog.config_json == {"type": "iceberg", "options": {}}

    assert run(["plugin-bindings", "--database-url", database_url]) == 0
    rerun = json.loads(capsys.readouterr().out)
    assert rerun["migratable"] == 0
    assert rerun["applied"] == 0
