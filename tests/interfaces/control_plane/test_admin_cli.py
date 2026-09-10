from __future__ import annotations

import json
from uuid import uuid4

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore
from dal_obscura.control_plane.interfaces.admin_cli import run


def test_status_reports_active_generation(tmp_path, capsys) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'control-plane.db'}"
    engine = create_engine_from_url(database_url)
    migrate_config_store(engine)
    cell_id = uuid4()
    tenant_id = uuid4()
    publication_id = uuid4()
    with session_factory(engine)() as session:
        store = PublicationStore(session)
        store.create_cell(cell_id=cell_id, name="default", region="local")
        store.create_tenant(tenant_id=tenant_id, slug="default", display_name="Default")
        store.assign_tenant_to_cell(cell_id=cell_id, tenant_id=tenant_id, shard_key="default")
        store.insert_publication(
            cell_id=cell_id,
            publication_id=publication_id,
            manifest_hash="a" * 64,
        )
        store.activate_publication(cell_id=cell_id, publication_id=publication_id)
        session.commit()

    assert run(["status", "--database-url", database_url]) == 0

    assert json.loads(capsys.readouterr().out) == {
        "active_publication": {
            "cell_id": str(cell_id),
            "manifest_hash": "a" * 64,
            "publication_id": str(publication_id),
            "status": "published",
        },
        "workspace": {"cell_id": str(cell_id), "tenant_id": str(tenant_id)},
    }


def test_status_reports_unconfigured_store(tmp_path, capsys) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'control-plane.db'}"
    engine = create_engine_from_url(database_url)
    migrate_config_store(engine)

    assert run(["status", "--database-url", database_url]) == 0

    assert json.loads(capsys.readouterr().out) == {
        "active_publication": None,
        "workspace": None,
    }


def test_validate_compiles_operator_manifest(tmp_path, capsys) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "runtime": {
                    "ticket_ttl_seconds": 300,
                    "max_tickets": 16,
                    "max_ticket_exchanges": 1,
                },
                "auth_providers": [{"args": {"issuer": "https://issuer.example"}}],
                "catalogs": [
                    {
                        "name": "analytics",
                        "options": {"type": "sql", "uri": "sqlite:///warehouse.db"},
                    }
                ],
                "assets": [
                    {
                        "catalog": "analytics",
                        "target": "default.users",
                        "table_identifier": "prod.users",
                        "rules": [{"principals": ["group:analyst"], "columns": ["id"]}],
                    }
                ],
            }
        )
    )

    assert run(["validate", str(path)]) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["asset_count"] == 1
    assert output["catalog_count"] == 1
    assert output["manifest_hash"]
