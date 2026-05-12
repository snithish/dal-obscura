from __future__ import annotations

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.interfaces.api import create_app

ADMIN_HEADERS = {"authorization": "Bearer test-admin"}


def _client() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return TestClient(create_app(session_factory(engine), admin_token="test-admin"))


def _create_tenant_and_cell(client: TestClient) -> tuple[dict[str, str], dict[str, str]]:
    tenant = client.post(
        "/v1/tenants",
        json={"slug": "default", "display_name": "Default"},
        headers=ADMIN_HEADERS,
    ).json()
    cell = client.post(
        "/v1/cells",
        json={"name": "default", "region": "local"},
        headers=ADMIN_HEADERS,
    ).json()
    client.put(
        f"/v1/cells/{cell['id']}/tenants/{tenant['id']}",
        json={"shard_key": "default"},
        headers=ADMIN_HEADERS,
    )
    return tenant, cell


def test_inventory_reads_require_admin_token():
    client = _client()

    assert client.get("/v1/tenants").status_code == 401
    assert client.get("/v1/cells").status_code == 401
    assert client.get("/v1/cell-tenant-assignments").status_code == 401


def test_lists_tenants_cells_and_assignments_after_writes():
    client = _client()
    tenant, cell = _create_tenant_and_cell(client)

    tenants = client.get("/v1/tenants", headers=ADMIN_HEADERS).json()
    cells = client.get("/v1/cells", headers=ADMIN_HEADERS).json()
    assignments = client.get("/v1/cell-tenant-assignments", headers=ADMIN_HEADERS).json()

    assert tenants == [
        {
            "id": tenant["id"],
            "slug": "default",
            "display_name": "Default",
            "status": "active",
        }
    ]
    assert cells == [
        {
            "id": cell["id"],
            "name": "default",
            "region": "local",
            "status": "active",
        }
    ]
    assert assignments == [
        {
            "cell_id": cell["id"],
            "tenant_id": tenant["id"],
            "shard_key": "default",
        }
    ]
