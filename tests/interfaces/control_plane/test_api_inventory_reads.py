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


def test_lists_cells_for_one_tenant_only():
    client = _client()
    tenant_one, cell_one = _create_tenant_and_cell(client)
    tenant_two = client.post(
        "/v1/tenants",
        json={"slug": "other", "display_name": "Other"},
        headers=ADMIN_HEADERS,
    ).json()
    cell_two = client.post(
        "/v1/cells",
        json={"name": "other", "region": "local"},
        headers=ADMIN_HEADERS,
    ).json()
    client.put(
        f"/v1/cells/{cell_two['id']}/tenants/{tenant_two['id']}",
        json={"shard_key": "other"},
        headers=ADMIN_HEADERS,
    )

    cells = client.get(
        f"/v1/tenants/{tenant_one['id']}/cells",
        headers=ADMIN_HEADERS,
    ).json()

    assert cells == [
        {
            "id": cell_one["id"],
            "name": "default",
            "region": "local",
            "status": "active",
            "shard_key": "default",
        }
    ]


def test_can_create_cell_directly_for_tenant_from_form_post():
    client = _client()
    tenant = client.post(
        "/v1/tenants",
        json={"slug": "default", "display_name": "Default"},
        headers=ADMIN_HEADERS,
    ).json()

    response = client.post(
        f"/v1/tenants/{tenant['id']}/cells",
        data={"name": "tenant-cell", "region": "local", "shard_key": "default"},
        headers={**ADMIN_HEADERS, "HX-Request": "true"},
    )

    assert response.status_code == 200
    assert "tenant-cell" in response.text
    assert 'data-set-context="cell_id"' in response.text


def test_can_assign_existing_cell_to_tenant_from_form_post():
    client = _client()
    tenant = client.post(
        "/v1/tenants",
        json={"slug": "default", "display_name": "Default"},
        headers=ADMIN_HEADERS,
    ).json()
    cell = client.post(
        "/v1/cells",
        json={"name": "existing", "region": "local"},
        headers=ADMIN_HEADERS,
    ).json()

    response = client.post(
        f"/v1/tenants/{tenant['id']}/cell-assignments",
        data={"cell_id": cell["id"], "shard_key": "default"},
        headers={**ADMIN_HEADERS, "HX-Request": "true"},
    )

    assert response.status_code == 200
    assert "existing" in response.text
    assert 'data-set-context="cell_id"' in response.text


ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
DEFAULT_AUTH_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_default.DefaultIdentityAdapter"
)


def _provision_draft(client: TestClient) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    tenant, cell = _create_tenant_and_cell(client)
    client.put(
        f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/runtime-settings",
        json={
            "ticket_ttl_seconds": 900,
            "max_tickets": 64,
            "max_ticket_exchanges": 2,
            "path_rules": [{"glob": "s3://warehouse/*", "allow": True}],
        },
        headers=ADMIN_HEADERS,
    )
    client.put(
        f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    asset = client.put(
        f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {"snapshot": 1}},
        headers=ADMIN_HEADERS,
    ).json()
    client.put(
        f"/v1/assets/{asset['id']}/policy-rules",
        json={
            "rules": [
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["user1"],
                    "when": {"tenant": "default"},
                    "columns": ["id", "email"],
                    "masks": {"email": {"type": "email"}},
                    "row_filter": "region = 'us'",
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    client.put(
        f"/v1/cells/{cell['id']}/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": DEFAULT_AUTH_MODULE,
                    "args": {"jwt_secret": {"secret": "DAL_OBSCURA_JWT_SECRET"}},
                    "enabled": True,
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    return tenant, cell, asset


def test_reads_cell_draft_resources_after_writes():
    client = _client()
    tenant, cell, asset = _provision_draft(client)

    runtime = client.get(
        f"/v1/cells/{cell['id']}/runtime-settings",
        headers=ADMIN_HEADERS,
    ).json()
    catalogs = client.get(f"/v1/cells/{cell['id']}/catalogs", headers=ADMIN_HEADERS).json()
    assets = client.get(f"/v1/cells/{cell['id']}/assets", headers=ADMIN_HEADERS).json()
    rules = client.get(f"/v1/assets/{asset['id']}/policy-rules", headers=ADMIN_HEADERS).json()
    auth = client.get(f"/v1/cells/{cell['id']}/auth-providers", headers=ADMIN_HEADERS).json()
    draft = client.get(f"/v1/cells/{cell['id']}/draft", headers=ADMIN_HEADERS).json()

    assert runtime == {
        "cell_id": cell["id"],
        "ticket_ttl_seconds": 900,
        "max_tickets": 64,
        "max_ticket_exchanges": 2,
        "path_rules": [{"glob": "s3://warehouse/*", "allow": True}],
    }
    assert catalogs[0]["tenant_id"] == tenant["id"]
    assert catalogs[0]["name"] == "analytics"
    assert catalogs[0]["module"] == ICEBERG_CATALOG_MODULE
    assert catalogs[0]["options"] == {"type": "sql", "uri": "sqlite:///catalog.db"}
    assert assets[0]["id"] == asset["id"]
    assert assets[0]["catalog"] == "analytics"
    assert assets[0]["target"] == "default.users"
    assert assets[0]["options"] == {"snapshot": 1}
    assert rules == [
        {
            "id": rules[0]["id"],
            "asset_id": asset["id"],
            "ordinal": 10,
            "effect": "allow",
            "principals": ["user1"],
            "when": {"tenant": "default"},
            "columns": ["id", "email"],
            "masks": {"email": {"type": "email"}},
            "row_filter": "region = 'us'",
        }
    ]
    assert auth == [
        {
            "id": auth[0]["id"],
            "cell_id": cell["id"],
            "ordinal": 1,
            "module": DEFAULT_AUTH_MODULE,
            "args": {"jwt_secret": {"secret": "DAL_OBSCURA_JWT_SECRET"}},
            "enabled": True,
        }
    ]
    assert draft["cell"] == {
        "id": cell["id"],
        "name": "default",
        "region": "local",
        "status": "active",
    }
    assert draft["runtime_settings"] == runtime
    assert draft["catalogs"] == catalogs
    assert draft["assets"][0]["policy_rules"] == rules
    assert draft["auth_providers"] == auth


def test_unknown_cell_draft_returns_404():
    client = _client()

    response = client.get(
        "/v1/cells/00000000-0000-0000-0000-000000000001/draft",
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "No cell 00000000-0000-0000-0000-000000000001"


def test_reads_publications_and_active_publication():
    client = _client()
    _, cell, _ = _provision_draft(client)

    assert (
        client.get(
            f"/v1/cells/{cell['id']}/active-publication",
            headers=ADMIN_HEADERS,
        ).status_code
        == 404
    )

    publication = client.post(
        f"/v1/cells/{cell['id']}/publications",
        headers=ADMIN_HEADERS,
    ).json()
    publications_before_activation = client.get(
        f"/v1/cells/{cell['id']}/publications",
        headers=ADMIN_HEADERS,
    ).json()

    assert publications_before_activation == [
        {
            "id": publication["publication_id"],
            "cell_id": cell["id"],
            "schema_version": 1,
            "status": "published",
            "manifest_hash": publication["manifest_hash"],
            "active": False,
        }
    ]

    client.post(
        f"/v1/cells/{cell['id']}/publications/{publication['publication_id']}/activate",
        headers=ADMIN_HEADERS,
    )
    active = client.get(
        f"/v1/cells/{cell['id']}/active-publication",
        headers=ADMIN_HEADERS,
    ).json()
    publications_after_activation = client.get(
        f"/v1/cells/{cell['id']}/publications",
        headers=ADMIN_HEADERS,
    ).json()

    assert active == {
        "cell_id": cell["id"],
        "publication_id": publication["publication_id"],
        "manifest_hash": publication["manifest_hash"],
        "status": "published",
    }
    assert publications_after_activation[0]["active"] is True
