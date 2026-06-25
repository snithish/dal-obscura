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


def test_inventory_reads_require_admin_token():
    client = _client()

    assert client.get("/v1/workspace/summary").status_code == 401
    assert client.get("/v1/catalogs").status_code == 401
    assert client.get("/v1/assets").status_code == 401
    assert client.get("/v1/settings/runtime").status_code == 401
    assert client.get("/v1/settings/auth-providers").status_code == 401
    assert client.get("/v1/publications").status_code == 401


ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
DEFAULT_AUTH_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_default.DefaultIdentityAdapter"
)


def _provision_draft(client: TestClient) -> dict[str, str]:
    client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 900,
            "max_tickets": 64,
            "max_ticket_exchanges": 2,
        },
        headers=ADMIN_HEADERS,
    )
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    asset = client.put(
        "/v1/assets/analytics/default.users",
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
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:owner@example.com"]},
        headers=ADMIN_HEADERS,
    )
    client.put(
        "/v1/settings/auth-providers",
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
    return asset


def test_reads_workspace_draft_resources_after_writes():
    client = _client()
    asset = _provision_draft(client)

    runtime = client.get(
        "/v1/settings/runtime",
        headers=ADMIN_HEADERS,
    ).json()
    catalogs = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
    assets = client.get("/v1/assets", headers=ADMIN_HEADERS).json()
    rules = client.get(f"/v1/assets/{asset['id']}/policy-rules", headers=ADMIN_HEADERS).json()
    auth = client.get("/v1/settings/auth-providers", headers=ADMIN_HEADERS).json()
    draft = client.get("/v1/publications/draft", headers=ADMIN_HEADERS).json()

    assert runtime == {
        "ticket_ttl_seconds": 900,
        "max_tickets": 64,
        "max_ticket_exchanges": 2,
    }
    assert catalogs[0]["name"] == "analytics"
    assert catalogs[0]["module"] == ICEBERG_CATALOG_MODULE
    assert catalogs[0]["options"] == {"type": "sql", "uri": "sqlite:///catalog.db"}
    assert assets[0]["id"] == asset["id"]
    assert assets[0]["catalog"] == "analytics"
    assert assets[0]["name"] == "default.users"
    assert assets[0]["owner_count"] == 1
    assert assets[0]["policy_status"] == "configured"
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
            "ordinal": 1,
            "module": DEFAULT_AUTH_MODULE,
            "args": {"jwt_secret": {"secret": "DAL_OBSCURA_JWT_SECRET"}},
            "enabled": True,
        }
    ]
    assert draft["catalog_count"] == 1
    assert draft["asset_count"] == 1
    assert draft["catalogs"][0]["name"] == "analytics"
    assert draft["assets"][0]["name"] == "default.users"


def test_cell_draft_route_is_not_public_workspace_api():
    client = _client()

    response = client.get(
        "/v1/cells/00000000-0000-0000-0000-000000000001/draft",
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Not Found"


def test_reads_publications_and_active_publication():
    client = _client()
    _provision_draft(client)

    publication = client.post(
        "/v1/publications",
        headers=ADMIN_HEADERS,
    ).json()
    publications_before_activation = client.get(
        "/v1/publications",
        headers=ADMIN_HEADERS,
    ).json()

    assert publications_before_activation == [
        {
            "id": publication["publication_id"],
            "schema_version": 1,
            "status": "published",
            "manifest_hash": publication["manifest_hash"],
            "active": False,
        }
    ]

    client.post(
        f"/v1/publications/{publication['publication_id']}/activate",
        headers=ADMIN_HEADERS,
    )
    summary = client.get("/v1/workspace/summary", headers=ADMIN_HEADERS).json()
    publications_after_activation = client.get(
        "/v1/publications",
        headers=ADMIN_HEADERS,
    ).json()

    assert summary["active_publication"]["publication_id"] == publication["publication_id"]
    assert summary["active_publication"]["status"] == "published"
    assert publications_after_activation[0]["active"] is True
