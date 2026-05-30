from __future__ import annotations

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.interfaces.api import create_app

ADMIN_HEADERS = {"authorization": "Bearer test-admin"}
ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
DEFAULT_AUTH_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_default.DefaultIdentityAdapter"
)


def _client() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return TestClient(create_app(session_factory(engine), admin_token="test-admin"))


def test_workspace_summary_is_empty_before_setup():
    client = _client()

    response = client.get("/v1/workspace/summary", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json() == {
        "catalog_count": 0,
        "asset_count": 0,
        "unowned_asset_count": 0,
        "missing_policy_count": 0,
        "draft_change_count": 0,
        "active_publication": None,
    }


def test_workspace_routes_require_admin_token():
    client = _client()

    assert client.get("/v1/workspace/summary").status_code == 401
    assert client.get("/v1/catalogs").status_code == 401
    assert client.put("/v1/catalogs/analytics", json={}).status_code == 401
    assert client.get("/v1/assets").status_code == 401
    assert client.put("/v1/assets/analytics/default.users", json={}).status_code == 401
    assert client.get("/v1/publications/draft").status_code == 401


def test_workspace_catalog_upsert_bootstraps_default_workspace():
    client = _client()

    response = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    catalogs = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
    summary = client.get("/v1/workspace/summary", headers=ADMIN_HEADERS).json()

    assert response.status_code == 200
    assert response.json() == {"id": response.json()["id"], "name": "analytics"}
    assert catalogs == [
        {
            "id": response.json()["id"],
            "name": "analytics",
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
            "status": "configured",
            "discovered_table_count": 0,
            "governed_asset_count": 0,
        }
    ]
    assert summary["catalog_count"] == 1
    assert summary["asset_count"] == 0


def test_workspace_runtime_settings_can_be_configured_without_tenant_or_cell_ids():
    client = _client()

    get_before_setup = client.get("/v1/settings/runtime", headers=ADMIN_HEADERS)
    put_response = client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 1200,
            "max_tickets": 32,
            "max_ticket_exchanges": 3,
            "path_rules": [{"glob": "s3://warehouse/*", "allow": True}],
        },
        headers=ADMIN_HEADERS,
    )
    get_after_setup = client.get("/v1/settings/runtime", headers=ADMIN_HEADERS)

    assert get_before_setup.status_code == 200
    assert get_before_setup.json() is None
    assert put_response.status_code == 200
    assert get_after_setup.json() == {
        "ticket_ttl_seconds": 1200,
        "max_tickets": 32,
        "max_ticket_exchanges": 3,
        "path_rules": [{"glob": "s3://warehouse/*", "allow": True}],
    }


def test_workspace_asset_upsert_uses_default_workspace_context():
    client = _client()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )

    response = client.put(
        "/v1/assets/analytics/default.users",
        json={
            "backend": "iceberg",
            "table_identifier": "prod.users",
            "options": {"snapshot": 7},
        },
        headers=ADMIN_HEADERS,
    )
    assets = client.get("/v1/assets", headers=ADMIN_HEADERS).json()
    detail = client.get(f"/v1/assets/{response.json()['id']}", headers=ADMIN_HEADERS).json()

    assert response.status_code == 200
    assert response.json() == {
        "id": response.json()["id"],
        "catalog": "analytics",
        "target": "default.users",
    }
    assert assets == [
        {
            "id": response.json()["id"],
            "name": "default.users",
            "catalog": "analytics",
            "backend": "iceberg",
            "table_identifier": "prod.users",
            "owner_count": 0,
            "policy_status": "missing",
            "draft_status": "draft",
        }
    ]
    assert detail["options"] == {"snapshot": 7}
    assert detail["policy_rules"] == []
    assert "tenant" not in _keys_recursive({"assets": assets, "detail": detail})
    assert "cell" not in _keys_recursive({"assets": assets, "detail": detail})


def test_workspace_policy_rules_can_be_replaced_from_asset_detail():
    client = _client()
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
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {}},
        headers=ADMIN_HEADERS,
    ).json()

    response = client.put(
        f"/v1/assets/{asset['id']}/policy-rules",
        json={
            "rules": [
                {
                    "ordinal": 1,
                    "effect": "allow",
                    "principals": ["group:data-stewards"],
                    "when": {},
                    "columns": ["id", "email"],
                    "masks": {"email": {"type": "email"}},
                    "row_filter": "region = 'us'",
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()

    assert response.status_code == 200
    assert detail["policy_status"] == "configured"
    assert detail["policy_rules"] == [
        {
            "id": detail["policy_rules"][0]["id"],
            "asset_id": asset["id"],
            "ordinal": 1,
            "effect": "allow",
            "principals": ["group:data-stewards"],
            "when": {},
            "columns": ["id", "email"],
            "masks": {"email": {"type": "email"}},
            "row_filter": "region = 'us'",
        }
    ]


def test_workspace_catalogs_assets_and_asset_detail_hide_runtime_ids():
    client = _client()
    asset = _provision_draft(client)

    summary = client.get("/v1/workspace/summary", headers=ADMIN_HEADERS).json()
    catalogs = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
    assets = client.get("/v1/assets", headers=ADMIN_HEADERS).json()
    asset_detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()

    assert summary == {
        "catalog_count": 1,
        "asset_count": 1,
        "unowned_asset_count": 1,
        "missing_policy_count": 0,
        "draft_change_count": 1,
        "active_publication": None,
    }
    assert catalogs == [
        {
            "id": catalogs[0]["id"],
            "name": "analytics",
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
            "status": "configured",
            "discovered_table_count": 0,
            "governed_asset_count": 1,
        }
    ]
    assert assets == [
        {
            "id": asset["id"],
            "name": "default.users",
            "catalog": "analytics",
            "backend": "iceberg",
            "table_identifier": "prod.users",
            "owner_count": 0,
            "policy_status": "configured",
            "draft_status": "draft",
        }
    ]
    assert asset_detail == {
        **assets[0],
        "options": {"snapshot": 1},
        "policy_rules": [
            {
                "id": asset_detail["policy_rules"][0]["id"],
                "asset_id": asset["id"],
                "ordinal": 10,
                "effect": "allow",
                "principals": ["user1"],
                "when": {"tenant": "default"},
                "columns": ["id", "email"],
                "masks": {"email": {"type": "email"}},
                "row_filter": "region = 'us'",
            }
        ],
    }
    assert "tenant" not in _keys_recursive(summary | {"catalogs": catalogs, "assets": assets})
    assert "cell" not in _keys_recursive(summary | {"catalogs": catalogs, "assets": assets})


def test_workspace_publish_routes_use_default_runtime_context():
    client = _client()
    _provision_draft(client)

    draft = client.get("/v1/publications/draft", headers=ADMIN_HEADERS).json()
    publication = client.post("/v1/publications", headers=ADMIN_HEADERS).json()
    activate = client.post(
        f"/v1/publications/{publication['publication_id']}/activate",
        headers=ADMIN_HEADERS,
    ).json()
    summary = client.get("/v1/workspace/summary", headers=ADMIN_HEADERS).json()

    assert draft["catalog_count"] == 1
    assert draft["asset_count"] == 1
    assert publication["catalog_count"] == 1
    assert publication["asset_count"] == 1
    assert activate["publication_id"] == publication["publication_id"]
    assert summary["active_publication"] == {
        "publication_id": publication["publication_id"],
        "manifest_hash": publication["manifest_hash"],
        "status": "published",
    }


def _provision_draft(client: TestClient) -> dict[str, str]:
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
    return asset


def _keys_recursive(value: object) -> set[str]:
    if isinstance(value, dict):
        keys = {str(key) for key in value}
        for item in value.values():
            keys.update(_keys_recursive(item))
        return keys
    if isinstance(value, list):
        keys: set[str] = set()
        for item in value:
            keys.update(_keys_recursive(item))
        return keys
    return set()
