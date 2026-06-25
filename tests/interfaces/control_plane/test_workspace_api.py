from __future__ import annotations

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import (
    Base,
)
from dal_obscura.control_plane.interfaces.api import create_app
from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    _active_policy_versions,
    _client,
    _provision_draft,
)


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
        "runtime_configured": False,
        "enabled_auth_provider_count": 0,
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
    assert client.get("/v1/policy-versions").status_code == 401
    assert client.get("/v1/settings/auth-providers").status_code == 401


def test_tenant_and_cell_routes_are_not_public_workspace_api():
    client = _client()

    assert client.get("/v1/tenants", headers=ADMIN_HEADERS).status_code == 404
    assert (
        client.post(
            "/v1/tenants",
            headers=ADMIN_HEADERS,
            json={"slug": "x", "display_name": "X"},
        ).status_code
        == 404
    )
    assert client.get("/v1/cells", headers=ADMIN_HEADERS).status_code == 404
    assert (
        client.post(
            "/v1/cells",
            headers=ADMIN_HEADERS,
            json={"name": "x", "region": "local"},
        ).status_code
        == 404
    )


def test_policy_publish_versions_one_asset_without_publishing_other_drafts():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = session_factory(engine)
    client = TestClient(create_app(factory, admin_token="test-admin"))
    first_asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{first_asset['id']}/owners",
        json={"owners": ["user:owner@example.com"]},
        headers=ADMIN_HEADERS,
    )
    second_asset = client.put(
        "/v1/assets/analytics/default.accounts",
        json={
            "backend": "iceberg",
            "table_identifier": "prod.accounts",
            "options": {"snapshot": 1},
        },
        headers=ADMIN_HEADERS,
    ).json()
    client.put(
        f"/v1/assets/{second_asset['id']}/owners",
        json={"owners": ["user:owner@example.com"]},
        headers=ADMIN_HEADERS,
    )
    client.put(
        f"/v1/assets/{second_asset['id']}/policy-rules",
        json={
            "rules": [
                {
                    "ordinal": 1,
                    "principals": ["user:owner@example.com"],
                    "columns": ["id", "account_id"],
                    "effect": "allow",
                    "when": {},
                    "masks": {},
                    "row_filter": None,
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    initial_response = client.post(
        f"/v1/assets/{first_asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    )
    assert initial_response.status_code == 200, initial_response.json()

    before_versions = _active_policy_versions(factory)
    client.put(
        f"/v1/assets/{first_asset['id']}/policy-rules",
        json={
            "rules": [
                {
                    "ordinal": 1,
                    "principals": ["user:owner@example.com"],
                    "columns": ["id", "email"],
                    "effect": "allow",
                    "when": {},
                    "masks": {"email": {"type": "redact", "value": "[redacted]"}},
                    "row_filter": "id > 10",
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    client.put(
        f"/v1/assets/{second_asset['id']}/policy-rules",
        json={
            "rules": [
                {
                    "ordinal": 1,
                    "principals": ["user:owner@example.com"],
                    "columns": ["id"],
                    "effect": "allow",
                    "when": {},
                    "masks": {},
                    "row_filter": "id > 99",
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )

    response = client.post(
        f"/v1/assets/{first_asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    )

    after_versions = _active_policy_versions(factory)
    assert response.status_code == 200
    assert response.json()["asset_id"] == first_asset["id"]
    assert (
        after_versions[("analytics", "default.users")]
        != before_versions[("analytics", "default.users")]
    )
    assert (
        after_versions[("analytics", "default.accounts")]
        == before_versions[("analytics", "default.accounts")]
    )
