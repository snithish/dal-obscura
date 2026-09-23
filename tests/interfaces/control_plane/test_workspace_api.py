from __future__ import annotations

from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    _client,
    _provision_asset,
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
        "runtime_configured": False,
        "enabled_auth_provider_count": 0,
    }


def test_workspace_observations_are_truthful_before_setup():
    client = _client()

    response = client.get("/v1/workspace/observations", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    body = response.json()
    assert body["available"] is False
    assert body["source"] == "control-plane-db"
    assert body["generation"] is None
    assert body["data_plane"] == {
        "status": "unobserved",
        "reason": "workspace_not_configured",
    }


def test_workspace_observations_report_live_configuration_without_claiming_flight_health():
    client = _client()
    _provision_asset(client)

    response = client.get("/v1/workspace/observations", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    body = response.json()
    assert body["available"] is True
    assert body["generation"]["config_revision"]
    assert body["data_plane"] == {
        "status": "unobserved",
        "reason": "flight_health_probe_not_configured",
    }


def test_workspace_routes_require_admin_token():
    client = _client()

    assert client.get("/v1/workspace/summary").status_code == 401
    assert client.get("/v1/catalogs").status_code == 401
    assert client.put("/v1/catalogs/analytics", json={}).status_code == 401
    assert client.get("/v1/assets").status_code == 401
    assert client.put("/v1/assets/analytics/default.users", json={}).status_code == 401
    assert client.get("/v1/publications/draft").status_code == 404
    assert client.get("/v1/policy-versions/page").status_code == 404
    assert client.get("/v1/assets/00000000-0000-0000-0000-000000000000/draft").status_code == 405
    assert client.get("/v1/workspace/publications").status_code == 404
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


def test_workspace_policy_rules_reject_deny_effect_before_save():
    client = _client()
    asset = _provision_asset(client)
    before = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()

    response = client.put(
        f"/v1/assets/{asset['id']}/policy",
        headers=ADMIN_HEADERS,
        json={
            "expected_revision": 0,
            "rules": [
                {
                    "ordinal": 1,
                    "principals": ["group:data-stewards"],
                    "columns": ["email"],
                    "effect": "deny",
                    "when": {},
                    "masks": {},
                    "row_filter": None,
                }
            ],
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == (
        "Policy rules are explicit grants; effect must be 'allow'."
    )
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()
    assert detail["policy_status"] == "configured"
    assert detail["policy_rules"] == before["policy_rules"]
