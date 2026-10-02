from __future__ import annotations

from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    _provision_asset,
)


def test_workspace_summary_and_observations_follow_configuration(client_factory):
    client = client_factory()
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

    response = client.get("/v1/workspace/observations", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    body = response.json()
    assert body["available"] is False
    assert body["source"] == "control-plane-db"
    assert body["data_plane"] == {"status": "unobserved", "reason": "workspace_not_configured"}

    _provision_asset(client)

    response = client.get("/v1/workspace/observations", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    body = response.json()
    assert body["available"] is True
    assert body["data_plane"] == {
        "status": "unobserved",
        "reason": "flight_health_probe_not_configured",
    }


def test_workspace_policy_rules_reject_deny_effect_before_save(client_factory):
    client = client_factory()
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

    assert response.status_code == 422
    assert response.json()["error"]["field_errors"][0]["field"] == "rules.0.effect"
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()
    assert detail["policy_status"] == "configured"
    assert detail["policy_rules"] == before["policy_rules"]
