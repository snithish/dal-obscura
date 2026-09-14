from __future__ import annotations

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.control_plane.interfaces.api import create_app
from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    _active_policy_versions,
    _client,
    _provision_draft,
    save_policy_draft,
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


def test_workspace_observations_bind_to_active_generation_without_claiming_flight_health():
    client = _client()
    asset = _provision_draft(client)
    owners = client.put(
        f"/v1/assets/{asset['id']}/owners",
        headers=ADMIN_HEADERS,
        json={"owners": ["platform:admin"], "expected_revision": 0},
    )
    assert owners.status_code == 200, owners.json()
    published = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    )
    assert published.status_code == 200, published.json()

    response = client.get("/v1/workspace/observations", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    body = response.json()
    assert body["available"] is True
    assert body["generation"]["publication_id"]
    assert body["generation"]["manifest_hash"]
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
    assert client.get("/v1/policy-versions").status_code == 401
    assert client.get("/v1/settings/auth-providers").status_code == 401


def test_workspace_publication_management_is_admin_scoped_and_staged():
    client = _client()
    asset = _provision_draft(client)
    assert (
        client.put(
            f"/v1/assets/{asset['id']}/owners",
            headers=ADMIN_HEADERS,
            json={"owners": ["platform:admin"], "expected_revision": 0},
        ).status_code
        == 200
    )

    created = client.post("/v1/workspace/publications", headers=ADMIN_HEADERS)
    assert created.status_code == 200, created.json()
    publication = created.json()
    assert publication["asset_count"] == 1
    assert publication["catalog_count"] == 1
    assert publication["manifest_hash"]

    listed = client.get("/v1/workspace/publications", headers=ADMIN_HEADERS)
    assert listed.status_code == 200
    assert listed.json()[0]["id"] == publication["publication_id"]
    assert listed.json()[0]["active"] is False
    assert listed.json()[0]["asset_count"] == 1
    assert listed.json()[0]["catalog_count"] == 1
    assert listed.json()[0]["created_at"]

    activated = client.post(
        f"/v1/workspace/publications/{publication['publication_id']}/activate",
        headers=ADMIN_HEADERS,
        json={"expected_publication_id": None},
    )
    assert activated.status_code == 200
    assert activated.json() == {"publication_id": publication["publication_id"]}
    active = client.get("/v1/workspace/publications", headers=ADMIN_HEADERS)
    assert active.json()[0]["active"] is True

    events = client.get("/v1/audit/events/page", headers=ADMIN_HEADERS).json()["items"]
    assert {event["action"] for event in events} >= {
        "workspace.publication.create",
        "workspace.publication.activate",
    }

    assert client.get("/v1/workspace/publications").status_code == 401


def test_workspace_publication_activation_rejects_stale_generation_precondition():
    client = _client()
    asset = _provision_draft(client)
    assert (
        client.put(
            f"/v1/assets/{asset['id']}/owners",
            headers=ADMIN_HEADERS,
            json={"owners": ["platform:admin"], "expected_revision": 0},
        ).status_code
        == 200
    )
    first = client.post("/v1/workspace/publications", headers=ADMIN_HEADERS).json()
    second = client.post("/v1/workspace/publications", headers=ADMIN_HEADERS).json()

    assert (
        client.post(
            f"/v1/workspace/publications/{first['publication_id']}/activate",
            headers=ADMIN_HEADERS,
            json={"expected_publication_id": None},
        ).status_code
        == 200
    )
    activated = client.post(
        f"/v1/workspace/publications/{second['publication_id']}/activate",
        headers=ADMIN_HEADERS,
        json={"expected_publication_id": first["publication_id"]},
    )
    stale = client.post(
        f"/v1/workspace/publications/{first['publication_id']}/activate",
        headers=ADMIN_HEADERS,
        json={"expected_publication_id": first["publication_id"]},
    )

    assert activated.status_code == 200
    assert stale.status_code == 409


def test_workspace_publication_activation_requires_generation_precondition():
    client = _client()
    asset = _provision_draft(client)
    assert (
        client.put(
            f"/v1/assets/{asset['id']}/owners",
            headers=ADMIN_HEADERS,
            json={"owners": ["platform:admin"], "expected_revision": 0},
        ).status_code
        == 200
    )
    publication = client.post("/v1/workspace/publications", headers=ADMIN_HEADERS).json()

    missing = client.post(
        f"/v1/workspace/publications/{publication['publication_id']}/activate",
        headers=ADMIN_HEADERS,
    )

    assert missing.status_code == 428


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
    asset = _provision_draft(client)

    draft = client.get(f"/v1/assets/{asset['id']}/draft", headers=ADMIN_HEADERS).json()
    response = client.put(
        f"/v1/assets/{asset['id']}/draft",
        headers=ADMIN_HEADERS,
        json={
            "expected_revision": draft["revision"],
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
        "Policy rules are explicit grants; use effect='allow' or omit deny rules."
    )
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()
    assert detail["policy_status"] == "configured"


def test_policy_publish_versions_one_asset_without_publishing_other_drafts():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    factory = session_factory(engine)
    client = TestClient(create_app(factory, admin_token="test-admin"))
    first_asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{first_asset['id']}/owners",
        json={"owners": ["user:owner@example.com"], "expected_revision": 0},
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
        json={"owners": ["user:owner@example.com"], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    save_policy_draft(
        client,
        second_asset["id"],
        [
            {
                "ordinal": 1,
                "principals": ["user:owner@example.com"],
                "columns": ["id", "account_id"],
                "effect": "allow",
                "when": {},
                "masks": {},
                "row_filter": None,
            }
        ],
    )
    initial_response = client.post(
        f"/v1/assets/{first_asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    )
    assert initial_response.status_code == 200, initial_response.json()

    before_versions = _active_policy_versions(factory)
    assert set(before_versions) == {("analytics", "default.users")}
    save_policy_draft(
        client,
        first_asset["id"],
        [
            {
                "ordinal": 1,
                "principals": ["user:owner@example.com"],
                "columns": ["id", "email"],
                "effect": "allow",
                "when": {},
                "masks": {"email": {"type": "redact", "value": "[redacted]"}},
                "row_filter": "id > 10",
            }
        ],
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

    publish_second = client.post(
        f"/v1/assets/{second_asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    )
    assert publish_second.status_code == 200, publish_second.json()
    final_versions = _active_policy_versions(factory)
    assert set(final_versions) == {
        ("analytics", "default.users"),
        ("analytics", "default.accounts"),
    }
