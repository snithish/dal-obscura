from __future__ import annotations

from tests.interfaces.control_plane.test_actor_auth import (
    _bearer,
)
from tests.interfaces.control_plane.test_actor_auth import (
    _client as _actor_client,
)
from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    DEFAULT_AUTH_MODULE,
    ICEBERG_CATALOG_MODULE,
    _client,
    _provision_draft,
)


def test_public_publication_routes_are_removed():
    client = _client()

    assert client.get("/v1/publications", headers=ADMIN_HEADERS).status_code == 404
    assert client.post("/v1/publications", headers=ADMIN_HEADERS).status_code == 404
    assert client.get("/v1/publications/draft", headers=ADMIN_HEADERS).status_code == 404


def test_policy_version_publish_rejects_unowned_assets():
    client = _client()
    asset = _provision_draft(client)

    response = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Cannot publish until 1 asset has an assigned owner."}


def test_policy_version_publish_rejects_assets_without_policy_rules():
    client = _client()
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
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {}},
        headers=ADMIN_HEADERS,
    ).json()
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:alice@example.com"]},
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

    response = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Cannot publish a policy version without policy rules."}


def test_policy_version_publish_rejects_missing_auth_provider():
    client = _client()
    asset = _provision_draft(client)
    client.put(
        "/v1/settings/auth-providers",
        json={"providers": []},
        headers=ADMIN_HEADERS,
    )
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:owner@example.com"]},
        headers=ADMIN_HEADERS,
    )

    response = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "Cannot publish until at least one auth provider is enabled."
    }


def test_policy_version_publish_bootstraps_default_runtime_context():
    client = _client()
    asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:owner@example.com"]},
        headers=ADMIN_HEADERS,
    )

    versions_before = client.get("/v1/policy-versions", headers=ADMIN_HEADERS).json()
    assets_before = client.get("/v1/assets", headers=ADMIN_HEADERS).json()
    catalogs_before = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
    version = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    ).json()
    versions_after_publish = client.get("/v1/policy-versions", headers=ADMIN_HEADERS).json()
    summary = client.get("/v1/workspace/summary", headers=ADMIN_HEADERS).json()

    assert len(catalogs_before) == 1
    assert len(assets_before) == 1
    assert versions_before == []
    assert version["asset_id"] == asset["id"]
    assert version["policy_version"] == versions_after_publish[0]["policy_version"]
    assert versions_after_publish[0]["asset_id"] == asset["id"]
    assert versions_after_publish[0]["asset_name"] == "default.users"
    assert versions_after_publish[0]["catalog"] == "analytics"
    assert versions_after_publish[0]["target"] == "default.users"
    assert versions_after_publish[0]["active"] is True
    assert "cell_id" not in versions_after_publish[0]
    assert "publication_id" not in version
    assert "manifest_hash" not in version
    assert "active_publication" not in summary
    assert summary["runtime_configured"] is True
    assert summary["enabled_auth_provider_count"] == 1


def test_policy_version_history_is_asset_focused():
    client = _client()
    asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:owner@example.com"]},
        headers=ADMIN_HEADERS,
    )
    created = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    ).json()

    response = client.get("/v1/policy-versions", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json() == [
        {
            "asset_id": asset["id"],
            "asset_name": "default.users",
            "catalog": "analytics",
            "target": "default.users",
            "policy_version": created["policy_version"],
            "active": True,
            "created_at": response.json()[0]["created_at"],
        }
    ]
    assert response.json()[0]["created_at"]
    assert "cell_id" not in response.json()[0]


def test_asset_policy_history_detail_and_restore_are_revisioned():
    client = _client()
    asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:owner@example.com"]},
        headers=ADMIN_HEADERS,
    )
    created = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    ).json()

    history_response = client.get(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    )
    detail_response = client.get(
        f"/v1/assets/{asset['id']}/policy-versions/{created['policy_version']}",
        headers=ADMIN_HEADERS,
    )

    assert history_response.status_code == 200
    assert [item["policy_version"] for item in history_response.json()] == [
        created["policy_version"]
    ]
    assert detail_response.status_code == 200
    assert detail_response.json()["asset_id"] == asset["id"]
    assert detail_response.json()["policy_version"] == created["policy_version"]
    assert detail_response.json()["rules"][0]["columns"] == ["id", "email"]
    assert "compiled_config" not in detail_response.json()

    restore_response = client.post(
        f"/v1/assets/{asset['id']}/policy-versions/{created['policy_version']}/restore",
        json={"expected_revision": 0},
        headers=ADMIN_HEADERS,
    )

    assert restore_response.status_code == 200
    restored = restore_response.json()
    assert restored["revision"] == 1
    assert restored["base_policy_version"] == created["policy_version"]
    assert restored["rules"][0]["columns"] == ["id", "email"]


def test_asset_policy_history_is_capability_scoped():
    client = _actor_client()
    asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["asset-owner"]},
        headers=ADMIN_HEADERS,
    )
    created = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    ).json()

    outsider = _bearer("outsider-token")
    assert client.get(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=outsider,
    ).status_code == 403
    assert client.get(
        f"/v1/assets/{asset['id']}/policy-versions/{created['policy_version']}",
        headers=outsider,
    ).status_code == 403
    assert client.post(
        f"/v1/assets/{asset['id']}/policy-versions/{created['policy_version']}/restore",
        json={"expected_revision": 0},
        headers=outsider,
    ).status_code == 403


def test_publish_idempotency_key_replays_committed_result_and_rejects_mismatch():
    client = _client()
    asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["owner"]},
        headers=ADMIN_HEADERS,
    )
    headers = {**ADMIN_HEADERS, "Idempotency-Key": "publish-once"}
    first = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=headers,
    )
    replay = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=headers,
    )
    mismatch = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        json={"expected_draft_revision": 0},
        headers=headers,
    )

    assert first.status_code == 200
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert mismatch.status_code == 409
