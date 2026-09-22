from __future__ import annotations

from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    DEFAULT_AUTH_MODULE,
    _client,
    _keys_recursive,
)


def test_workspace_runtime_settings_can_be_configured_without_tenant_or_cell_ids():
    client = _client()

    get_before_setup = client.get("/v1/settings/runtime", headers=ADMIN_HEADERS)
    stale_creation = client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 1200,
            "max_tickets": 32,
            "max_ticket_exchanges": 3,
            "expected_revision": 7,
        },
        headers=ADMIN_HEADERS,
    )
    assert stale_creation.status_code == 409
    assert client.get("/v1/settings/runtime", headers=ADMIN_HEADERS).json() is None
    put_response = client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 1200,
            "max_tickets": 32,
            "max_ticket_exchanges": 3,
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
        "path_rules": [],
        "revision": 1,
    }
    updated = client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 1800,
            "max_tickets": 32,
            "max_ticket_exchanges": 3,
            "expected_revision": 1,
        },
        headers=ADMIN_HEADERS,
    )
    stale = client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 2400,
            "max_tickets": 32,
            "max_ticket_exchanges": 3,
            "expected_revision": 1,
        },
        headers=ADMIN_HEADERS,
    )
    assert updated.status_code == 200
    assert updated.json()["revision"] == 2
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "revision_conflict"
    assert stale.json()["error"]["request_id"]
    events = client.get("/v1/audit/events/page", headers=ADMIN_HEADERS).json()["items"]
    runtime_events = [event for event in events if event["action"] == "workspace.runtime.update"]
    assert runtime_events[0]["actor"] == "platform:admin"
    assert runtime_events[0]["details"] == {
        "ticket_ttl_seconds": 1800,
        "max_tickets": 32,
        "max_ticket_exchanges": 3,
        "path_rules": [],
    }


def test_workspace_runtime_settings_accepts_path_rules_and_audits_roots():
    client = _client()

    response = client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 1200,
            "max_tickets": 32,
            "max_ticket_exchanges": 3,
            "path_rules": [{"root": "s3://warehouse"}],
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 200
    assert response.json()["path_rules"] == [{"root": "s3://warehouse"}]
    events = client.get("/v1/audit/events/page", headers=ADMIN_HEADERS).json()["items"]
    runtime_events = [event for event in events if event["action"] == "workspace.runtime.update"]
    assert runtime_events[0]["details"]["path_rules"] == [{"root": "s3://warehouse"}]


def test_workspace_runtime_settings_rejects_unsafe_path_rules():
    client = _client()

    response = client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 1200,
            "max_tickets": 32,
            "max_ticket_exchanges": 3,
            "path_rules": [{"root": "s3://warehouse/*"}],
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "validation_error"


def test_workspace_runtime_settings_rejects_non_string_path_root():
    client = _client()

    response = client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 1200,
            "max_tickets": 32,
            "max_ticket_exchanges": 3,
            "path_rules": [{"root": 123}],
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "validation_error"


def test_workspace_auth_providers_can_be_configured_without_cell_ids():
    client = _client()

    before_setup = client.get("/v1/settings/auth-providers", headers=ADMIN_HEADERS)
    put_response = client.put(
        "/v1/settings/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": DEFAULT_AUTH_MODULE,
                    "args": {"issuer": "https://issuer.example"},
                    "enabled": True,
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    after_setup = client.get("/v1/settings/auth-providers", headers=ADMIN_HEADERS)

    assert before_setup.status_code == 200
    assert before_setup.json() == []
    assert put_response.status_code == 200
    assert after_setup.json() == [
        {
            "id": after_setup.json()[0]["id"],
            "ordinal": 1,
            "module": DEFAULT_AUTH_MODULE,
            "args": {"issuer": "https://issuer.example"},
            "enabled": True,
            "revision": 0,
        }
    ]
    assert "cell" not in _keys_recursive(after_setup.json())
    events = client.get("/v1/audit/events/page", headers=ADMIN_HEADERS).json()["items"]
    provider_events = [
        event for event in events if event["action"] == "workspace.auth_providers.update"
    ]
    assert provider_events[0]["actor"] == "platform:admin"
    assert provider_events[0]["details"] == {"provider_count": 1, "enabled_count": 1}

    updated = client.put(
        "/v1/settings/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": DEFAULT_AUTH_MODULE,
                    "args": {"issuer": "https://issuer.example"},
                    "enabled": False,
                }
            ],
            "expected_revision": 0,
        },
        headers=ADMIN_HEADERS,
    )
    assert updated.status_code == 200
    assert updated.json()[0]["revision"] == 1

    missing = client.put(
        "/v1/settings/auth-providers",
        json={"providers": []},
        headers=ADMIN_HEADERS,
    )
    stale = client.put(
        "/v1/settings/auth-providers",
        json={"providers": [], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    assert missing.status_code == 428
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "revision_conflict"
    assert missing.json()["error"]["code"] == "revision_precondition_required"
    removed = client.put(
        "/v1/settings/auth-providers",
        json={"providers": [], "expected_revision": 1},
        headers=ADMIN_HEADERS,
    )
    assert removed.status_code == 200
    assert client.get("/v1/settings/auth-providers/revision", headers=ADMIN_HEADERS).json() == {
        "revision": 2
    }
    recreated = client.put(
        "/v1/settings/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": DEFAULT_AUTH_MODULE,
                    "args": {"issuer": "https://issuer.example"},
                    "enabled": True,
                }
            ],
            "expected_revision": 2,
        },
        headers=ADMIN_HEADERS,
    )
    assert recreated.status_code == 200
    assert recreated.json()[0]["revision"] == 3


def test_workspace_auth_providers_reject_unsupported_modules_and_inline_key_material():
    client = _client()

    unsupported = client.put(
        "/v1/settings/auth-providers",
        json={
            "providers": [
                {"ordinal": 1, "module": "untrusted.Provider", "args": {}, "enabled": True}
            ]
        },
        headers=ADMIN_HEADERS,
    )
    inline_secret = client.put(
        "/v1/settings/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": DEFAULT_AUTH_MODULE,
                    "args": {
                        "issuer": "https://issuer.example",
                        "jwks": {"keys": [{"kty": "RSA", "n": "private"}]},
                    },
                    "enabled": True,
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )

    assert unsupported.status_code == 400
    assert "only built-in OIDC" in unsupported.json()["detail"]
    assert inline_secret.status_code == 400
    assert "static JWKS" in inline_secret.json()["detail"]


def test_workspace_auth_providers_normalize_malformed_endpoint_errors():
    client = _client()

    response = client.put(
        "/v1/settings/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": DEFAULT_AUTH_MODULE,
                    "args": {"issuer": "https://[broken/realm"},
                    "enabled": True,
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload["error"]["code"] == "validation_error"
    assert "valid HTTP(S) URL" in payload["error"]["message"]
    assert payload["error"]["request_id"]
