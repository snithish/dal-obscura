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
    }


def test_workspace_runtime_settings_rejects_path_rules():
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

    assert response.status_code == 422


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
                    "args": {"jwt_secret": {"secret": "DAL_OBSCURA_JWT_SECRET"}},
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
            "args": {"jwt_secret": {"secret": "DAL_OBSCURA_JWT_SECRET"}},
            "enabled": True,
        }
    ]
    assert "cell" not in _keys_recursive(after_setup.json())
