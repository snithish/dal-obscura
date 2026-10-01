from __future__ import annotations

import asyncio
from collections.abc import MutableMapping
from typing import Any

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.control_plane.interfaces.api import create_app

ADMIN_HEADERS = {"authorization": "Bearer test-admin"}


def _client() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    return TestClient(create_app(session_factory(engine), admin_token="test-admin"))


def _replace_live_policy(client: TestClient, asset_id: str, rules: list[dict]) -> None:
    asset = client.get(f"/v1/assets/{asset_id}", headers=ADMIN_HEADERS).json()
    response = client.put(
        f"/v1/assets/{asset_id}/policy",
        json={"expected_revision": asset["policy_revision"], "rules": rules},
        headers=ADMIN_HEADERS,
    )
    assert response.status_code == 200, response.text


def test_public_health_probes_and_request_correlation():
    client = _client()
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

    supplied = client.get("/healthz", headers={"x-request-id": "support-42"})
    generated = client.get("/healthz", headers={"x-request-id": "bad/id"})

    assert supplied.headers["x-request-id"] == "support-42"
    assert len(generated.headers["x-request-id"]) == 32
    assert generated.headers["x-request-id"].isalnum()

    response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "checks": {"database": "ok"}}


def test_control_plane_rejects_invalid_content_length_with_structured_error():
    client = _client()

    response = client.post("/v1/logout", content=b"{}", headers={"content-length": "invalid"})

    assert response.status_code == 400
    payload = response.json()
    assert payload["detail"] == "Invalid content length"
    assert payload["error"]["code"] == "validation_error"
    assert payload["error"]["request_id"] == response.headers["x-request-id"]


def test_control_plane_rejects_oversized_chunked_body_without_content_length():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    app = create_app(session_factory(engine), admin_token="test-admin", max_request_bytes=64)
    messages: list[dict[str, Any]] = [
        {"type": "http.request", "body": b"x" * 40, "more_body": True},
        {"type": "http.request", "body": b"y" * 25, "more_body": False},
    ]
    sent: list[MutableMapping[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return messages.pop(0)

    async def send(message: MutableMapping[str, Any]) -> None:
        sent.append(message)

    async def invoke() -> None:
        await app(
            {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "PUT",
                "scheme": "http",
                "path": "/v1/catalogs/analytics",
                "raw_path": b"/v1/catalogs/analytics",
                "query_string": b"",
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"authorization", b"Bearer test-admin"),
                ],
                "client": ("testclient", 123),
                "server": ("testserver", 80),
            },
            receive,
            send,
        )

    asyncio.run(invoke())

    assert sent[0]["status"] == 413
    assert b"Request body too large" in sent[1]["body"]


ICEBERG_CATALOG_ID = "iceberg.sql"
DEFAULT_AUTH_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
)


def _provision_asset(client: TestClient) -> dict[str, str]:
    client.put(
        "/v1/settings/runtime",
        json={"ticket_ttl_seconds": 900, "max_tickets": 64, "max_ticket_exchanges": 2},
        headers=ADMIN_HEADERS,
    )
    client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    asset = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {"snapshot": 1}},
        headers=ADMIN_HEADERS,
    ).json()
    _replace_live_policy(
        client,
        asset["id"],
        [
            {
                "ordinal": 10,
                "effect": "allow",
                "principals": ["user1"],
                "when": {"department": "default"},
                "columns": ["id", "email"],
                "masks": {"email": {"type": "email"}},
                "row_filter": "region = 'us'",
            }
        ],
    )
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:owner@example.com"], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    client.put(
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
    return asset


def test_reads_live_workspace_resources_after_writes():
    client = _client()
    asset = _provision_asset(client)

    runtime = client.get("/v1/settings/runtime", headers=ADMIN_HEADERS).json()
    catalogs = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
    assets = client.get("/v1/assets", headers=ADMIN_HEADERS).json()
    rules = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()["policy_rules"]
    auth = client.get("/v1/settings/auth-providers", headers=ADMIN_HEADERS).json()

    assert runtime == {
        "ticket_ttl_seconds": 900,
        "max_tickets": 64,
        "max_ticket_exchanges": 2,
        "path_rules": [],
        "revision": 1,
    }
    assert catalogs[0]["name"] == "analytics"
    assert catalogs[0]["plugin_id"] == ICEBERG_CATALOG_ID
    assert "module" not in catalogs[0]
    assert catalogs[0]["options"] == {"type": "sql", "uri": "sqlite:///catalog.db"}
    assert assets[0]["id"] == asset["id"]
    assert assets[0]["catalog"] == "analytics"
    assert assets[0]["name"] == "default.users"
    assert assets[0]["owner_count"] == 1
    assert assets[0]["policy_status"] == "configured"
    assert [
        {
            key: rule[key]
            for key in ("ordinal", "effect", "principals", "when", "columns", "masks", "row_filter")
        }
        for rule in rules
    ] == [
        {
            "ordinal": 10,
            "effect": "allow",
            "principals": ["user1"],
            "when": {"department": "default"},
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
            "args": {"issuer": "https://issuer.example"},
            "enabled": True,
            "revision": 0,
        }
    ]
    assert len(catalogs) == 1
    assert len(assets) == 1
