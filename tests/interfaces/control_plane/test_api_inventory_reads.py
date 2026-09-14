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


def _save_policy_draft(client: TestClient, asset_id: str, rules: list[dict]) -> None:
    draft = client.get(f"/v1/assets/{asset_id}/draft", headers=ADMIN_HEADERS).json()
    response = client.put(
        f"/v1/assets/{asset_id}/draft",
        json={"expected_revision": draft["revision"], "rules": rules},
        headers=ADMIN_HEADERS,
    )
    assert response.status_code == 200, response.text


def test_inventory_reads_require_admin_token():
    client = _client()

    assert client.get("/v1/workspace/summary").status_code == 401
    assert client.get("/v1/catalogs").status_code == 401
    assert client.get("/v1/assets").status_code == 401
    assert client.get("/v1/settings/runtime").status_code == 401
    assert client.get("/v1/settings/auth-providers").status_code == 401
    assert client.get("/v1/policy-versions").status_code == 401


def test_control_plane_healthz_is_public():
    client = _client()

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_control_plane_returns_bounded_request_correlation_id():
    client = _client()

    supplied = client.get("/healthz", headers={"x-request-id": "support-42"})
    generated = client.get("/healthz", headers={"x-request-id": "bad/id"})

    assert supplied.headers["x-request-id"] == "support-42"
    assert len(generated.headers["x-request-id"]) == 32
    assert generated.headers["x-request-id"].isalnum()


def test_control_plane_readyz_checks_database():
    client = _client()

    response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "checks": {"database": "ok"}}


def test_control_plane_rejects_oversized_requests_before_authentication():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    client = TestClient(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            max_request_bytes=64,
        )
    )

    response = client.post("/v1/logout", content=b"x" * 65)

    assert response.status_code == 413
    assert response.json() == {"detail": "Request body too large"}


def test_control_plane_rejects_oversized_chunked_body_without_content_length():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    app = create_app(
        session_factory(engine),
        admin_token="test-admin",
        max_request_bytes=64,
    )
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


ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
DEFAULT_AUTH_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
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
    _save_policy_draft(
        client,
        asset["id"],
        [
            {
                "ordinal": 10,
                "effect": "allow",
                "principals": ["user1"],
                "when": {"tenant": "default"},
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


def test_reads_workspace_draft_resources_after_writes():
    client = _client()
    asset = _provision_draft(client)

    runtime = client.get(
        "/v1/settings/runtime",
        headers=ADMIN_HEADERS,
    ).json()
    catalogs = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
    assets = client.get("/v1/assets", headers=ADMIN_HEADERS).json()
    rules = client.get(f"/v1/assets/{asset['id']}/draft", headers=ADMIN_HEADERS).json()["rules"]
    auth = client.get("/v1/settings/auth-providers", headers=ADMIN_HEADERS).json()

    assert runtime == {
        "ticket_ttl_seconds": 900,
        "max_tickets": 64,
        "max_ticket_exchanges": 2,
        "revision": 0,
    }
    assert catalogs[0]["name"] == "analytics"
    assert catalogs[0]["module"] == ICEBERG_CATALOG_MODULE
    assert catalogs[0]["plugin_id"] == "iceberg.sql"
    assert catalogs[0]["options"] == {"type": "sql", "uri": "sqlite:///catalog.db"}
    assert assets[0]["id"] == asset["id"]
    assert assets[0]["catalog"] == "analytics"
    assert assets[0]["name"] == "default.users"
    assert assets[0]["owner_count"] == 1
    assert assets[0]["policy_status"] == "configured"
    assert rules == [
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


def test_cell_draft_route_is_not_public_workspace_api():
    client = _client()

    response = client.get(
        "/v1/cells/00000000-0000-0000-0000-000000000001/draft",
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Not Found"


def test_reads_policy_versions_without_public_publication_fields():
    client = _client()
    asset = _provision_draft(client)

    created = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers=ADMIN_HEADERS,
    ).json()
    versions = client.get(
        "/v1/policy-versions",
        headers=ADMIN_HEADERS,
    ).json()

    assert versions == [
        {
            "asset_id": asset["id"],
            "asset_name": "default.users",
            "catalog": "analytics",
            "target": "default.users",
            "policy_version": created["policy_version"],
            "active": True,
            "created_at": versions[0]["created_at"],
        }
    ]

    summary = client.get("/v1/workspace/summary", headers=ADMIN_HEADERS).json()

    assert "active_publication" not in summary
    assert "publication_id" not in created
    assert "manifest_hash" not in created
    assert versions[0]["active"] is True
