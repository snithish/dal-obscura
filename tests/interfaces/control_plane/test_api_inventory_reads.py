from __future__ import annotations

import asyncio
from collections.abc import MutableMapping
from typing import Any

from dal_obscura.interfaces.http.app import create_app
from dal_obscura.storage.database.db import (
    session_factory,
)

ADMIN_HEADERS = {"authorization": "Bearer test-admin"}


def test_public_health_probes_and_request_correlation(client_factory):
    client = client_factory()
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


def test_control_plane_rejects_invalid_content_length_with_structured_error(client_factory):
    client = client_factory()

    response = client.post("/v1/logout", content=b"{}", headers={"content-length": "invalid"})

    assert response.status_code == 400
    payload = response.json()
    assert payload["detail"] == "Invalid content length"
    assert payload["error"]["code"] == "validation_error"
    assert payload["error"]["request_id"] == response.headers["x-request-id"]


def test_control_plane_rejects_oversized_chunked_body_without_content_length(db_engine):
    engine = db_engine
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
DEFAULT_AUTH_MODULE = "dal_obscura.identity.oidc.OidcJwksIdentityProvider"
