"""Customer-visible contracts shared by runtime, OpenAPI, and browser clients."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

ORIGIN = "https://console.example.com"
ADMIN = {"authorization": "Bearer test-admin"}


def test_catalog_write_documents_its_actual_json_request(client_factory):
    operation = client_factory().get("/openapi.json").json()["paths"]["/v1/catalogs/{name}"]["put"]
    assert operation["requestBody"]["required"] is True
    assert operation["requestBody"]["content"]["application/json"]["schema"]["$ref"].endswith(
        "/CatalogRequest"
    )


@pytest.mark.parametrize("body", [b"{", b"[]", b'{"plugin_id": "", "secret": "sentinel-secret"}'])
def test_catalog_invalid_json_has_safe_field_errors_and_never_mutates(client_factory, body):
    client = client_factory()
    response = client.put(
        "/v1/catalogs/analytics",
        content=body,
        headers={**ADMIN, "content-type": "application/json"},
    )
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation_error"
    assert error["field_errors"]
    assert error["request_id"] == response.headers["x-request-id"]
    assert "sentinel-secret" not in response.text
    assert client.get("/v1/catalogs", headers=ADMIN).json() == []


@pytest.mark.parametrize(
    ("method", "path", "status", "code"),
    [
        ("GET", "/v1/not-a-route", 404, "not_found"),
        ("POST", "/v1/catalogs", 405, "method_not_allowed"),
        ("GET", "/ui/assets/example", 404, "not_found"),
    ],
)
def test_framework_errors_follow_the_public_error_contract(
    client_factory, method, path, status, code
):
    response = client_factory().request(method, path, headers={"x-request-id": "support-42"})
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert (
        response.json()["error"]["request_id"] == response.headers["x-request-id"] == "support-42"
    )
    if status == 405:
        assert response.headers["allow"] == "GET"


def test_openapi_documents_bearer_auth_and_actual_validation_envelope(client_factory):
    schema = client_factory().get("/openapi.json").json()
    assert schema["components"]["securitySchemes"]["BearerAuth"] == {
        "type": "http",
        "scheme": "bearer",
    }
    for path, method in [("/v1/catalogs", "get"), ("/v1/assets/{asset_id}/policy", "put")]:
        operation = schema["paths"][path][method]
        assert {"BearerAuth": []} in operation["security"]
        assert not any(p["name"] == "authorization" for p in operation.get("parameters", []))
        assert operation["responses"]["422"]["content"]["application/json"]["schema"][
            "$ref"
        ].endswith("/ApiErrorResponse")
    assert not schema["paths"]["/v1/session/options"]["get"].get("security")


def test_cors_supports_request_correlation_and_retry_headers(client_factory):
    client = client_factory(cors_origins=(ORIGIN,))
    preflight = client.options(
        "/v1/catalogs/analytics",
        headers={
            "origin": ORIGIN,
            "access-control-request-method": "PATCH",
            "access-control-request-headers": (
                "authorization,content-type,x-request-id,x-csrf-token"
            ),
        },
    )
    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == ORIGIN
    assert preflight.headers["access-control-allow-credentials"] == "true"
    assert {"GET", "PUT", "PATCH"} <= {
        method.strip() for method in preflight.headers["access-control-allow-methods"].split(",")
    }
    assert {"authorization", "content-type", "x-request-id", "x-csrf-token"} <= {
        header.strip().lower()
        for header in preflight.headers["access-control-allow-headers"].split(",")
    }
    response = client.get("/v1/catalogs", headers={"origin": ORIGIN, "x-request-id": "support-42"})
    exposed = {
        item.strip().lower()
        for item in response.headers["access-control-expose-headers"].split(",")
    }
    assert {"x-request-id", "retry-after"} <= exposed


def test_early_body_rejection_preserves_customer_request_id(client_factory):
    response = client_factory(max_request_bytes=16).put(
        "/v1/catalogs/analytics", content=b"x" * 17, headers={"x-request-id": "support-42"}
    )
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "request_too_large"
    assert response.json()["detail"] == "Request body too large"
    assert (
        response.json()["error"]["request_id"] == response.headers["x-request-id"] == "support-42"
    )


def test_missing_credentials_advertise_bearer_authentication(client_factory):
    client = client_factory()
    for method, path in [
        ("GET", "/v1/workspace/summary"),
        ("GET", "/v1/catalogs"),
        ("PUT", "/v1/catalogs/analytics"),
        ("GET", "/v1/assets"),
        ("PUT", "/v1/assets/analytics/default.users"),
        ("GET", "/v1/settings/runtime"),
        ("GET", "/v1/settings/auth-providers"),
    ]:
        response = client.request(method, path, json={} if method == "PUT" else None)
        assert response.status_code == 401, (method, path, response.text)
        assert response.headers["www-authenticate"] == "Bearer", (method, path)


def test_readiness_failure_documents_its_probe_envelope(client_factory):
    client = client_factory()
    schema = client.get("/openapi.json").json()
    with patch("sqlalchemy.orm.Session.execute", side_effect=RuntimeError("sentinel-secret")):
        response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert response.json()["checks"] == {"database": "failed"}
    assert response.json()["error"]["request_id"] == response.headers["x-request-id"]
    assert "sentinel-secret" not in response.text
    assert schema["paths"]["/readyz"]["get"]["responses"]["503"]["content"]["application/json"][
        "schema"
    ]["$ref"].endswith("/ReadinessResponse")


@pytest.mark.parametrize("path", ["/docs", "/redoc"])
def test_documentation_security_policy_allows_its_renderer_only_on_documentation(
    client_factory, path
):
    client = client_factory()
    response = client.get(path)
    assert response.status_code == 200
    assert "test-admin" not in response.text
    policy = response.headers["content-security-policy"]
    assert "https://cdn.jsdelivr.net" in response.text
    assert "https://cdn.jsdelivr.net" in policy
    assert "'unsafe-inline'" in policy
    api_policy = client.get("/v1/session/options").headers["content-security-policy"]
    assert "https://cdn.jsdelivr.net" not in api_policy
    assert "'unsafe-inline'" not in api_policy


def test_checked_in_openapi_matches_the_served_customer_contract(client_factory):
    snapshot = Path(__file__).resolve().parents[3] / "apps/governance-ui/openapi/control-plane.json"
    assert json.loads(snapshot.read_text()) == client_factory().get("/openapi.json").json()


def test_cors_is_absent_without_configured_ui_origin(client_factory) -> None:
    client = client_factory()

    response = client.options(
        "/v1/ui-auth-config",
        headers={
            "Access-Control-Request-Method": "GET",
            "Origin": "http://127.0.0.1:8821",
        },
    )

    assert "access-control-allow-origin" not in response.headers


def test_browser_session_lifetimes_are_bounded(client_factory):
    with pytest.raises(ValueError, match="24 hours"):
        client_factory(session_ttl_seconds=86_401)
