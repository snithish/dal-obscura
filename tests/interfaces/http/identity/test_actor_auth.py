from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient
from httpx import Response

from dal_obscura.adapters.sql.database.db import (
    session_factory,
)
from dal_obscura.bootstrap.control import build_control_app
from dal_obscura.identity import http_session
from dal_obscura.identity.http_session import create_oidc_actor_resolver
from dal_obscura.kernel.identity.authentication import AuthenticationRequest
from dal_obscura.kernel.identity.principal import Principal
from tests.support.control.actors import _actor_for_token, _bearer, _client
from tests.support.control.workspace import ADMIN_HEADERS


def _client_with_ui_auth_config(http_client, db_engine) -> TestClient:
    engine = db_engine
    return http_client(
        build_control_app(
            session_factory(engine),
            admin_token="test-admin",
            oidc_actor_resolver=_actor_for_token,
            oidc_admin_group="platform-admins",
            ui_auth_config={
                "authority": "http://127.0.0.1:8080/realms/dal-obscura-demo",
                "client_id": "dal-obscura-ui",
                "redirect_uri": "http://127.0.0.1:8820/ui/auth/callback",
                "post_logout_redirect_uri": "http://127.0.0.1:8820/ui",
                "scope": "openid profile",
                "client_secret": "must-not-leak",
            },
            oidc_nonce_actor_resolver=lambda token, nonce_hash: {
                "principal": "asset-owner",
                "groups": ["asset-owners"],
            },
            authorization_code_exchange=lambda config, code, verifier: {
                "access_token": "owner-token",
                "id_token": "owner-id-token",
            },
        ),
        base_url="https://127.0.0.1:8820",
    )


def _login_as_asset_owner(client: TestClient) -> Response:
    start = client.get("/auth/login", follow_redirects=False)
    assert start.status_code == 303
    state = parse_qs(urlsplit(start.headers["location"]).query)["state"][0]
    callback = client.get(f"/auth/callback?code=test-code&state={state}", follow_redirects=False)
    assert callback.status_code == 303, callback.text
    return callback


def test_ui_auth_config_returns_public_oidc_browser_config_without_secret(http_client, db_engine):
    client = _client_with_ui_auth_config(http_client, db_engine)

    response = client.get("/v1/ui-auth-config")

    assert response.status_code == 200
    assert response.json() == {
        "authority": "http://127.0.0.1:8080/realms/dal-obscura-demo",
        "client_id": "dal-obscura-ui",
        "redirect_uri": "http://127.0.0.1:8820/ui/auth/callback",
        "post_logout_redirect_uri": "http://127.0.0.1:8820/ui",
        "scope": "openid profile",
    }


def test_cookie_session_requires_csrf_header_for_mutations(http_client, db_engine):
    client = _client_with_ui_auth_config(http_client, db_engine)
    login = _login_as_asset_owner(client)

    cookie_header = (
        f"__Host-dal_obscura_session={login.cookies['__Host-dal_obscura_session']}; "
        f"__Host-dal_obscura_csrf={login.cookies['__Host-dal_obscura_csrf']}"
    )
    session = client.get("/v1/session", headers={"cookie": cookie_header})
    rejected = client.put(
        "/v1/assets/00000000-0000-0000-0000-000000000000/policy",
        json={"expected_revision": 0, "rules": []},
        headers={"cookie": cookie_header},
    )
    csrf = client.put(
        "/v1/assets/00000000-0000-0000-0000-000000000000/policy",
        json={"expected_revision": 0, "rules": []},
        headers={"cookie": cookie_header, "x-csrf-token": login.cookies["__Host-dal_obscura_csrf"]},
    )

    assert session.status_code == 200
    assert session.json()["principal"] == "asset-owner"
    assert rejected.status_code == 403
    assert rejected.json()["detail"] == "CSRF validation failed"
    # A valid CSRF token reaches the resource authorization boundary. The
    # actor has no visibility of this synthetic asset, so the API deliberately
    # conceals it as a not-found response rather than leaking its existence.
    assert csrf.status_code == 404
    assert csrf.json()["detail"] != "CSRF validation failed"


def test_cookie_session_rejects_a_forged_csrf_cookie(http_client, db_engine):
    client = _client_with_ui_auth_config(http_client, db_engine)
    login = _login_as_asset_owner(client)
    cookie_header = (
        f"__Host-dal_obscura_session={login.cookies['__Host-dal_obscura_session']}; "
        "__Host-dal_obscura_csrf=forged"
    )

    response = client.get("/v1/session", headers={"cookie": cookie_header})

    assert response.status_code == 403
    assert response.json()["detail"] == "CSRF validation failed"


def test_legacy_browser_cookies_are_not_authenticated(http_client, db_engine):
    client = _client_with_ui_auth_config(http_client, db_engine)
    response = client.get(
        "/v1/session", headers={"cookie": "dal_obscura_session=forged; dal_obscura_csrf=forged"}
    )

    assert response.status_code == 401


def test_cookie_session_logout_requires_csrf_and_expires_browser_cookies(http_client, db_engine):
    client = _client_with_ui_auth_config(http_client, db_engine)
    login = _login_as_asset_owner(client)
    cookie_header = (
        f"__Host-dal_obscura_session={login.cookies['__Host-dal_obscura_session']}; "
        f"__Host-dal_obscura_csrf={login.cookies['__Host-dal_obscura_csrf']}"
    )

    rejected = client.post("/v1/logout", headers={"cookie": cookie_header})
    logout = client.post(
        "/v1/logout",
        headers={"cookie": cookie_header, "x-csrf-token": login.cookies["__Host-dal_obscura_csrf"]},
    )

    assert rejected.status_code == 403
    assert logout.status_code == 200
    assert logout.json()["authenticated"] is False
    assert urlsplit(logout.json()["logout_url"]).path.endswith("/protocol/openid-connect/logout")
    cookies = logout.headers.get_list("set-cookie")
    assert any('__Host-dal_obscura_session=""' in cookie for cookie in cookies)
    assert any('__Host-dal_obscura_csrf=""' in cookie for cookie in cookies)
    assert client.get("/v1/session", headers={"cookie": cookie_header}).status_code == 401

    repeated = client.post(
        "/v1/logout",
        headers={"cookie": cookie_header, "x-csrf-token": login.cookies["__Host-dal_obscura_csrf"]},
    )
    assert repeated.status_code == 200
    assert repeated.json() == logout.json()


@pytest.mark.parametrize(
    ("extra_headers", "expected_status"),
    [
        ({}, 200),
        ({"origin": "http://127.0.0.1:8820"}, 200),
        (
            {"origin": "http://127.0.0.1:8820", "x-forwarded-host": "attacker.example"},
            200,
        ),
        ({"host": "attacker.example"}, 403),
        ({"host": "attacker.example", "origin": "http://attacker.example"}, 403),
        ({"origin": "https://attacker.example"}, 403),
        (
            {"host": "127.0.0.1:8820", "origin": "http://127.0.0.1:8820"},
            200,
        ),
    ],
)
def test_cookie_mutation_origin_and_host_matrix(
    http_client, db_engine, extra_headers: dict[str, str], expected_status: int
) -> None:
    client = _client_with_ui_auth_config(http_client, db_engine)
    login = _login_as_asset_owner(client)
    headers = {
        "cookie": (
            f"__Host-dal_obscura_session={login.cookies['__Host-dal_obscura_session']}; "
            f"__Host-dal_obscura_csrf={login.cookies['__Host-dal_obscura_csrf']}"
        ),
        "x-csrf-token": login.cookies["__Host-dal_obscura_csrf"],
        **extra_headers,
    }

    response = client.post("/v1/logout", headers=headers)

    assert response.status_code == expected_status
    if expected_status == 403:
        assert response.json()["detail"] == "Origin validation failed"


def test_static_bootstrap_token_can_be_disabled_when_oidc_admin_is_available(
    http_client, db_engine
):
    from dal_obscura.adapters.sql.database.db import (
        session_factory,
    )
    from dal_obscura.bootstrap.control import build_control_app

    engine = db_engine
    client = http_client(
        build_control_app(
            session_factory(engine),
            admin_token="test-admin",
            oidc_actor_resolver=_actor_for_token,
            oidc_admin_group="platform-admins",
            bootstrap_enabled=False,
        )
    )

    assert client.get("/v1/session", headers=ADMIN_HEADERS).status_code == 401
    oidc_admin = client.get("/v1/session", headers=_bearer("admin-oidc-token"))
    assert oidc_admin.status_code == 200
    assert oidc_admin.json()["platform_admin"] is True


def test_ui_auth_config_is_404_when_browser_oidc_is_not_configured(client_factory):
    client = _client(client_factory)

    response = client.get("/v1/ui-auth-config")

    assert response.status_code == 404


def test_session_reports_admin_token_actor(client_factory):
    client = _client(client_factory)

    response = client.get("/v1/session", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["cross-origin-opener-policy"] == "same-origin"
    assert "camera=()" in response.headers["permissions-policy"]
    csp = response.headers["content-security-policy"]
    assert "unsafe-eval" not in csp
    assert "script-src 'self'" in csp
    assert "frame-ancestors 'none'" in csp
    assert response.json() == {
        "principal": "platform:admin",
        "groups": [],
        "platform_admin": True,
        "capabilities": ["workspace:admin"],
    }


def test_secure_requests_include_transport_isolation_headers(client_factory):
    response = _client(client_factory, secure=True).get("/v1/session", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.headers["strict-transport-security"] == "max-age=31536000; includeSubDomains"


def test_local_bootstrap_login_exchanges_bearer_for_browser_session(client_factory):
    client = _client(client_factory, secure=True)

    response = client.post("/v1/session/bootstrap", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json() == {"authenticated": True}
    assert response.cookies["__Host-dal_obscura_session"]
    assert response.cookies["__Host-dal_obscura_csrf"]
    assert "secure" in response.headers["set-cookie"].lower()
    session = client.get("/v1/session")
    assert session.status_code == 200
    assert session.json()["principal"] == "platform:admin"


def test_local_bootstrap_login_rejects_invalid_bearer(client_factory):
    client = _client(client_factory)

    response = client.post("/v1/session/bootstrap", headers={"authorization": "Bearer wrong"})

    assert response.status_code == 401
    payload = response.json()
    assert payload["detail"] == "Invalid bootstrap credential"
    assert payload["error"]["code"] == "authentication_required"
    assert payload["error"]["request_id"]


def test_local_bootstrap_login_is_unavailable_when_disabled(http_client, db_engine):
    engine = db_engine
    client = http_client(
        build_control_app(
            session_factory(engine), admin_token="test-admin", bootstrap_enabled=False
        )
    )

    response = client.post("/v1/session/bootstrap", headers=ADMIN_HEADERS)

    assert response.status_code == 404
    payload = response.json()
    assert payload["detail"] == "Local bootstrap login is disabled"
    assert payload["error"]["code"] == "not_found"
    assert payload["error"]["request_id"]


def test_session_options_disclose_only_enabled_login_methods(client_factory):
    client = _client(client_factory)

    response = client.get("/v1/session/options")

    assert response.status_code == 200
    assert response.json() == {"bootstrap_enabled": True, "oidc": None}


def test_session_reports_oidc_actor_and_platform_admin_group(client_factory):
    client = _client(client_factory)

    owner = client.get("/v1/session", headers=_bearer("owner-token"))
    admin = client.get("/v1/session", headers=_bearer("admin-oidc-token"))

    assert owner.status_code == 200
    assert owner.json() == {
        "principal": "asset-owner",
        "groups": ["asset-owners"],
        "platform_admin": False,
        "capabilities": [],
    }
    assert admin.status_code == 200
    assert admin.json() == {
        "principal": "demo-admin",
        "groups": ["platform-admins"],
        "platform_admin": True,
        "capabilities": ["workspace:admin"],
    }


def test_oidc_actor_resolver_builds_actor_from_validated_token(monkeypatch):
    class FakeProvider:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def authenticate(self, request: AuthenticationRequest) -> Principal:
            assert request.headers == {"authorization": "Bearer token-123"}
            return Principal(id="asset-owner", groups=["asset-owners"], attributes={})

    monkeypatch.setattr(http_session, "OidcJwksIdentityProvider", FakeProvider)

    resolver = create_oidc_actor_resolver(
        issuer="http://keycloak:8080/realms/demo",
        audience="dal-obscura",
        jwks_url="http://keycloak:8080/realms/demo/protocol/openid-connect/certs",
        subject_claim="preferred_username",
        group_claims=("groups",),
    )

    assert resolver("token-123") == {"principal": "asset-owner", "groups": ["asset-owners"]}


def test_oidc_actor_resolver_preserves_validated_issuer_scope(monkeypatch):
    class FakeProvider:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def authenticate(self, request: AuthenticationRequest) -> Principal:
            return Principal(
                id="asset-owner",
                groups=["asset-owners"],
                attributes={},
                issuer="https://issuer.example/realms/demo",
            )

    monkeypatch.setattr(http_session, "OidcJwksIdentityProvider", FakeProvider)

    resolver = create_oidc_actor_resolver(
        issuer="https://issuer.example/realms/demo",
        audience="dal-obscura",
        jwks_url="https://issuer.example/realms/demo/certs",
        subject_claim="preferred_username",
        group_claims=("groups",),
    )

    assert resolver("token-123") == {
        "principal": "asset-owner",
        "groups": ["asset-owners"],
        "issuer": "https://issuer.example/realms/demo",
    }
