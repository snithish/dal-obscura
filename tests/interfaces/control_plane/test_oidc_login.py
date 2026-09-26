from __future__ import annotations

from email.message import Message
from io import BytesIO
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.control_plane.interfaces import api as api_module
from dal_obscura.control_plane.interfaces.api import create_app
from dal_obscura.control_plane.interfaces.routes import session as session_routes
from dal_obscura.control_plane.interfaces.routes.session import _oidc_endpoint, _post_login_redirect
from dal_obscura.control_plane.interfaces.session_api import exchange_authorization_code


def _client(nonce_resolver) -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    return TestClient(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            oidc_nonce_actor_resolver=nonce_resolver,
            ui_auth_config={
                "authority": "https://issuer.example/realms/demo",
                "client_id": "dal-obscura-ui",
                "redirect_uri": "http://testserver/auth/callback",
                "post_login_redirect_uri": "http://testserver/",
                "scope": "openid profile",
            },
        ),
        base_url="https://testserver",
    )


def test_oidc_login_uses_state_pkce_nonce_and_opaque_session(monkeypatch) -> None:
    exchanges: list[tuple[str, str]] = []

    def exchange(config, code, verifier):
        exchanges.append((code, verifier))
        return {"access_token": "access-token", "id_token": "id-token"}

    monkeypatch.setattr(api_module, "_exchange_authorization_code", exchange)
    client = _client(lambda token, nonce_hash: {"principal": "alice", "groups": ["analysts"]})

    start = client.get("/auth/login", follow_redirects=False)
    location = start.headers["location"]
    query = parse_qs(urlsplit(location).query)
    state = query["state"][0]

    assert start.status_code == 303
    assert query["response_type"] == ["code"]
    assert query["code_challenge_method"] == ["S256"]
    assert len(query["code_challenge"][0]) == 43
    assert query["nonce"][0]
    assert "__Host-dal_obscura_auth_state" in start.headers.get("set-cookie", "")

    callback = client.get(
        "/auth/callback",
        params={"code": "auth-code", "state": state},
        follow_redirects=False,
    )

    assert callback.status_code == 303
    assert callback.headers["location"] == "http://testserver/"
    assert exchanges[0][0] == "auth-code"
    assert len(exchanges[0][1]) >= 43
    session_cookie = client.cookies["__Host-dal_obscura_session"]
    assert session_cookie not in {"access-token", "id-token"}
    assert '__Host-dal_obscura_auth_state=""' in callback.headers.get("set-cookie", "")
    session = client.get("/v1/session")
    assert session.status_code == 200
    assert session.json()["principal"] == "alice"


def test_oidc_callback_rejects_state_replay_and_nonce_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        api_module,
        "_exchange_authorization_code",
        lambda config, code, verifier: {"access_token": "access", "id_token": "id"},
    )
    client = _client(lambda token, nonce_hash: None)
    start = client.get("/auth/login", follow_redirects=False)
    state = parse_qs(urlsplit(start.headers["location"]).query)["state"][0]

    nonce_failure = client.get(
        "/auth/callback",
        params={"code": "code", "state": state},
        follow_redirects=False,
    )
    replay = client.get(
        "/auth/callback",
        params={"code": "code", "state": state},
        follow_redirects=False,
    )

    assert nonce_failure.status_code == 401
    assert replay.status_code == 400


def test_oidc_login_is_bounded_per_client_and_returns_retry_after() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    client = TestClient(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            ui_auth_config={
                "authority": "https://issuer.example/realms/demo",
                "client_id": "dal-obscura-ui",
                "redirect_uri": "http://testserver/auth/callback",
            },
            login_rate_limit_attempts=2,
            login_rate_limit_window_seconds=60,
            login_rate_limit_block_seconds=30,
        )
    )

    assert client.get("/auth/login", follow_redirects=False).status_code == 303
    assert client.get("/auth/login", follow_redirects=False).status_code == 303
    blocked = client.get("/auth/login", follow_redirects=False)

    assert blocked.status_code == 429
    assert blocked.headers["retry-after"] == "30"
    blocked_payload = blocked.json()
    assert blocked_payload["detail"] == "Login temporarily unavailable"
    assert blocked_payload["error"]["code"] == "rate_limited"
    assert blocked_payload["error"]["request_id"]


def _request_with_peer(peer: str, forwarded: str | None = None) -> Request:
    headers = [] if forwarded is None else [(b"x-forwarded-for", forwarded.encode())]
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/auth/login",
            "headers": headers,
            "client": (peer, 443),
            "scheme": "https",
            "server": ("gateway.example", 443),
        }
    )


def test_forwarded_login_rate_identity_requires_configured_proxy_peer() -> None:
    trusted = _request_with_peer("10.0.0.8", "198.51.100.7")
    spoofed = _request_with_peer("198.51.100.8", "198.51.100.7")
    malformed = _request_with_peer("10.0.0.8", "not-an-ip")

    assert session_routes._rate_key_for_request(trusted, ("10.0.0.8/32",)) == (
        "client:198.51.100.7",
        "aggregate:10.0.0.8",
    )
    assert session_routes._rate_key_for_request(spoofed, ("10.0.0.8/32",)) == (
        "client:198.51.100.8",
        None,
    )
    assert session_routes._rate_key_for_request(malformed, ("10.0.0.8/32",)) == (
        "client:10.0.0.8",
        None,
    )


def test_trusted_gateway_keeps_per_client_and_aggregate_login_budgets() -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    app = create_app(
        session_factory(engine),
        admin_token="test-admin",
        ui_auth_config={
            "authority": "https://issuer.example/realms/demo",
            "client_id": "dal-obscura-ui",
            "redirect_uri": "http://testserver/auth/callback",
        },
        login_rate_limit_attempts=10,
        login_rate_limit_aggregate_attempts=1,
        trusted_proxy_peers=("10.0.0.8/32",),
    )
    first_client = TestClient(
        app,
        base_url="https://testserver",
        client=("10.0.0.8", 443),
        headers={"x-forwarded-for": "198.51.100.7"},
    )
    second_client = TestClient(
        app,
        client=("10.0.0.8", 443),
        headers={"x-forwarded-for": "198.51.100.8"},
    )

    assert first_client.get("/auth/login", follow_redirects=False).status_code == 303
    blocked = second_client.get("/auth/login", follow_redirects=False)

    assert blocked.status_code == 429


@pytest.mark.parametrize("peer", ["gateway.internal", "10.0.0.8/not-a-mask", ""])
def test_invalid_trusted_proxy_configuration_fails_closed(peer: str) -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)

    with pytest.raises(ValueError, match="trusted proxy peer"):
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            trusted_proxy_peers=(peer,),
        )


def test_successful_oidc_callback_clears_client_login_limit(monkeypatch) -> None:
    monkeypatch.setattr(
        api_module,
        "_exchange_authorization_code",
        lambda config, code, verifier: {"access_token": "access", "id_token": "id"},
    )
    client = _client(lambda token, nonce_hash: {"principal": "alice", "groups": []})

    start = client.get("/auth/login", follow_redirects=False)
    state = parse_qs(urlsplit(start.headers["location"]).query)["state"][0]
    callback = client.get(
        "/auth/callback",
        params={"code": "code", "state": state},
        follow_redirects=False,
    )

    assert callback.status_code == 303
    # A successful callback resets the client window, so the default limiter
    # can immediately admit another login start.
    assert client.get("/auth/login", follow_redirects=False).status_code == 303


def test_successful_oidc_callback_does_not_clear_shared_gateway_budget(monkeypatch) -> None:
    monkeypatch.setattr(
        api_module,
        "_exchange_authorization_code",
        lambda config, code, verifier: {"access_token": "access", "id_token": "id"},
    )
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    app = create_app(
        session_factory(engine),
        admin_token="test-admin",
        oidc_nonce_actor_resolver=lambda token, nonce_hash: {
            "principal": "alice",
            "groups": [],
        },
        ui_auth_config={
            "authority": "https://issuer.example/realms/demo",
            "client_id": "dal-obscura-ui",
            "redirect_uri": "http://testserver/auth/callback",
        },
        login_rate_limit_attempts=10,
        login_rate_limit_aggregate_attempts=1,
        trusted_proxy_peers=("10.0.0.8/32",),
    )
    first_client = TestClient(
        app,
        base_url="https://testserver",
        client=("10.0.0.8", 443),
        headers={"x-forwarded-for": "198.51.100.7"},
    )
    second_client = TestClient(
        app,
        client=("10.0.0.8", 443),
        headers={"x-forwarded-for": "198.51.100.8"},
    )

    start = first_client.get("/auth/login", follow_redirects=False)
    state = parse_qs(urlsplit(start.headers["location"]).query)["state"][0]
    callback = first_client.get(
        "/auth/callback",
        params={"code": "code", "state": state},
        follow_redirects=False,
    )

    assert callback.status_code == 303
    assert second_client.get("/auth/login", follow_redirects=False).status_code == 429


def test_login_redirect_does_not_reuse_logout_destination() -> None:
    assert (
        _post_login_redirect(
            {"post_logout_redirect_uri": "https://issuer.example/logout"},
            "https://gateway.example/auth/callback",
        )
        == "https://gateway.example/"
    )


@pytest.mark.parametrize(
    "configured",
    [
        "https://attacker.example/",
        "https://gateway.example/complete?next=https://attacker.example",
        "https://user:pass@gateway.example/",
        "https://[broken/complete",
    ],
)
def test_login_redirect_rejects_external_or_ambiguous_destination(configured: str) -> None:
    with pytest.raises(HTTPException, match="post-login redirect"):
        _post_login_redirect(
            {"post_login_redirect_uri": configured},
            "https://gateway.example/auth/callback",
        )


@pytest.mark.parametrize(
    "redirect_uri", ["https://[broken/callback", "https://gateway.example:99999/callback"]
)
def test_login_redirect_rejects_malformed_callback_configuration(redirect_uri: str) -> None:
    with pytest.raises(HTTPException, match="UI redirect URI is invalid"):
        _post_login_redirect({}, redirect_uri)


@pytest.mark.parametrize(
    "authorization_endpoint",
    [
        "https://issuer.example/auth?next=https://attacker.example",
        "https://user:pass@issuer.example/auth",
        "https://issuer.example:99999/auth",
        "file:///tmp/oidc-auth",
        "https://issuer.example/auth#fragment",
    ],
)
def test_oidc_authorization_endpoint_rejects_ambiguous_configuration(
    authorization_endpoint: str,
) -> None:
    with pytest.raises(HTTPException, match="authorization_endpoint is invalid"):
        _oidc_endpoint(
            {"authorization_endpoint": authorization_endpoint},
            "authorization_endpoint",
            "/protocol/openid-connect/auth",
        )


def test_oidc_authorization_endpoint_resolves_valid_authority() -> None:
    assert (
        _oidc_endpoint(
            {"authority": "https://issuer.example/realms/demo"},
            "authorization_endpoint",
            "/protocol/openid-connect/auth",
        )
        == "https://issuer.example/realms/demo/protocol/openid-connect/auth"
    )


def test_authorization_code_exchange_uses_canonical_token_endpoint(monkeypatch) -> None:
    seen: dict[str, object] = {}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"access_token":"access","id_token":"id"}'

    def open_token(request):
        seen["url"] = request.full_url
        return _Response()

    monkeypatch.setattr(
        "dal_obscura.control_plane.interfaces.session_api._open_token_endpoint",
        open_token,
    )

    result = exchange_authorization_code(
        {
            "authority": "https://issuer.example/realms/demo",
            "client_id": "governance-ui",
            "redirect_uri": "https://gateway.example/auth/callback",
        },
        "code",
        "verifier",
    )

    assert result["access_token"] == "access"
    assert seen == {
        "url": "https://issuer.example/realms/demo/protocol/openid-connect/token",
    }


def test_authorization_code_exchange_logs_safe_upstream_error_metadata(monkeypatch, caplog) -> None:
    def reject_exchange(_request):
        raise HTTPError(
            "https://issuer.example/token",
            400,
            "Bad Request",
            hdrs=Message(),
            fp=BytesIO(b'{"error":"invalid_grant","error_description":"private upstream details"}'),
        )

    monkeypatch.setattr(
        "dal_obscura.control_plane.interfaces.session_api._open_token_endpoint",
        reject_exchange,
    )

    with pytest.raises(HTTPException, match="OIDC code exchange failed"):
        exchange_authorization_code(
            {
                "token_endpoint": "https://issuer.example/token",
                "client_id": "ui",
                "redirect_uri": "https://gateway.example/callback",
            },
            "authorization-code-value",
            "pkce-verifier-value",
        )

    record = next(
        record for record in caplog.records if record.message == "oidc_code_exchange_rejected"
    )
    assert record.upstream_status == 400
    assert record.upstream_error == "invalid_grant"
    assert record.exception_type == "HTTPError"
    assert "private upstream details" not in caplog.text
    assert "authorization-code-value" not in caplog.text
    assert "pkce-verifier-value" not in caplog.text


@pytest.mark.parametrize(
    "token_endpoint", ["https://[broken/token", "https://issuer.example:99999/token"]
)
def test_authorization_code_exchange_rejects_malformed_token_endpoint(token_endpoint: str) -> None:
    with pytest.raises(HTTPException, match="token endpoint is invalid"):
        exchange_authorization_code(
            {
                "token_endpoint": token_endpoint,
                "client_id": "ui",
                "redirect_uri": "https://gateway.example/callback",
            },
            "code",
            "verifier",
        )
