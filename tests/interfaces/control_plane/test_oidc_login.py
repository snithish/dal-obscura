from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.control_plane.interfaces import api as api_module
from dal_obscura.control_plane.interfaces.api import create_app
from dal_obscura.control_plane.interfaces.routes.session import _post_login_redirect


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
        )
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
    assert "dal_obscura_auth_state" in start.headers.get("set-cookie", "")

    callback = client.get(
        "/auth/callback",
        params={"code": "auth-code", "state": state},
        follow_redirects=False,
    )

    assert callback.status_code == 303
    assert callback.headers["location"] == "http://testserver/"
    assert exchanges[0][0] == "auth-code"
    assert len(exchanges[0][1]) >= 43
    session_cookie = client.cookies["dal_obscura_session"]
    assert session_cookie not in {"access-token", "id-token"}
    assert 'dal_obscura_auth_state=""' in callback.headers.get("set-cookie", "")
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
    ],
)
def test_login_redirect_rejects_external_or_ambiguous_destination(configured: str) -> None:
    with pytest.raises(HTTPException, match="post-login redirect"):
        _post_login_redirect(
            {"post_login_redirect_uri": configured},
            "https://gateway.example/auth/callback",
        )
