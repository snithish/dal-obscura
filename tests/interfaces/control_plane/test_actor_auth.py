from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from fastapi.testclient import TestClient

from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.control_plane.interfaces import api as api_module
from dal_obscura.control_plane.interfaces.api import create_app, create_oidc_actor_resolver
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest

ADMIN_HEADERS = {"authorization": "Bearer test-admin"}
ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
OIDC_AUTH_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
)


@dataclass(frozen=True)
class DemoToken:
    principal: str
    groups: tuple[str, ...] = ()


def _actor_for_token(token: str) -> DemoToken:
    if token == "owner-token":
        return DemoToken("asset-owner", ("asset-owners",))
    if token == "grant-manager-token":
        return DemoToken("grant-manager", ())
    if token == "outsider-token":
        return DemoToken("outsider", ("analysts",))
    if token == "admin-oidc-token":
        return DemoToken("demo-admin", ("platform-admins",))
    if token == "editor-token":
        return DemoToken("editor")
    if token == "publisher-token":
        return DemoToken("publisher")
    raise PermissionError("bad token")


def _client() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    return TestClient(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            oidc_actor_resolver=_actor_for_token,
            oidc_admin_group="platform-admins",
        )
    )


def _save_policy_draft(client: TestClient, asset_id: UUID, rules: list[dict], headers) -> object:
    current = client.get(f"/v1/assets/{asset_id}/draft", headers=headers)
    assert current.status_code == 200, current.text
    return client.put(
        f"/v1/assets/{asset_id}/draft",
        json={"expected_revision": current.json()["revision"], "rules": rules},
        headers=headers,
    )


def _client_with_ui_auth_config() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    return TestClient(
        create_app(
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
                "login_shortcuts": [
                    {"label": "Platform owner", "login_hint": "demo-admin"},
                    {"label": "Data asset owner", "login_hint": "asset-owner"},
                    {"label": "Broken", "login_hint": ""},
                ],
                "demo_login": {
                    "token_url": "http://keycloak/token",
                    "client_id": "dal-obscura-cli",
                    "client_secret": "secret",
                    "passwords": {
                        "demo-admin": "admin-pass",
                        "asset-owner": "owner-pass",
                    },
                },
            },
        )
    )


def _bearer(token: str) -> dict[str, str]:
    return {"authorization": f"Bearer {token}"}


def test_ui_auth_config_returns_public_oidc_browser_config_without_secret():
    client = _client_with_ui_auth_config()

    response = client.get("/v1/ui-auth-config")

    assert response.status_code == 200
    assert response.json() == {
        "authority": "http://127.0.0.1:8080/realms/dal-obscura-demo",
        "client_id": "dal-obscura-ui",
        "redirect_uri": "http://127.0.0.1:8820/ui/auth/callback",
        "post_logout_redirect_uri": "http://127.0.0.1:8820/ui",
        "scope": "openid profile",
        "login_shortcuts": [
            {
                "label": "Platform owner",
                "login_hint": "demo-admin",
                "demo_login_path": "/v1/demo-login",
            },
            {
                "label": "Data asset owner",
                "login_hint": "asset-owner",
                "demo_login_path": "/v1/demo-login",
            },
        ],
    }


def test_demo_login_sets_http_only_session_and_csrf_cookies(monkeypatch):
    client = _client_with_ui_auth_config()
    calls = []

    def fake_exchange(config, username):
        calls.append((config["token_url"], config["client_id"], username))
        return "owner-token"

    monkeypatch.setattr(api_module, "_exchange_demo_password_token", fake_exchange)

    response = client.post("/v1/demo-login", json={"login_hint": "asset-owner"})

    assert response.status_code == 200
    assert response.json() == {"authenticated": True}
    assert response.cookies["dal_obscura_session"] != "owner-token"
    assert len(response.cookies["dal_obscura_session"]) >= 40
    assert response.cookies["dal_obscura_csrf"]
    session_cookie = next(
        cookie
        for cookie in response.headers.get_list("set-cookie")
        if "dal_obscura_session" in cookie
    )
    assert "HttpOnly" in session_cookie
    assert "samesite=lax" in session_cookie.lower()
    assert calls == [("http://keycloak/token", "dal-obscura-cli", "asset-owner")]


def test_cookie_session_requires_csrf_header_for_mutations(monkeypatch):
    client = _client_with_ui_auth_config()
    monkeypatch.setattr(
        api_module,
        "_exchange_demo_password_token",
        lambda config, username: "owner-token",
    )
    login = client.post("/v1/demo-login", json={"login_hint": "asset-owner"})

    cookie_header = (
        f"dal_obscura_session={login.cookies['dal_obscura_session']}; "
        f"dal_obscura_csrf={login.cookies['dal_obscura_csrf']}"
    )
    session = client.get("/v1/session", headers={"cookie": cookie_header})
    rejected = client.put(
        "/v1/assets/00000000-0000-0000-0000-000000000000/draft",
        json={"expected_revision": 0, "rules": []},
        headers={"cookie": cookie_header},
    )
    csrf = client.put(
        "/v1/assets/00000000-0000-0000-0000-000000000000/draft",
        json={"expected_revision": 0, "rules": []},
        headers={"cookie": cookie_header, "x-csrf-token": login.cookies["dal_obscura_csrf"]},
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


def test_cookie_session_rejects_a_forged_csrf_cookie(monkeypatch):
    client = _client_with_ui_auth_config()
    monkeypatch.setattr(
        api_module,
        "_exchange_demo_password_token",
        lambda config, username: "owner-token",
    )
    login = client.post("/v1/demo-login", json={"login_hint": "asset-owner"})
    cookie_header = (
        f"dal_obscura_session={login.cookies['dal_obscura_session']}; dal_obscura_csrf=forged"
    )

    response = client.get("/v1/session", headers={"cookie": cookie_header})

    assert response.status_code == 403
    assert response.json()["detail"] == "CSRF validation failed"


def test_cookie_session_logout_requires_csrf_and_expires_browser_cookies(monkeypatch):
    client = _client_with_ui_auth_config()
    monkeypatch.setattr(
        api_module,
        "_exchange_demo_password_token",
        lambda config, username: "owner-token",
    )
    login = client.post("/v1/demo-login", json={"login_hint": "asset-owner"})
    cookie_header = (
        f"dal_obscura_session={login.cookies['dal_obscura_session']}; "
        f"dal_obscura_csrf={login.cookies['dal_obscura_csrf']}"
    )

    rejected = client.post("/v1/logout", headers={"cookie": cookie_header})
    logout = client.post(
        "/v1/logout",
        headers={"cookie": cookie_header, "x-csrf-token": login.cookies["dal_obscura_csrf"]},
    )

    assert rejected.status_code == 403
    assert logout.status_code == 200
    assert logout.json() == {"authenticated": False}
    cookies = logout.headers.get_list("set-cookie")
    assert any('dal_obscura_session=""' in cookie for cookie in cookies)
    assert any('dal_obscura_csrf=""' in cookie for cookie in cookies)
    assert client.get("/v1/session", headers={"cookie": cookie_header}).status_code == 401

    repeated = client.post(
        "/v1/logout",
        headers={"cookie": cookie_header, "x-csrf-token": login.cookies["dal_obscura_csrf"]},
    )
    assert repeated.status_code == 200
    assert repeated.json() == {"authenticated": False}


def test_cookie_mutation_rejects_untrusted_origin(monkeypatch):
    client = _client_with_ui_auth_config()
    monkeypatch.setattr(
        api_module,
        "_exchange_demo_password_token",
        lambda config, username: "owner-token",
    )
    login = client.post("/v1/demo-login", json={"login_hint": "asset-owner"})
    cookie_header = (
        f"dal_obscura_session={login.cookies['dal_obscura_session']}; "
        f"dal_obscura_csrf={login.cookies['dal_obscura_csrf']}"
    )

    response = client.post(
        "/v1/logout",
        headers={
            "cookie": cookie_header,
            "x-csrf-token": login.cookies["dal_obscura_csrf"],
            "origin": "https://attacker.example",
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Origin validation failed"


def test_cookie_mutation_cannot_trust_forged_host_and_matching_origin(monkeypatch):
    client = _client_with_ui_auth_config()
    monkeypatch.setattr(
        api_module,
        "_exchange_demo_password_token",
        lambda config, username: "owner-token",
    )
    login = client.post("/v1/demo-login", json={"login_hint": "asset-owner"})
    cookie_header = (
        f"dal_obscura_session={login.cookies['dal_obscura_session']}; "
        f"dal_obscura_csrf={login.cookies['dal_obscura_csrf']}"
    )

    response = client.post(
        "/v1/logout",
        headers={
            "cookie": cookie_header,
            "x-csrf-token": login.cookies["dal_obscura_csrf"],
            "host": "attacker.example",
            "origin": "http://attacker.example",
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Origin validation failed"


def test_demo_login_rejects_unknown_shortcut():
    client = _client_with_ui_auth_config()

    response = client.post("/v1/demo-login", json={"login_hint": "us-analyst"})

    assert response.status_code == 404


def test_static_bootstrap_token_can_be_disabled_when_oidc_admin_is_available():
    from dal_obscura.common.config_store.db import (
        create_engine_from_url,
        migrate_config_store,
        session_factory,
    )
    from dal_obscura.control_plane.interfaces.api import create_app

    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    client = TestClient(
        create_app(
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


def test_ui_auth_config_is_404_when_browser_oidc_is_not_configured():
    client = _client()

    response = client.get("/v1/ui-auth-config")

    assert response.status_code == 404


def test_session_reports_admin_token_actor():
    client = _client()

    response = client.get("/v1/session", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.json() == {
        "principal": "platform:admin",
        "groups": [],
        "platform_admin": True,
    }


def test_local_bootstrap_login_exchanges_bearer_for_browser_session():
    client = _client()

    response = client.post("/v1/session/bootstrap", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json() == {"authenticated": True}
    assert response.cookies["dal_obscura_session"]
    assert response.cookies["dal_obscura_csrf"]
    session = client.get("/v1/session")
    assert session.status_code == 200
    assert session.json()["principal"] == "platform:admin"


def test_local_bootstrap_login_rejects_invalid_bearer():
    client = _client()

    response = client.post("/v1/session/bootstrap", headers={"authorization": "Bearer wrong"})

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid bootstrap credential"}


def test_local_bootstrap_login_is_unavailable_when_disabled():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    client = TestClient(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            bootstrap_enabled=False,
        )
    )

    response = client.post("/v1/session/bootstrap", headers=ADMIN_HEADERS)

    assert response.status_code == 404
    assert response.json() == {"detail": "Local bootstrap login is disabled"}


def test_session_options_disclose_only_enabled_login_methods():
    client = _client()

    response = client.get("/v1/session/options")

    assert response.status_code == 200
    assert response.json() == {"bootstrap_enabled": True, "oidc": None}


def test_session_reports_oidc_actor_and_platform_admin_group():
    client = _client()

    owner = client.get("/v1/session", headers=_bearer("owner-token"))
    admin = client.get("/v1/session", headers=_bearer("admin-oidc-token"))

    assert owner.status_code == 200
    assert owner.json() == {
        "principal": "asset-owner",
        "groups": ["asset-owners"],
        "platform_admin": False,
    }
    assert admin.status_code == 200
    assert admin.json() == {
        "principal": "demo-admin",
        "groups": ["platform-admins"],
        "platform_admin": True,
    }


def test_oidc_actor_resolver_builds_actor_from_validated_token(monkeypatch):
    class FakeProvider:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def authenticate(self, request: AuthenticationRequest) -> Principal:
            assert request.headers == {"authorization": "Bearer token-123"}
            return Principal(id="asset-owner", groups=["asset-owners"], attributes={})

    monkeypatch.setattr(api_module, "OidcJwksIdentityProvider", FakeProvider)

    resolver = create_oidc_actor_resolver(
        issuer="http://keycloak:8080/realms/demo",
        audience="dal-obscura",
        jwks_url="http://keycloak:8080/realms/demo/protocol/openid-connect/certs",
        subject_claim="preferred_username",
        group_claims=("groups",),
    )

    assert resolver("token-123") == {
        "principal": "asset-owner",
        "groups": ["asset-owners"],
    }


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

    monkeypatch.setattr(api_module, "OidcJwksIdentityProvider", FakeProvider)

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


def test_asset_owner_can_replace_policy_rules_through_api():
    client = _client()
    asset = _provision_owned_asset(client)

    response = _save_policy_draft(
        client, asset, [_allow_rule(row_filter="region = 'us'")], _bearer("owner-token")
    )

    assert response.status_code == 200
    rules = client.get(f"/v1/assets/{asset}/draft", headers=_bearer("owner-token")).json()["rules"]
    assert rules[0]["row_filter"] == "region = 'us'"


def test_group_owner_can_publish_policy_version_through_api():
    client = _client()
    asset = _provision_owned_asset(client)
    grant = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [{"principal": "group:asset-owners", "capability": "publish"}],
            "expected_revision": 1,
        },
        headers=ADMIN_HEADERS,
    )
    _save_policy_draft(
        client, asset, [_allow_rule(row_filter="region = 'eu'")], _bearer("owner-token")
    )

    response = client.post(
        f"/v1/assets/{asset}/policy-versions",
        headers=_bearer("owner-token"),
    )

    assert grant.status_code == 200
    assert response.status_code == 200
    assert UUID(response.json()["asset_id"]) == asset
    assert response.json()["policy_version"] > 0


def test_publish_uses_saved_draft_and_rejects_stale_draft_revision():
    client = _client()
    asset = _provision_owned_asset(client)
    owner = _bearer("owner-token")
    grant = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [{"principal": "group:asset-owners", "capability": "publish"}],
            "expected_revision": 1,
        },
        headers=ADMIN_HEADERS,
    )

    saved = client.put(
        f"/v1/assets/{asset}/draft",
        json={"expected_revision": 0, "rules": [_allow_rule(row_filter="region = 'eu'")]},
        headers=owner,
    )
    published = client.post(
        f"/v1/assets/{asset}/policy-versions",
        json={"expected_draft_revision": 1},
        headers=owner,
    )
    stale = client.post(
        f"/v1/assets/{asset}/policy-versions",
        json={"expected_draft_revision": 0},
        headers=owner,
    )

    assert saved.status_code == 200
    assert grant.status_code == 200
    assert published.status_code == 200
    assert published.json()["policy_version"] != 0
    assert stale.status_code == 409


def test_non_owner_cannot_change_policy_rules_or_publish_policy_version():
    client = _client()
    asset = _provision_owned_asset(client)

    replace = client.put(
        f"/v1/assets/{asset}/draft",
        json={"expected_revision": 0, "rules": [_allow_rule(row_filter="region = 'us'")]},
        headers=_bearer("outsider-token"),
    )
    publish = client.post(
        f"/v1/assets/{asset}/policy-versions",
        headers=_bearer("outsider-token"),
    )

    assert replace.status_code == 403
    assert publish.status_code == 403


def test_non_owner_cannot_read_asset_policy_or_preview():
    client = _client()
    asset = _provision_owned_asset(client)

    inventory = client.get("/v1/assets", headers=_bearer("outsider-token"))
    detail = client.get(f"/v1/assets/{asset}", headers=_bearer("outsider-token"))
    rules = client.get(f"/v1/assets/{asset}/draft", headers=_bearer("outsider-token"))
    preview = client.post(
        f"/v1/assets/{asset}/policy-evaluate",
        json={"principal": "analyst", "groups": [], "claims": {}},
        headers=_bearer("outsider-token"),
    )

    assert inventory.status_code == 200
    assert inventory.json() == []
    assert detail.status_code == 403
    assert rules.status_code == 403
    assert preview.status_code == 403


def test_workspace_summary_is_scoped_to_visible_assets():
    client = _client()
    _provision_owned_asset(client)

    outsider = client.get("/v1/workspace/summary", headers=_bearer("outsider-token"))
    owner = client.get("/v1/workspace/summary", headers=_bearer("owner-token"))

    assert outsider.status_code == 200
    assert outsider.json()["asset_count"] == 0
    assert outsider.json()["catalog_count"] == 0
    assert owner.status_code == 200
    assert owner.json()["asset_count"] == 1
    assert owner.json()["enabled_auth_provider_count"] == 0


def test_policy_history_is_scoped_to_owned_assets():
    client = _client()
    asset = _provision_owned_asset(client)
    _save_policy_draft(client, asset, [_allow_rule(row_filter=None)], ADMIN_HEADERS)
    client.post(f"/v1/assets/{asset}/policy-versions", headers=ADMIN_HEADERS)

    outsider = client.get("/v1/policy-versions", headers=_bearer("outsider-token"))
    owner = client.get("/v1/policy-versions", headers=_bearer("owner-token"))

    assert outsider.status_code == 200
    assert outsider.json() == []
    assert owner.status_code == 200
    assert owner.json()[0]["asset_id"] == str(asset)


def test_non_admin_cannot_read_catalog_or_auth_settings():
    client = _client()

    catalogs = client.get("/v1/catalogs", headers=_bearer("outsider-token"))
    runtime = client.get("/v1/settings/runtime", headers=_bearer("outsider-token"))
    providers = client.get("/v1/settings/auth-providers", headers=_bearer("outsider-token"))

    assert catalogs.status_code == 403
    assert runtime.status_code == 403
    assert providers.status_code == 403


def test_asset_owner_can_delegate_read_without_edit_or_publish():
    client = _client()
    asset = _provision_owned_asset(client)
    grant = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [{"principal": "group:asset-owners", "capability": "grant"}],
            "expected_revision": 1,
        },
        headers=ADMIN_HEADERS,
    )
    grants = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [{"principal": "outsider", "capability": "read"}],
            "expected_revision": 2,
        },
        headers=_bearer("owner-token"),
    )

    inventory = client.get("/v1/assets", headers=_bearer("outsider-token"))
    detail = client.get(f"/v1/assets/{asset}", headers=_bearer("outsider-token"))
    rules = client.get(f"/v1/assets/{asset}/draft", headers=_bearer("outsider-token"))
    replace = client.put(
        f"/v1/assets/{asset}/draft",
        json={"expected_revision": 0, "rules": [_allow_rule(row_filter=None)]},
        headers=_bearer("outsider-token"),
    )

    assert grant.status_code == 200
    assert grants.status_code == 200
    assert grants.json()["grants"] == [{"principal": "outsider", "capability": "read"}]
    assert inventory.status_code == 200
    assert inventory.json()[0]["id"] == str(asset)
    assert detail.status_code == 200
    assert detail.json()["id"] == str(asset)
    assert rules.status_code == 200
    assert replace.status_code == 403


def test_asset_owner_cannot_delegate_grant_management():
    client = _client()
    asset = _provision_owned_asset(client)
    grant = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [{"principal": "group:asset-owners", "capability": "grant"}],
            "expected_revision": 1,
        },
        headers=ADMIN_HEADERS,
    )

    response = client.put(
        f"/v1/assets/{asset}/grants",
        json={"grants": [{"principal": "outsider", "capability": "grant"}]},
        headers=_bearer("owner-token"),
    )

    assert grant.status_code == 200
    assert response.status_code == 403
    assert "Only platform admins" in response.json()["detail"]


def test_grant_manager_cannot_self_escalate_but_can_delegate_held_authority():
    client = _client()
    asset = _provision_owned_asset(client)

    delegated = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [{"principal": "grant-manager", "capability": "grant"}],
            "expected_revision": 1,
        },
        headers=ADMIN_HEADERS,
    )
    self_escalation = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [
                {"principal": "grant-manager", "capability": "grant"},
                {"principal": "grant-manager", "capability": "edit"},
            ],
            "expected_revision": 2,
        },
        headers=_bearer("grant-manager-token"),
    )
    delegation = client.put(
        f"/v1/assets/{asset}/grants",
        json={
            "grants": [
                {"principal": "grant-manager", "capability": "grant"},
                {"principal": "outsider", "capability": "edit"},
            ],
            "expected_revision": 2,
        },
        headers=_bearer("grant-manager-token"),
    )

    assert delegated.status_code == 200
    assert self_escalation.status_code == 403
    assert "already hold" in self_escalation.json()["detail"]
    assert delegation.status_code == 200


def test_policy_save_rejects_invalid_row_filter_before_publish():
    client = _client()
    asset = _provision_owned_asset(client)

    response = _save_policy_draft(
        client, asset, [_allow_rule(row_filter="region =")], _bearer("owner-token")
    )

    assert response.status_code == 400
    assert "Invalid row_filter SQL" in response.json()["detail"]


def test_policy_save_rejects_deny_rule_with_mask_before_publish():
    client = _client()
    asset = _provision_owned_asset(client)
    rule = _allow_rule(row_filter=None)
    rule["effect"] = "deny"
    rule["masks"] = {"email": {"type": "email"}}

    response = _save_policy_draft(client, asset, [rule], _bearer("owner-token"))

    assert response.status_code == 400
    assert "Policy rules are explicit grants" in response.json()["detail"]


def test_platform_admin_can_assign_owner_and_bootstrap_policy():
    client = _client()
    asset = _provision_asset_without_owner(client)

    owners = client.put(
        f"/v1/assets/{asset}/owners",
        json={"owners": ["group:asset-owners"], "expected_revision": 0},
        headers=_bearer("admin-oidc-token"),
    )
    policy = _save_policy_draft(
        client, asset, [_allow_rule(row_filter=None)], _bearer("admin-oidc-token")
    )

    assert owners.status_code == 200
    assert owners.json()["owners"] == ["group:asset-owners"]
    assert policy.status_code == 200


def _provision_owned_asset(client: TestClient) -> UUID:
    asset = _provision_asset_without_owner(client)
    response = client.put(
        f"/v1/assets/{asset}/owners",
        json={"owners": ["group:asset-owners"], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    assert response.status_code == 200
    return asset


def _provision_asset_without_owner(client: TestClient) -> UUID:
    client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 900,
            "max_tickets": 64,
            "max_ticket_exchanges": 1,
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
        json={"backend": "iceberg", "table_identifier": "default.users", "options": {}},
        headers=ADMIN_HEADERS,
    ).json()
    client.put(
        "/v1/settings/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": OIDC_AUTH_MODULE,
                    "args": {
                        "issuer": "http://keycloak:8080/realms/dal-obscura-demo",
                        "audience": "dal-obscura",
                        "jwks_url": (
                            "http://keycloak:8080/realms/dal-obscura-demo/"
                            "protocol/openid-connect/certs"
                        ),
                        "subject_claim": "preferred_username",
                        "group_claims": ["groups"],
                    },
                    "enabled": True,
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    _save_policy_draft(client, asset["id"], [_allow_rule(row_filter=None)], ADMIN_HEADERS)
    return UUID(asset["id"])


def _allow_rule(*, row_filter: str | None) -> dict[str, object]:
    return {
        "ordinal": 10,
        "effect": "allow",
        "principals": ["group:analysts"],
        "when": {},
        "columns": ["id", "email", "region"],
        "masks": {"email": {"type": "email"}},
        "row_filter": row_filter,
    }
