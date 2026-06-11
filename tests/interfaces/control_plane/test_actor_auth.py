from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from fastapi.testclient import TestClient

from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
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
    if token == "outsider-token":
        return DemoToken("outsider", ("analysts",))
    if token == "admin-oidc-token":
        return DemoToken("demo-admin", ("platform-admins",))
    raise PermissionError("bad token")


def _client() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return TestClient(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            oidc_actor_resolver=_actor_for_token,
            oidc_admin_group="platform-admins",
        )
    )


def _client_with_ui_auth_config() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return TestClient(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
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


def test_demo_login_returns_token_for_configured_shortcut(monkeypatch):
    client = _client_with_ui_auth_config()
    calls = []

    def fake_exchange(config, username):
        calls.append((config["token_url"], config["client_id"], username))
        return f"token-for-{username}"

    monkeypatch.setattr(api_module, "_exchange_demo_password_token", fake_exchange)

    response = client.post("/v1/demo-login", json={"login_hint": "asset-owner"})

    assert response.status_code == 200
    assert response.json() == {"access_token": "token-for-asset-owner"}
    assert calls == [("http://keycloak/token", "dal-obscura-cli", "asset-owner")]


def test_demo_login_rejects_unknown_shortcut():
    client = _client_with_ui_auth_config()

    response = client.post("/v1/demo-login", json={"login_hint": "us-analyst"})

    assert response.status_code == 404


def test_ui_auth_config_is_404_when_browser_oidc_is_not_configured():
    client = _client()

    response = client.get("/v1/ui-auth-config")

    assert response.status_code == 404


def test_session_reports_admin_token_actor():
    client = _client()

    response = client.get("/v1/session", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json() == {
        "principal": "platform:admin",
        "groups": [],
        "platform_admin": True,
    }


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


def test_asset_owner_can_replace_policy_rules_through_api():
    client = _client()
    asset = _provision_owned_asset(client)

    response = client.put(
        f"/v1/assets/{asset}/policy-rules",
        json={"rules": [_allow_rule(row_filter="region = 'us'")]},
        headers=_bearer("owner-token"),
    )

    assert response.status_code == 200
    rules = client.get(f"/v1/assets/{asset}/policy-rules", headers=ADMIN_HEADERS).json()
    assert rules[0]["row_filter"] == "region = 'us'"


def test_group_owner_can_publish_policy_version_through_api():
    client = _client()
    asset = _provision_owned_asset(client)
    client.put(
        f"/v1/assets/{asset}/policy-rules",
        json={"rules": [_allow_rule(row_filter="region = 'eu'")]},
        headers=_bearer("owner-token"),
    )

    response = client.post(
        f"/v1/assets/{asset}/policy-versions",
        headers=_bearer("owner-token"),
    )

    assert response.status_code == 200
    assert UUID(response.json()["asset_id"]) == asset
    assert response.json()["policy_version"] > 0


def test_non_owner_cannot_change_policy_rules_or_publish_policy_version():
    client = _client()
    asset = _provision_owned_asset(client)

    replace = client.put(
        f"/v1/assets/{asset}/policy-rules",
        json={"rules": [_allow_rule(row_filter="region = 'us'")]},
        headers=_bearer("outsider-token"),
    )
    publish = client.post(
        f"/v1/assets/{asset}/policy-versions",
        headers=_bearer("outsider-token"),
    )

    assert replace.status_code == 403
    assert publish.status_code == 403


def test_platform_admin_can_assign_owner_and_bootstrap_policy():
    client = _client()
    asset = _provision_asset_without_owner(client)

    owners = client.put(
        f"/v1/assets/{asset}/owners",
        json={"owners": ["group:asset-owners"]},
        headers=_bearer("admin-oidc-token"),
    )
    policy = client.put(
        f"/v1/assets/{asset}/policy-rules",
        json={"rules": [_allow_rule(row_filter=None)]},
        headers=_bearer("admin-oidc-token"),
    )

    assert owners.status_code == 200
    assert owners.json()["owners"] == ["group:asset-owners"]
    assert policy.status_code == 200


def _provision_owned_asset(client: TestClient) -> UUID:
    asset = _provision_asset_without_owner(client)
    response = client.put(
        f"/v1/assets/{asset}/owners",
        json={"owners": ["group:asset-owners"]},
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
            "path_rules": [],
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
    client.put(
        f"/v1/assets/{asset['id']}/policy-rules",
        json={"rules": [_allow_rule(row_filter=None)]},
        headers=ADMIN_HEADERS,
    )
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
