from __future__ import annotations

from dal_obscura.control_plane.interfaces.cli import cors_origins_from_env, ui_auth_config_from_env


def test_ui_auth_config_from_env_uses_public_spa_values_without_secret() -> None:
    config = ui_auth_config_from_env(
        {
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER": (
                "http://keycloak:8080/realms/dal-obscura-demo"
            ),
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE": "dal-obscura",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "dal-obscura-ui",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI": (
                "http://127.0.0.1:8820/ui/auth/callback"
            ),
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI": (
                "http://127.0.0.1:8820/ui"
            ),
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_SECRET": "must-not-leak",
        }
    )

    assert config == {
        "authority": "http://keycloak:8080/realms/dal-obscura-demo",
        "client_id": "dal-obscura-ui",
        "redirect_uri": "http://127.0.0.1:8820/ui/auth/callback",
        "post_logout_redirect_uri": "http://127.0.0.1:8820/ui",
        "scope": "openid profile",
    }


def test_ui_auth_config_from_env_parses_login_shortcuts() -> None:
    config = ui_auth_config_from_env(
        {
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER": "http://keycloak/realms/demo",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "dal-obscura-ui",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI": "http://ui/callback",
            "DAL_OBSCURA_CONTROL_PLANE_UI_LOGIN_SHORTCUTS": (
                "Platform owner=demo-admin; Data asset owner=asset-owner; Broken="
            ),
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_TOKEN_URL": "http://keycloak/token",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_ID": "dal-obscura-cli",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_SECRET": "secret",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_PASSWORDS": (
                "demo-admin=admin-pass;asset-owner=owner-pass"
            ),
        }
    )

    assert config is not None
    assert config["login_shortcuts"] == [
        {"label": "Platform owner", "login_hint": "demo-admin"},
        {"label": "Data asset owner", "login_hint": "asset-owner"},
    ]
    assert config["demo_login"] == {
        "token_url": "http://keycloak/token",
        "client_id": "dal-obscura-cli",
        "client_secret": "secret",
        "passwords": {
            "demo-admin": "admin-pass",
            "asset-owner": "owner-pass",
        },
    }


def test_ui_auth_config_from_env_is_absent_without_public_client_id() -> None:
    assert (
        ui_auth_config_from_env(
            {"DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER": "http://keycloak/realms/demo"}
        )
        is None
    )


def test_cors_origins_from_env_parses_comma_separated_origins() -> None:
    assert cors_origins_from_env(
        {
            "DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS": (
                " http://127.0.0.1:8821,https://console.example.test, "
            )
        }
    ) == ("http://127.0.0.1:8821", "https://console.example.test")
