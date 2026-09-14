from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest
from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError

from dal_obscura.common.config_store.db import create_engine_from_url, migrate_config_store
from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.control_plane.interfaces import control_plane_cli


def test_control_plane_cli_starts_configured_app(monkeypatch, tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'control-plane.db'}"
    migrate_config_store(create_engine_from_url(database_url))
    calls: dict[str, object] = {}
    environment = {
        "DAL_OBSCURA_DATABASE_URL": database_url,
        "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "test-admin",
        "DAL_OBSCURA_CONTROL_PLANE_HOST": "127.0.0.1",
        "DAL_OBSCURA_CONTROL_PLANE_PORT": "8820",
    }

    monkeypatch.setattr(
        control_plane_cli.uvicorn,
        "run",
        lambda app, **kwargs: calls.update({"app": app, **kwargs}),
    )

    assert control_plane_cli.run(environment) == 0
    assert calls["host"] == "127.0.0.1"
    assert calls["port"] == 8820
    assert cast(FastAPI, calls["app"]).title == "dal-obscura control-plane API"


def test_control_plane_cli_passes_login_rate_limits(monkeypatch, tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'control-plane.db'}"
    migrate_config_store(create_engine_from_url(database_url))
    captured: dict[str, object] = {}
    environment = {
        "DAL_OBSCURA_DATABASE_URL": database_url,
        "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "test-admin",
        "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_ATTEMPTS": "5",
        "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_WINDOW_SECONDS": "120",
        "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_BLOCK_SECONDS": "42",
    }

    monkeypatch.setattr(
        control_plane_cli,
        "create_app",
        lambda *args, **kwargs: captured.update(kwargs) or FastAPI(),
    )
    monkeypatch.setattr(control_plane_cli.uvicorn, "run", lambda app, **kwargs: None)

    assert control_plane_cli.run(environment) == 0
    assert captured["login_rate_limit_attempts"] == 5
    assert captured["login_rate_limit_window_seconds"] == 120
    assert captured["login_rate_limit_block_seconds"] == 42
    assert captured["cors_origins"] == ("http://127.0.0.1:5173", "http://localhost:5173")
    registry = cast(PluginRegistry, captured["plugin_registry"])
    assert ("catalog", "iceberg.sql") in registry.admitted()
    assert ("table_format", "iceberg") in registry.admitted()


def test_ui_auth_config_excludes_removed_demo_login_settings() -> None:
    config = control_plane_cli._ui_auth_config(
        {
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER": "https://issuer.example",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "governance-ui",
            "DAL_OBSCURA_CONTROL_PLANE_UI_LOGIN_SHORTCUTS": "Owner=owner",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_TOKEN_URL": "https://issuer.example/token",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_ID": "legacy-client",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_SECRET": "legacy-secret",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_PASSWORDS": "owner=legacy-password",
        }
    )

    assert config == {
        "authority": "https://issuer.example",
        "client_id": "governance-ui",
        "scope": "openid profile",
    }


def test_control_plane_cli_passes_configured_secret_provider(monkeypatch, tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'control-plane.db'}"
    migrate_config_store(create_engine_from_url(database_url))
    captured: dict[str, object] = {}
    monkeypatch.setenv("LOCAL_catalog-password", "value")
    environment = {
        "DAL_OBSCURA_DATABASE_URL": database_url,
        "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "test-admin",
        "DAL_OBSCURA_SECRET_PROVIDER_CONFIG": '{"prefix":"LOCAL_"}',
    }

    monkeypatch.setattr(
        control_plane_cli,
        "create_app",
        lambda *args, **kwargs: captured.update(kwargs) or FastAPI(),
    )
    monkeypatch.setattr(control_plane_cli.uvicorn, "run", lambda app, **kwargs: None)

    assert control_plane_cli.run(environment) == 0
    provider = captured["secret_provider"]
    assert provider.get_secret("catalog-password") == "value"


def test_control_plane_cli_passes_dedicated_review_secret(monkeypatch, tmp_path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'control-plane.db'}"
    migrate_config_store(create_engine_from_url(database_url))
    captured: dict[str, object] = {}
    environment = {
        "DAL_OBSCURA_DATABASE_URL": database_url,
        "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "test-admin",
        "DAL_OBSCURA_CONTROL_PLANE_REVIEW_SECRET": "review-secret",
    }

    monkeypatch.setattr(
        control_plane_cli,
        "create_app",
        lambda *args, **kwargs: captured.update(kwargs) or FastAPI(),
    )
    monkeypatch.setattr(control_plane_cli.uvicorn, "run", lambda app, **kwargs: None)

    assert control_plane_cli.run(environment) == 0
    assert captured["review_secret"] == "review-secret"


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        ({}, "DAL_OBSCURA_DATABASE_URL is required"),
        (
            {"DAL_OBSCURA_DATABASE_URL": "sqlite+pysqlite:///:memory:"},
            "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN is required",
        ),
    ],
)
def test_control_plane_cli_rejects_missing_required_configuration(
    environment: Mapping[str, str],
    message: str,
    capsys,
) -> None:
    assert control_plane_cli.run(environment) == 2
    assert message in capsys.readouterr().err


def test_control_plane_cli_requires_current_schema(tmp_path, capsys) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'control-plane.db'}"

    assert (
        control_plane_cli.run(
            {
                "DAL_OBSCURA_DATABASE_URL": database_url,
                "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "test-admin",
            }
        )
        == 1
    )
    assert "Run `dal-obscura-migrate upgrade`" in capsys.readouterr().err


def test_control_plane_cli_redacts_database_startup_errors(monkeypatch, tmp_path, capsys) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'control-plane.db'}"
    monkeypatch.setattr(
        control_plane_cli,
        "check_config_store_schema",
        lambda _engine: (_ for _ in ()).throw(
            SQLAlchemyError("postgresql://user:secret@db.internal/control_plane")
        ),
    )

    result = control_plane_cli.run(
        {
            "DAL_OBSCURA_DATABASE_URL": database_url,
            "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "test-admin",
        }
    )

    assert result == 1
    error = capsys.readouterr().err
    assert error.strip() == "Control-plane database unavailable"
    assert "secret" not in error


def test_control_plane_cli_exposes_help_without_runtime_configuration(capsys) -> None:
    with pytest.raises(SystemExit) as exit_info:
        control_plane_cli.run({}, argv=("--help",))

    assert exit_info.value.code == 0
    assert "Start the dal-obscura control-plane HTTP server" in capsys.readouterr().out


def test_control_plane_cli_rejects_insecure_production_profile(capsys):
    result = control_plane_cli.run(
        {
            "DAL_OBSCURA_CONTROL_PLANE_PROFILE": "production",
            "DAL_OBSCURA_DATABASE_URL": "sqlite+pysqlite:///:memory:",
            "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "short-token",
        }
    )

    assert result == 1
    assert "at least 32 characters" in capsys.readouterr().err


def test_control_plane_cli_requires_real_tls_oidc_in_production(capsys):
    database_url = "postgresql+psycopg://user:pass@db.example/control_plane"
    environment = {
        "DAL_OBSCURA_CONTROL_PLANE_PROFILE": "production",
        "DAL_OBSCURA_DATABASE_URL": database_url,
        "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "x" * 40,
        "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER": "http://issuer.example",
        "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE": "dal-obscura-admin",
    }

    result = control_plane_cli.run(environment)

    assert result == 1
    assert "HTTPS bearer OIDC issuer" in capsys.readouterr().err


def test_control_plane_cli_rejects_sqlite_in_production(capsys):
    result = control_plane_cli.run(
        {
            "DAL_OBSCURA_CONTROL_PLANE_PROFILE": "production",
            "DAL_OBSCURA_DATABASE_URL": "sqlite+pysqlite:///:memory:",
            "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "x" * 40,
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER": "https://issuer.example",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE": "dal-obscura-admin",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP": "platform-admins",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER": "https://issuer.example",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "dal-obscura-ui",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI": "https://console.example/auth/callback",
            "DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS": "https://console.example",
        }
    )

    assert result == 1
    assert "PostgreSQL" in capsys.readouterr().err


def test_control_plane_cli_rejects_enabled_static_bootstrap_in_production(capsys):
    result = control_plane_cli.run(
        {
            "DAL_OBSCURA_CONTROL_PLANE_PROFILE": "production",
            "DAL_OBSCURA_DATABASE_URL": "postgresql+psycopg://user:pass@db.example/control_plane",
            "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "x" * 40,
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER": "https://issuer.example",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE": "dal-obscura-admin",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP": "platform-admins",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER": "https://issuer.example",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "dal-obscura-ui",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI": "https://console.example/auth/callback",
            "DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS": "https://console.example",
            "DAL_OBSCURA_CONTROL_PLANE_CATALOG_EGRESS_ALLOWLIST": "catalog.example",
            "DAL_OBSCURA_CONTROL_PLANE_BOOTSTRAP_ENABLED": "true",
        }
    )

    assert result == 1
    assert "bootstrap admin access" in capsys.readouterr().err


def test_control_plane_cli_requires_separate_review_secret_in_production(capsys):
    result = control_plane_cli.run(
        {
            "DAL_OBSCURA_CONTROL_PLANE_PROFILE": "production",
            "DAL_OBSCURA_DATABASE_URL": "postgresql+psycopg://user:pass@db.example/control_plane",
            "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "x" * 40,
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER": "https://issuer.example",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE": "dal-obscura-admin",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP": "platform-admins",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER": "https://issuer.example",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "dal-obscura-ui",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI": "https://console.example/auth/callback",
            "DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS": "https://console.example",
            "DAL_OBSCURA_CONTROL_PLANE_CATALOG_EGRESS_ALLOWLIST": "catalog.example",
        }
    )

    assert result == 1
    assert "REVIEW_SECRET" in capsys.readouterr().err


def test_control_plane_cli_requires_production_admin_group(capsys):
    result = control_plane_cli.run(
        {
            "DAL_OBSCURA_CONTROL_PLANE_PROFILE": "production",
            "DAL_OBSCURA_DATABASE_URL": "postgresql+psycopg://user:pass@db.example/control_plane",
            "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "x" * 40,
            "DAL_OBSCURA_CONTROL_PLANE_REVIEW_SECRET": "r" * 40,
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER": "https://issuer.example",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE": "dal-obscura-admin",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER": "https://issuer.example",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "dal-obscura-ui",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI": "https://console.example/auth/callback",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI": "https://console.example/",
            "DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS": "https://console.example",
            "DAL_OBSCURA_CONTROL_PLANE_CATALOG_EGRESS_ALLOWLIST": "catalog.example",
        }
    )

    assert result == 1
    assert "admin group" in capsys.readouterr().err


def test_control_plane_cli_requires_oidc_redirect_origins_in_cors(capsys):
    result = control_plane_cli.run(
        {
            "DAL_OBSCURA_CONTROL_PLANE_PROFILE": "production",
            "DAL_OBSCURA_DATABASE_URL": "postgresql+psycopg://user:pass@db.example/control_plane",
            "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "x" * 40,
            "DAL_OBSCURA_CONTROL_PLANE_REVIEW_SECRET": "r" * 40,
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER": "https://issuer.example",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE": "dal-obscura-admin",
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP": "platform-admins",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER": "https://issuer.example",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "dal-obscura-ui",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI": "https://other.example/auth/callback",
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI": "https://console.example/",
            "DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS": "https://console.example",
            "DAL_OBSCURA_CONTROL_PLANE_CATALOG_EGRESS_ALLOWLIST": "catalog.example",
        }
    )

    assert result == 1
    assert "redirect origins" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        (
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_JWKS_URL",
            "http://issuer.example/jwks",
            "HTTPS OIDC JWKS URL",
        ),
        (
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_AUTHORIZATION_ENDPOINT",
            "http://issuer.example/auth",
            "HTTPS browser authorization endpoint",
        ),
        (
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_TOKEN_ENDPOINT",
            "http://issuer.example/token",
            "HTTPS browser token endpoint",
        ),
        (
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI",
            "http://console.example",
            "HTTPS browser post-logout redirect URI",
        ),
    ],
)
def test_control_plane_cli_rejects_insecure_explicit_oidc_endpoints(name, value, message, capsys):
    database_url = "postgresql+psycopg://user:pass@db.example/control_plane"
    environment = {
        "DAL_OBSCURA_CONTROL_PLANE_PROFILE": "production",
        "DAL_OBSCURA_DATABASE_URL": database_url,
        "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN": "x" * 40,
        "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER": "https://issuer.example",
        "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE": "dal-obscura-admin",
        "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP": "platform-admins",
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER": "https://issuer.example",
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID": "dal-obscura-ui",
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI": "https://console.example/auth/callback",
        "DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS": "https://console.example",
        "DAL_OBSCURA_CONTROL_PLANE_CATALOG_EGRESS_ALLOWLIST": "catalog.example",
        name: value,
    }

    result = control_plane_cli.run(environment)

    assert result == 1
    assert message in capsys.readouterr().err
