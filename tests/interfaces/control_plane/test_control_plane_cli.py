from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest
from fastapi import FastAPI

from dal_obscura.common.config_store.db import create_engine_from_url, migrate_config_store
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
