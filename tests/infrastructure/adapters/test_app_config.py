from __future__ import annotations

import textwrap

import pytest

from dal_obscura.infrastructure.adapters.app_config import load_app_config


def test_load_app_config_resolves_relative_catalog_and_policy_paths(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
    (tmp_path / "catalogs.yaml").write_text("catalogs: {}\n")
    (tmp_path / "policies.yaml").write_text("version: 1\n")
    monkeypatch.setenv("DAL_OBSCURA_JWT_SECRET", "jwt-secret")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")

    app_path.write_text(
        textwrap.dedent(
            """
            secret_provider:
              module: dal_obscura.infrastructure.adapters.secret_providers.EnvSecretProvider
              args: {}
            ticket:
              ttl_seconds: 900
              max_tickets: 64
              secret:
                key: DAL_OBSCURA_TICKET_SECRET
            auth:
              jwt_secret:
                key: DAL_OBSCURA_JWT_SECRET
            """
        )
    )

    config = load_app_config(app_path)

    assert config.catalog_file == (tmp_path / "catalogs.yaml").resolve()
    assert config.policy_file == (tmp_path / "policies.yaml").resolve()
    assert config.ticket.secret == "ticket-secret"
    assert config.auth.jwt_secret == "jwt-secret"


def test_load_app_config_rejects_missing_env_secret(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
    monkeypatch.delenv("MISSING_SECRET", raising=False)
    app_path.write_text(
        textwrap.dedent(
            """
            secret_provider:
              module: dal_obscura.infrastructure.adapters.secret_providers.EnvSecretProvider
              args: {}
            ticket:
              ttl_seconds: 900
              max_tickets: 64
              secret:
                key: MISSING_SECRET
            auth:
              jwt_secret:
                key: MISSING_SECRET
            """
        )
    )

    with pytest.raises(ValueError, match="MISSING_SECRET"):
        load_app_config(app_path)


def test_load_app_config_rejects_unknown_keys(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
    monkeypatch.setenv("DAL_OBSCURA_JWT_SECRET", "jwt-secret")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    app_path.write_text(
        textwrap.dedent(
            """
            secret_provider:
              module: dal_obscura.infrastructure.adapters.secret_providers.EnvSecretProvider
              args: {}
            ticket:
              ttl_seconds: 900
              max_tickets: 64
              secret:
                key: DAL_OBSCURA_TICKET_SECRET
            auth:
              jwt_secret:
                key: DAL_OBSCURA_JWT_SECRET
            unknown_key: true
            """
        )
    )

    with pytest.raises(ValueError, match="unknown_key"):
        load_app_config(app_path)


def test_load_app_config_rejects_non_positive_ticket_values(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
    monkeypatch.setenv("DAL_OBSCURA_JWT_SECRET", "jwt-secret")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    app_path.write_text(
        textwrap.dedent(
            """
            secret_provider:
              module: dal_obscura.infrastructure.adapters.secret_providers.EnvSecretProvider
              args: {}
            ticket:
              ttl_seconds: 0
              max_tickets: -1
              secret:
                key: DAL_OBSCURA_TICKET_SECRET
            auth:
              jwt_secret:
                key: DAL_OBSCURA_JWT_SECRET
            """
        )
    )

    with pytest.raises(ValueError, match="ticket"):
        load_app_config(app_path)


def test_load_app_config_constructs_provider_with_args(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
    monkeypatch.setenv("PREFIXED_TICKET", "ticket-secret")
    monkeypatch.setenv("PREFIXED_JWT", "jwt-secret")

    app_path.write_text(
        textwrap.dedent(
            """
            secret_provider:
              module: dal_obscura.infrastructure.adapters.secret_providers.EnvSecretProvider
              args:
                prefix: PREFIXED_
            ticket:
              ttl_seconds: 900
              max_tickets: 64
              secret:
                key: TICKET
            auth:
              jwt_secret:
                key: JWT
            """
        )
    )

    config = load_app_config(app_path)
    assert config.ticket.secret == "ticket-secret"
    assert config.auth.jwt_secret == "jwt-secret"


def test_load_app_config_rejects_provider_without_interface(tmp_path):
    app_path = tmp_path / "app.yaml"
    app_path.write_text(
        textwrap.dedent(
            """
            secret_provider:
              module: builtins.dict
              args: {}
            ticket:
              ttl_seconds: 900
              max_tickets: 64
              secret:
                key: TICKET
            auth:
              jwt_secret:
                key: JWT
            """
        )
    )

    with pytest.raises(ValueError, match="must inherit from SecretProvider"):
        load_app_config(app_path)
