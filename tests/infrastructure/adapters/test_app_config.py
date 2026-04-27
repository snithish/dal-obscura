from __future__ import annotations

import textwrap

import jwt
import pytest

from dal_obscura.application.ports.identity import AuthenticationRequest
from dal_obscura.infrastructure.adapters.app_config import load_app_config

JWT_SECRET = "test-jwt-secret-32-characters-long"


def _authorization_header(secret: str, subject: str = "user1") -> AuthenticationRequest:
    token = jwt.encode({"sub": subject}, secret, algorithm="HS256")
    return AuthenticationRequest(headers={"authorization": f"Bearer {token}"})


def test_load_app_config_resolves_relative_catalog_and_policy_paths(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
    (tmp_path / "catalogs.yaml").write_text("catalogs: {}\n")
    (tmp_path / "policies.yaml").write_text("version: 1\n")
    monkeypatch.setenv("DAL_OBSCURA_JWT_SECRET", JWT_SECRET)
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
    principal = config.auth.identity_provider.authenticate(_authorization_header(JWT_SECRET))
    assert principal.id == "user1"


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
    monkeypatch.setenv("PREFIXED_JWT", JWT_SECRET)

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
    principal = config.auth.identity_provider.authenticate(_authorization_header(JWT_SECRET))
    assert principal.id == "user1"


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


def test_load_app_config_loads_module_auth_provider_and_resolves_secret_args(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("AUTH_PROVIDER_SECRET", "resolved-auth-secret")
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
              module: tests.support.identity_provider_fakes.RecordingIdentityProvider
              args:
                issuer: https://issuer.example.test
                nested:
                  secret:
                    key: AUTH_PROVIDER_SECRET
                claim_paths:
                  - groups
                  - realm_access.roles
            """
        )
    )

    config = load_app_config(app_path)

    provider = config.auth.identity_provider
    assert provider.authenticate(AuthenticationRequest()).id == "provider-user"
    assert provider.kwargs == {
        "issuer": "https://issuer.example.test",
        "nested": {"secret": "resolved-auth-secret"},
        "claim_paths": ["groups", "realm_access.roles"],
    }


def test_load_app_config_loads_auth_provider_chain(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("AUTH_PROVIDER_SECRET", "resolved-auth-secret")
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
              providers:
                - module: tests.support.identity_provider_fakes.RecordingIdentityProvider
                  args:
                    nested:
                      secret:
                        key: AUTH_PROVIDER_SECRET
                - module: tests.support.identity_provider_fakes.RecordingIdentityProvider
                  args:
                    issuer: https://issuer.example.test
            """
        )
    )

    config = load_app_config(app_path)

    provider = config.auth.identity_provider
    principal = provider.authenticate(AuthenticationRequest())
    assert principal.id == "provider-user"
    assert len(provider.providers) == 2
    assert provider.providers[0].kwargs == {"nested": {"secret": "resolved-auth-secret"}}


def test_load_app_config_resolves_api_key_provider_secrets(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("SPARK_API_KEY", "api-secret-1")
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
              module: dal_obscura.infrastructure.adapters.identity_api_key.ApiKeyIdentityProvider
              args:
                keys:
                  - id: spark-prod
                    secret:
                      key: SPARK_API_KEY
                    groups: ["spark"]
                    attributes:
                      tenant: acme
            """
        )
    )

    config = load_app_config(app_path)
    principal = config.auth.identity_provider.authenticate(
        AuthenticationRequest(headers={"x-api-key": "api-secret-1"})
    )

    assert principal.id == "spark-prod"
    assert principal.groups == ["spark"]
    assert principal.attributes == {"tenant": "acme"}


def test_load_app_config_resolves_transport_tls_secrets(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
    server_cert = "-----BEGIN CERTIFICATE-----\\nserver\\n-----END CERTIFICATE-----"
    server_key = "-----BEGIN PRIVATE KEY-----\\nkey\\n-----END PRIVATE KEY-----"
    client_ca = "-----BEGIN CERTIFICATE-----\\nca\\n-----END CERTIFICATE-----"
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("DAL_OBSCURA_JWT_SECRET", JWT_SECRET)
    monkeypatch.setenv("SERVER_CERT", server_cert)
    monkeypatch.setenv("SERVER_KEY", server_key)
    monkeypatch.setenv("CLIENT_CA", client_ca)
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
            transport:
              tls:
                cert:
                  key: SERVER_CERT
                key:
                  key: SERVER_KEY
                client_ca:
                  key: CLIENT_CA
                verify_client: true
            """
        )
    )

    config = load_app_config(app_path)

    tls = config.transport.tls
    assert tls is not None
    assert tls.cert.startswith("-----BEGIN CERTIFICATE-----")
    assert tls.key.startswith("-----BEGIN PRIVATE KEY-----")
    assert tls.client_ca is not None
    assert tls.client_ca.startswith("-----BEGIN CERTIFICATE-----")
    assert tls.verify_client is True


def test_load_app_config_rejects_auth_provider_without_authenticate(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
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
              module: tests.support.identity_provider_fakes.MissingAuthenticateProvider
              args: {}
            """
        )
    )

    with pytest.raises(ValueError, match="authenticate"):
        load_app_config(app_path)


def test_load_app_config_wraps_auth_provider_constructor_errors(tmp_path, monkeypatch):
    app_path = tmp_path / "app.yaml"
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
              module: tests.support.identity_provider_fakes.FailingIdentityProvider
              args: {}
            """
        )
    )

    with pytest.raises(ValueError, match="FailingIdentityProvider"):
        load_app_config(app_path)
