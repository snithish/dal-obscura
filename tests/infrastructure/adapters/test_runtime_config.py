from __future__ import annotations

import pytest

from dal_obscura.data_plane.infrastructure.adapters.runtime_config import (
    load_data_plane_runtime_config,
)


def test_runtime_config_reads_required_database_and_cell(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_CELL_ID", "00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("DAL_OBSCURA_LOCATION", "grpc://127.0.0.1:8815")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")

    config = load_data_plane_runtime_config()

    assert config.database_url == "sqlite+pysqlite:///:memory:"
    assert str(config.cell_id) == "00000000-0000-0000-0000-000000000001"
    assert config.location == "grpc://127.0.0.1:8815"
    assert config.ticket_secret == "ticket-secret"


def test_runtime_config_reads_tls_environment(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_CELL_ID", "00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("DAL_OBSCURA_TLS_CERT", "server-cert")
    monkeypatch.setenv("DAL_OBSCURA_TLS_KEY", "server-key")
    monkeypatch.setenv("DAL_OBSCURA_TLS_CLIENT_CA", "client-ca")
    monkeypatch.setenv("DAL_OBSCURA_TLS_VERIFY_CLIENT", "true")

    config = load_data_plane_runtime_config()

    assert config.tls_cert == "server-cert"
    assert config.tls_key == "server-key"
    assert config.tls_client_ca == "client-ca"
    assert config.tls_verify_client is True


def test_runtime_config_does_not_expose_stale_policy_ticket_compatibility(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_CELL_ID", "00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("DAL_OBSCURA_ALLOW_STALE_POLICY_TICKETS", "true")

    config = load_data_plane_runtime_config()

    assert not hasattr(config, "allow_stale_policy_tickets")


def test_runtime_config_reads_module_based_secret_provider(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_CELL_ID", "00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv(
        "DAL_OBSCURA_SECRET_PROVIDER_MODULE",
        "tests.support.secret_provider_fakes.FakeSecretProvider",
    )
    monkeypatch.setenv("DAL_OBSCURA_SECRET_PROVIDER_CONFIG", '{"prefix":"local"}')
    monkeypatch.setenv(
        "DAL_OBSCURA_SECRET_PROVIDER_SECRETS",
        '{"token":{"env":"DAL_OBSCURA_PROVIDER_TOKEN"}}',
    )

    config = load_data_plane_runtime_config()

    assert config.secret_provider.module == "tests.support.secret_provider_fakes.FakeSecretProvider"
    assert config.secret_provider.config == {"prefix": "local"}
    assert config.secret_provider.secrets == {"token": {"env": "DAL_OBSCURA_PROVIDER_TOKEN"}}


def test_runtime_config_rejects_missing_database_url(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("DAL_OBSCURA_DATABASE_URL", raising=False)
    monkeypatch.setenv("DAL_OBSCURA_CELL_ID", "00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")

    with pytest.raises(ValueError, match="DAL_OBSCURA_DATABASE_URL"):
        load_data_plane_runtime_config()
