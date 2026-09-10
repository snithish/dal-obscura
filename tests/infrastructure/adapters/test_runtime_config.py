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
    assert config.max_active_streams == 16
    assert config.duckdb_memory_limit == "512MB"
    assert config.max_input_batch_bytes == 64 * 1024 * 1024
    assert config.max_ticket_payload_bytes == 16 * 1024 * 1024


def test_runtime_config_reads_stream_resource_limits(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_CELL_ID", "00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("DAL_OBSCURA_MAX_ACTIVE_STREAMS", "3")
    monkeypatch.setenv("DAL_OBSCURA_DUCKDB_MEMORY_LIMIT", "256MB")
    monkeypatch.setenv("DAL_OBSCURA_MAX_INPUT_BATCH_BYTES", "1048576")
    monkeypatch.setenv("DAL_OBSCURA_MAX_TICKET_PAYLOAD_BYTES", "524288")

    config = load_data_plane_runtime_config()

    assert config.max_active_streams == 3
    assert config.duckdb_memory_limit == "256MB"
    assert config.max_input_batch_bytes == 1048576
    assert config.max_ticket_payload_bytes == 524288


def test_runtime_config_rejects_stale_config_window(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_ALLOW_STALE_CONFIG_SECONDS", "60")

    with pytest.raises(ValueError, match="ALLOW_STALE_CONFIG_SECONDS is unsupported"):
        load_data_plane_runtime_config()


def test_runtime_config_reads_data_plane_health_port(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_CELL_ID", "00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("DAL_OBSCURA_DATA_PLANE_HEALTH_HOST", "0.0.0.0")
    monkeypatch.setenv("DAL_OBSCURA_DATA_PLANE_HEALTH_PORT", "8825")

    config = load_data_plane_runtime_config()

    assert config.health_host == "0.0.0.0"
    assert config.health_port == 8825


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


def test_runtime_config_rejects_module_based_secret_provider(monkeypatch: pytest.MonkeyPatch):
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

    with pytest.raises(ValueError, match="SECRET_PROVIDER_MODULE is unsupported"):
        load_data_plane_runtime_config()


def test_runtime_config_rejects_missing_database_url(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("DAL_OBSCURA_DATABASE_URL", raising=False)
    monkeypatch.setenv("DAL_OBSCURA_CELL_ID", "00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")

    with pytest.raises(ValueError, match="DAL_OBSCURA_DATABASE_URL"):
        load_data_plane_runtime_config()
