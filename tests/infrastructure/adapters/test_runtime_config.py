from __future__ import annotations

import pytest

from dal_obscura.data_plane.infrastructure.adapters.runtime_config import (
    load_data_plane_runtime_config,
)
from dal_obscura.data_plane.interfaces.cli.main import _tls_material


def test_runtime_config_defaults(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_LOCATION", "grpc://127.0.0.1:8815")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")

    config = load_data_plane_runtime_config()

    assert config.database_url == "sqlite+pysqlite:///:memory:"
    assert config.location == "grpc://127.0.0.1:8815"
    assert config.ticket_secret == "ticket-secret"
    assert config.ticket_previous_secrets == ()
    assert config.max_active_streams == 16
    assert config.duckdb_memory_limit == "512MB"
    assert config.max_input_batch_bytes == 64 * 1024 * 1024
    assert config.max_output_batch_bytes == 64 * 1024 * 1024
    assert config.max_ticket_payload_bytes == 16 * 1024 * 1024
    assert config.max_stream_seconds == 300
    assert config.ticket_cleanup_interval_seconds == 60


def test_runtime_config_rejects_relative_memory_limits(monkeypatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "test-secret")
    monkeypatch.setenv("DAL_OBSCURA_DUCKDB_MEMORY_LIMIT", "80%")
    with pytest.raises(ValueError, match="DAL_OBSCURA_DUCKDB_MEMORY_LIMIT"):
        load_data_plane_runtime_config()


def test_runtime_config_maps_custom_resource_tls_health_and_rotation_settings(monkeypatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_PREVIOUS_SECRETS", "old-one, old-two")
    monkeypatch.setenv("DAL_OBSCURA_MAX_ACTIVE_STREAMS", "3")
    monkeypatch.setenv("DAL_OBSCURA_DUCKDB_MEMORY_LIMIT", "256MB")
    monkeypatch.setenv("DAL_OBSCURA_MAX_INPUT_BATCH_BYTES", "1048576")
    monkeypatch.setenv("DAL_OBSCURA_MAX_OUTPUT_BATCH_BYTES", "2097152")
    monkeypatch.setenv("DAL_OBSCURA_MAX_TICKET_PAYLOAD_BYTES", "524288")
    monkeypatch.setenv("DAL_OBSCURA_MAX_STREAM_SECONDS", "45")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_CLEANUP_INTERVAL_SECONDS", "15")
    monkeypatch.setenv("DAL_OBSCURA_DATA_PLANE_HEALTH_HOST", "0.0.0.0")
    monkeypatch.setenv("DAL_OBSCURA_DATA_PLANE_HEALTH_PORT", "8825")
    monkeypatch.setenv("DAL_OBSCURA_TLS_CERT", "server-cert")
    monkeypatch.setenv("DAL_OBSCURA_TLS_KEY", "server-key")
    monkeypatch.setenv("DAL_OBSCURA_TLS_CLIENT_CA", "client-ca")
    monkeypatch.setenv("DAL_OBSCURA_TLS_VERIFY_CLIENT", "true")
    monkeypatch.setenv("DAL_OBSCURA_MAX_CACHED_CATALOG_PROVIDERS", "7")
    monkeypatch.setenv("DAL_OBSCURA_CATALOG_PROVIDER_WAIT_SECONDS", "2")

    config = load_data_plane_runtime_config()

    assert config.ticket_previous_secrets == ("old-one", "old-two")
    assert config.max_active_streams == 3
    assert config.duckdb_memory_limit == "256MB"
    assert config.max_input_batch_bytes == 1048576
    assert config.max_output_batch_bytes == 2097152
    assert config.max_ticket_payload_bytes == 524288
    assert config.max_stream_seconds == 45
    assert config.ticket_cleanup_interval_seconds == 15
    assert config.health_host == "0.0.0.0"
    assert config.health_port == 8825
    assert config.tls_cert == "server-cert"
    assert config.tls_key == "server-key"
    assert config.tls_client_ca == "client-ca"
    assert config.tls_verify_client is True
    assert config.max_cached_catalog_providers == 7
    assert config.catalog_provider_wait_seconds == 2


def test_runtime_config_rejects_duplicate_previous_ticket_secrets(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_PREVIOUS_SECRETS", "old,old")

    with pytest.raises(ValueError, match="unique"):
        load_data_plane_runtime_config()


def test_runtime_config_rejects_duplicate_secret_provider_config_keys(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("DAL_OBSCURA_SECRET_PROVIDER_CONFIG", '{"prefix":"one","prefix":"two"}')

    with pytest.raises(ValueError, match="duplicate JSON key"):
        load_data_plane_runtime_config()


def test_runtime_config_rejects_stale_config_window(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_ALLOW_STALE_CONFIG_SECONDS", "60")

    with pytest.raises(ValueError, match="ALLOW_STALE_CONFIG_SECONDS is unsupported"):
        load_data_plane_runtime_config()


def test_runtime_config_rejects_module_based_secret_provider(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv(
        "DAL_OBSCURA_SECRET_PROVIDER_MODULE",
        "tests.support.secret_provider_fakes.FakeSecretProvider",
    )
    monkeypatch.setenv("DAL_OBSCURA_SECRET_PROVIDER_CONFIG", '{"prefix":"local"}')

    with pytest.raises(ValueError, match="SECRET_PROVIDER_MODULE is unsupported"):
        load_data_plane_runtime_config()


def test_runtime_config_rejects_retired_secret_payload(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")
    monkeypatch.setenv("DAL_OBSCURA_SECRET_PROVIDER_SECRETS", "{}")

    with pytest.raises(ValueError, match="SECRET_PROVIDER_SECRETS is unsupported"):
        load_data_plane_runtime_config()


def test_runtime_config_rejects_missing_database_url(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("DAL_OBSCURA_DATABASE_URL", raising=False)
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "ticket-secret")

    with pytest.raises(ValueError, match="DAL_OBSCURA_DATABASE_URL"):
        load_data_plane_runtime_config()


def test_runtime_config_requires_secure_production_data_plane(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "postgresql+psycopg://db/app")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "t" * 32)
    monkeypatch.setenv("DAL_OBSCURA_DATA_PLANE_PROFILE", "production")
    monkeypatch.setenv("DAL_OBSCURA_LOCATION", "grpc://flight:8815")

    with pytest.raises(ValueError, match="grpc\\+tls"):
        load_data_plane_runtime_config()


def test_runtime_config_accepts_secure_production_data_plane(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "postgresql+psycopg://db/app")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "t" * 32)
    monkeypatch.setenv("DAL_OBSCURA_DATA_PLANE_PROFILE", "production")
    monkeypatch.setenv("DAL_OBSCURA_LOCATION", "grpc+tls://flight:8815")
    monkeypatch.setenv("DAL_OBSCURA_TLS_CERT", "server-cert")
    monkeypatch.setenv("DAL_OBSCURA_TLS_KEY", "server-key")
    monkeypatch.setenv(
        "DAL_OBSCURA_SECRET_PROVIDER_CONFIG", '{"scope_grants":{"identity":["JWT_SECRET"]}}'
    )

    config = load_data_plane_runtime_config()

    assert config.profile == "production"


def test_tls_material_reads_bounded_file_contents(tmp_path):
    certificate = tmp_path / "server.crt"
    certificate.write_bytes(b"PEM-CERTIFICATE")

    assert _tls_material(str(certificate), "certificate") == b"PEM-CERTIFICATE"


@pytest.mark.parametrize(
    "name",
    ["DAL_OBSCURA_MAX_CACHED_CATALOG_PROVIDERS", "DAL_OBSCURA_CATALOG_PROVIDER_WAIT_SECONDS"],
)
@pytest.mark.parametrize("value", ["0", "-1"])
def test_runtime_config_rejects_invalid_provider_cache_limits(monkeypatch, name, value):
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "test-secret")
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        load_data_plane_runtime_config()
