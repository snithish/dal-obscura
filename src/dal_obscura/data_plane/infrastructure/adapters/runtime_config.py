"""Environment-backed data-plane runtime configuration.

Example:
    ```python
    config = load_data_plane_runtime_config()
    assert config.location.startswith("grpc")
    ```
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from urllib.parse import urlsplit
from uuid import UUID

from dal_obscura.data_plane.infrastructure.adapters.memory_limits import validate_memory_limit
from dal_obscura.data_plane.infrastructure.adapters.secret_providers import (
    ENV_SECRET_PROVIDER_MODULE,
    SecretProviderConfig,
)


@dataclass(frozen=True)
class DataPlaneRuntimeConfig:
    """Configuration required to start one data-plane process.

    Example:
        ```python
        config = DataPlaneRuntimeConfig(
            database_url="sqlite+pysqlite:///control-plane.db",
            cell_id=cell_id,
            location="grpc://0.0.0.0:8815",
            ticket_secret="dev-ticket-secret",
        )
        ```
    """

    database_url: str
    cell_id: UUID
    location: str
    ticket_secret: str
    ticket_previous_secrets: tuple[str, ...] = ()
    log_level: str = "INFO"
    json_logs: bool = False
    tls_cert: str | None = None
    tls_key: str | None = None
    tls_client_ca: str | None = None
    tls_verify_client: bool = False
    health_host: str = "127.0.0.1"
    health_port: int | None = None
    max_active_streams: int = 16
    duckdb_memory_limit: str = "512MB"
    max_input_batch_bytes: int = 64 * 1024 * 1024
    max_output_batch_bytes: int = 64 * 1024 * 1024
    max_ticket_payload_bytes: int = 16 * 1024 * 1024
    max_stream_seconds: int = 300
    secret_provider: SecretProviderConfig = field(default_factory=SecretProviderConfig)
    profile: str = "local"
    ticket_cleanup_interval_seconds: int = 60
    plugin_lock_file: str | None = None


def load_data_plane_runtime_config() -> DataPlaneRuntimeConfig:
    """Loads data-plane configuration from environment variables."""
    if os.getenv("DAL_OBSCURA_ALLOW_STALE_CONFIG_SECONDS"):
        raise ValueError("DAL_OBSCURA_ALLOW_STALE_CONFIG_SECONDS is unsupported")
    database_url = _required_env("DAL_OBSCURA_DATABASE_URL")
    cell_id = UUID(_required_env("DAL_OBSCURA_CELL_ID"))
    location = os.getenv("DAL_OBSCURA_LOCATION", "grpc://0.0.0.0:8815").strip()
    ticket_secret = _required_env("DAL_OBSCURA_TICKET_SECRET")
    ticket_previous_secrets = _secret_list_env("DAL_OBSCURA_TICKET_PREVIOUS_SECRETS")
    profile = os.getenv("DAL_OBSCURA_DATA_PLANE_PROFILE", "local").strip().lower()
    log_level = os.getenv("DAL_OBSCURA_LOG_LEVEL", "INFO").strip() or "INFO"
    json_logs = _bool_env(os.getenv("DAL_OBSCURA_JSON_LOGS"))
    tls_verify_client = _bool_env(os.getenv("DAL_OBSCURA_TLS_VERIFY_CLIENT"))
    config = DataPlaneRuntimeConfig(
        database_url=database_url,
        cell_id=cell_id,
        location=location,
        ticket_secret=ticket_secret,
        ticket_previous_secrets=ticket_previous_secrets,
        profile=profile,
        log_level=log_level,
        json_logs=json_logs,
        tls_cert=_optional_env("DAL_OBSCURA_TLS_CERT"),
        tls_key=_optional_env("DAL_OBSCURA_TLS_KEY"),
        tls_client_ca=_optional_env("DAL_OBSCURA_TLS_CLIENT_CA"),
        tls_verify_client=tls_verify_client,
        health_host=os.getenv("DAL_OBSCURA_DATA_PLANE_HEALTH_HOST", "127.0.0.1").strip()
        or "127.0.0.1",
        health_port=_optional_int_env("DAL_OBSCURA_DATA_PLANE_HEALTH_PORT"),
        max_active_streams=_positive_int_env("DAL_OBSCURA_MAX_ACTIVE_STREAMS", default=16),
        duckdb_memory_limit=_memory_limit_env(),
        max_input_batch_bytes=_positive_int_env(
            "DAL_OBSCURA_MAX_INPUT_BATCH_BYTES", default=64 * 1024 * 1024
        ),
        max_output_batch_bytes=_positive_int_env(
            "DAL_OBSCURA_MAX_OUTPUT_BATCH_BYTES", default=64 * 1024 * 1024
        ),
        max_ticket_payload_bytes=_positive_int_env(
            "DAL_OBSCURA_MAX_TICKET_PAYLOAD_BYTES", default=16 * 1024 * 1024
        ),
        max_stream_seconds=_positive_int_env("DAL_OBSCURA_MAX_STREAM_SECONDS", default=300),
        ticket_cleanup_interval_seconds=_positive_int_env(
            "DAL_OBSCURA_TICKET_CLEANUP_INTERVAL_SECONDS",
            default=60,
        ),
        secret_provider=_secret_provider_config(),
        plugin_lock_file=_optional_env("DAL_OBSCURA_PLUGIN_LOCK_FILE"),
    )
    _validate_profile(config)
    return config


def _validate_profile(config: DataPlaneRuntimeConfig) -> None:
    if config.profile not in {"local", "production"}:
        raise ValueError("DAL_OBSCURA_DATA_PLANE_PROFILE must be local or production")
    if config.profile != "production":
        return
    if not config.database_url.lower().startswith("postgresql"):
        raise ValueError("Production data plane requires a PostgreSQL control-plane database")
    if len(config.ticket_secret) < 32:
        raise ValueError(
            "DAL_OBSCURA_TICKET_SECRET must contain at least 32 characters in production"
        )
    if any(len(secret) < 32 for secret in config.ticket_previous_secrets):
        raise ValueError(
            "DAL_OBSCURA_TICKET_PREVIOUS_SECRETS entries must contain at least "
            "32 characters in production"
        )
    if urlsplit(config.location).scheme != "grpc+tls":
        raise ValueError("Production data plane requires a grpc+tls location")
    if not config.tls_cert or not config.tls_key:
        raise ValueError(
            "Production data plane requires DAL_OBSCURA_TLS_CERT and DAL_OBSCURA_TLS_KEY"
        )
    if config.tls_verify_client and not config.tls_client_ca:
        raise ValueError(
            "DAL_OBSCURA_TLS_CLIENT_CA is required when client verification is enabled"
        )
    scope_grants = config.secret_provider.config.get("scope_grants")
    if not isinstance(scope_grants, dict) or not scope_grants:
        raise ValueError(
            "Production requires non-empty DAL_OBSCURA_SECRET_PROVIDER_CONFIG scope_grants"
        )


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable {name}")
    return value.strip()


def _optional_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return value


def _secret_list_env(name: str) -> tuple[str, ...]:
    value = os.getenv(name, "")
    if not value.strip():
        return ()
    entries = tuple(item.strip() for item in value.split(",") if item.strip())
    if len(set(entries)) != len(entries):
        raise ValueError(f"{name} entries must be unique")
    return entries


def _bool_env(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than 0")
    return parsed


def _positive_int_env(name: str, *, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than 0")
    return parsed


def _memory_limit_env() -> str:
    value = os.getenv("DAL_OBSCURA_DUCKDB_MEMORY_LIMIT", "512MB").strip()
    try:
        return validate_memory_limit(value)
    except ValueError as exc:
        raise ValueError(f"DAL_OBSCURA_DUCKDB_MEMORY_LIMIT: {exc}") from exc


def _secret_provider_config() -> SecretProviderConfig:
    if "DAL_OBSCURA_SECRET_PROVIDER_SECRETS" in os.environ:
        raise ValueError("DAL_OBSCURA_SECRET_PROVIDER_SECRETS is unsupported")
    module = os.getenv("DAL_OBSCURA_SECRET_PROVIDER_MODULE", ENV_SECRET_PROVIDER_MODULE).strip()
    if module != ENV_SECRET_PROVIDER_MODULE:
        raise ValueError("DAL_OBSCURA_SECRET_PROVIDER_MODULE is unsupported")
    return SecretProviderConfig(
        module=module,
        config=_json_object_env("DAL_OBSCURA_SECRET_PROVIDER_CONFIG"),
    )


def _json_object_env(name: str) -> dict[str, object]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return {}
    value = json.loads(raw, object_pairs_hook=_unique_json_object)
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return {str(key): item for key, item in value.items()}


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate environment configuration keys instead of overriding."""

    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"{key} contains duplicate JSON key {key!r}")
        result[key] = value
    return result
