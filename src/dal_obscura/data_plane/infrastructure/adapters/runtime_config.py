from __future__ import annotations

import os
from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class DataPlaneRuntimeConfig:
    database_url: str
    cell_id: UUID
    location: str
    ticket_secret: str
    log_level: str = "INFO"
    json_logs: bool = False
    tls_cert: str | None = None
    tls_key: str | None = None
    tls_client_ca: str | None = None
    tls_verify_client: bool = False


def load_data_plane_runtime_config() -> DataPlaneRuntimeConfig:
    database_url = _required_env("DAL_OBSCURA_DATABASE_URL")
    cell_id = UUID(_required_env("DAL_OBSCURA_CELL_ID"))
    location = os.getenv("DAL_OBSCURA_LOCATION", "grpc://0.0.0.0:8815").strip()
    ticket_secret = _required_env("DAL_OBSCURA_TICKET_SECRET")
    log_level = os.getenv("DAL_OBSCURA_LOG_LEVEL", "INFO").strip() or "INFO"
    json_logs = _bool_env(os.getenv("DAL_OBSCURA_JSON_LOGS"))
    tls_verify_client = _bool_env(os.getenv("DAL_OBSCURA_TLS_VERIFY_CLIENT"))
    return DataPlaneRuntimeConfig(
        database_url=database_url,
        cell_id=cell_id,
        location=location,
        ticket_secret=ticket_secret,
        log_level=log_level,
        json_logs=json_logs,
        tls_cert=_optional_env("DAL_OBSCURA_TLS_CERT"),
        tls_key=_optional_env("DAL_OBSCURA_TLS_KEY"),
        tls_client_ca=_optional_env("DAL_OBSCURA_TLS_CLIENT_CA"),
        tls_verify_client=tls_verify_client,
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


def _bool_env(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}
