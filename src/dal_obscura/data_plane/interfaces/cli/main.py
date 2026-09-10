from __future__ import annotations

import logging
import socket
import threading
from typing import Any, cast

import pyarrow.flight as flight
import uvicorn
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.common.config_store.db import (
    check_config_store_schema,
    create_engine_from_url,
    session_factory,
)
from dal_obscura.data_plane.application.access_flow import AccessFlow
from dal_obscura.data_plane.application.ports.identity import IdentityPort
from dal_obscura.data_plane.application.use_cases.get_schema import GetSchemaUseCase
from dal_obscura.data_plane.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks import (
    OidcJwksIdentityProvider,
)
from dal_obscura.data_plane.infrastructure.adapters.published_config import (
    PublishedConfigAuthorizer,
    PublishedConfigCatalogRegistry,
    PublishedConfigStore,
    PublishedRuntime,
)
from dal_obscura.data_plane.infrastructure.adapters.runtime_config import (
    DataPlaneRuntimeConfig,
    load_data_plane_runtime_config,
)
from dal_obscura.data_plane.infrastructure.adapters.secret_providers import (
    SecretProvider,
    SecretProviderContext,
    load_secret_provider,
    resolve_secret_refs,
)
from dal_obscura.data_plane.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter
from dal_obscura.data_plane.infrastructure.adapters.ticket_store_sqlalchemy import (
    SqlAlchemyTicketStore,
)
from dal_obscura.data_plane.interfaces.flight.server import DataAccessFlightService
from dal_obscura.data_plane.interfaces.health import create_health_app, published_runtime_readiness
from dal_obscura.logging_config import LoggingConfig, setup_logging

LOGGER = logging.getLogger(__name__)

_OIDC_IDENTITY_PROVIDER = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
)


def main() -> None:
    """CLI entry point that wires the data plane from published control-plane state."""
    runtime_config = load_data_plane_runtime_config()
    setup_logging(LoggingConfig(level=runtime_config.log_level, json=runtime_config.json_logs))
    LOGGER.info("Starting dal-obscura data plane")

    engine = create_engine_from_url(runtime_config.database_url)
    check_config_store_schema(engine)
    session_maker = session_factory(engine)
    _start_health_server(session_maker, runtime_config)
    config_store = PublishedConfigStore(
        session_maker,
        cell_id=runtime_config.cell_id,
    )
    published_runtime = config_store.get_runtime()
    secret_provider = load_secret_provider(
        runtime_config.secret_provider,
        context=SecretProviderContext(
            database_url=runtime_config.database_url,
            cell_id=runtime_config.cell_id,
        ),
    )

    identity = _identity_from_runtime(published_runtime, secret_provider=secret_provider)
    authorizer = PublishedConfigAuthorizer(config_store)
    catalog_registry = PublishedConfigCatalogRegistry(config_store, secret_provider=secret_provider)
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(
        masking,
        max_active_streams=runtime_config.max_active_streams,
        duckdb_memory_limit=runtime_config.duckdb_memory_limit,
        max_input_batch_bytes=runtime_config.max_input_batch_bytes,
        max_output_batch_bytes=runtime_config.max_output_batch_bytes,
    )
    ticket_codec = HmacTicketCodecAdapter(runtime_config.ticket_secret)
    ticket_store = SqlAlchemyTicketStore(session_maker, cell_id=runtime_config.cell_id)
    ticket_settings = published_runtime.ticket
    access_flow = AccessFlow(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=catalog_registry,
        masking=masking,
        row_transform=row_transform,
        ticket_codec=ticket_codec,
        ticket_store=ticket_store,
        ticket_ttl_seconds=int(ticket_settings.get("ttl_seconds", 300)),
        max_tickets=int(ticket_settings.get("max_tickets", 1)),
        max_ticket_exchanges=int(ticket_settings.get("max_exchanges", 1)),
        max_ticket_payload_bytes=runtime_config.max_ticket_payload_bytes,
        max_stream_seconds=runtime_config.max_stream_seconds,
    )

    get_schema = GetSchemaUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=catalog_registry,
        masking=masking,
    )
    server = DataAccessFlightService(
        location=runtime_config.location,
        get_schema_use_case=get_schema,
        access_flow=access_flow,
        tls_certificates=_tls_certificates(
            cert=runtime_config.tls_cert,
            key=runtime_config.tls_key,
        ),
        verify_client=runtime_config.tls_verify_client,
        root_certificates=_tls_root_certificates(runtime_config.tls_client_ca),
    )
    server.serve()


def _identity_from_runtime(
    runtime: PublishedRuntime,
    *,
    secret_provider: SecretProvider,
) -> IdentityPort:
    providers_raw = _provider_records(runtime.auth_chain.get("providers", []))
    if not providers_raw:
        raise ValueError("Published runtime auth_chain must define at least one provider")

    enabled = [provider for provider in providers_raw if bool(provider.get("enabled", True))]
    if not enabled:
        raise ValueError("Published runtime auth_chain has no enabled providers")
    if len(enabled) != 1:
        raise ValueError("Published runtime must define exactly one enabled OIDC provider")
    return _load_identity_provider(enabled[0], secret_provider=secret_provider)


def _start_health_server(
    session_maker: sessionmaker[Session],
    runtime_config: DataPlaneRuntimeConfig,
) -> None:
    if runtime_config.health_port is None:
        return
    health_socket = _bind_health_socket(runtime_config.health_host, runtime_config.health_port)

    def readiness() -> dict[str, object]:
        with session_maker() as health_session:
            store = PublishedConfigStore(
                health_session,
                cell_id=runtime_config.cell_id,
            )
            return published_runtime_readiness(store)

    app = create_health_app(readiness=readiness)
    config = uvicorn.Config(
        app,
        host=runtime_config.health_host,
        port=runtime_config.health_port,
        log_level=runtime_config.log_level.lower(),
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(
        target=lambda: server.run(sockets=[health_socket]),
        name="dal-obscura-health",
        daemon=True,
    )
    thread.start()


def _bind_health_socket(host: str, port: int) -> socket.socket:
    health_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        health_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        health_socket.bind((host, port))
        health_socket.listen()
        return health_socket
    except OSError:
        health_socket.close()
        raise


def _tls_certificates(cert: str | None, key: str | None) -> list[flight.CertKeyPair] | None:
    if cert is None and key is None:
        return None
    if cert is None or key is None:
        raise ValueError("DAL_OBSCURA_TLS_CERT and DAL_OBSCURA_TLS_KEY must be set together")
    return [flight.CertKeyPair(cert.encode("utf-8"), key.encode("utf-8"))]


def _tls_root_certificates(client_ca: str | None) -> bytes | None:
    if client_ca is None:
        return None
    return client_ca.encode("utf-8")


def _load_identity_provider(
    raw: dict[str, object],
    *,
    secret_provider: SecretProvider,
) -> IdentityPort:
    module_path = raw.get("module")
    if module_path != _OIDC_IDENTITY_PROVIDER:
        raise ValueError("Unsupported identity provider; only built-in OIDC is supported")
    args = cast(
        dict[str, object],
        resolve_secret_refs(raw.get("args", {}), provider=secret_provider),
    )
    return OidcJwksIdentityProvider(**cast(Any, args))


def _provider_records(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [cast(dict[str, object], item) for item in value if isinstance(item, dict)]
