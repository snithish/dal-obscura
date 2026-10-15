from __future__ import annotations

import logging
import socket
import threading
import time
from contextlib import ExitStack
from typing import Any, cast

import uvicorn
from dal_obscura_plugin_api.storage import PathRuleEnforcer
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.adapters.duckdb.compiler import DuckDBProjectionCompiler
from dal_obscura.adapters.duckdb.transform import DuckDBRowTransformAdapter
from dal_obscura.adapters.providers.published import LiveConfigCatalogRegistry
from dal_obscura.adapters.providers.task_codec import SourceTaskCodec
from dal_obscura.adapters.sql.database.db import (
    check_config_store_schema,
    create_engine_from_url,
    session_factory,
)
from dal_obscura.adapters.sql.snapshots import LiveConfigStore
from dal_obscura.adapters.sql.tickets import SqlAlchemyTicketStore
from dal_obscura.adapters.tickets.signing import HmacTicketCodecAdapter
from dal_obscura.application.reads.execution import ExecutionSupervisor
from dal_obscura.application.reads.ports import IdentityPort
from dal_obscura.application.reads.service import ReadService, ReadSettings
from dal_obscura.application.reads.sources import LiveRuntime
from dal_obscura.application.reads.tickets.contracts import TicketStorePort
from dal_obscura.identity.oidc import OidcJwksIdentityProvider
from dal_obscura.interfaces.flight.server import DataAccessFlightService
from dal_obscura.interfaces.flight.tls import _tls_certificates, _tls_root_certificates
from dal_obscura.interfaces.read_health import create_health_app, live_runtime_readiness
from dal_obscura.logging_config import LoggingConfig, setup_logging
from dal_obscura.plugins import load_plugin_lock_file
from dal_obscura.plugins.bootstrap import create_plugin_registry
from dal_obscura.runtime.configuration import DataPlaneRuntimeConfig, load_data_plane_runtime_config
from dal_obscura.runtime.secrets import SecretProvider, load_secret_provider, resolve_secret_refs

LOGGER = logging.getLogger(__name__)

_OIDC_IDENTITY_PROVIDER = "dal_obscura.identity.oidc.OidcJwksIdentityProvider"


def run_read_service_from_environment() -> None:
    runtime_config = load_data_plane_runtime_config()
    setup_logging(LoggingConfig(level=runtime_config.log_level, json=runtime_config.json_logs))
    run_read_service(runtime_config)


def run_read_service(runtime_config: DataPlaneRuntimeConfig) -> None:
    """Own read dependencies and background resources through server shutdown."""
    LOGGER.info("Starting dal-obscura data plane")
    with ExitStack() as resources:
        engine = create_engine_from_url(runtime_config.database_url)
        resources.callback(engine.dispose)
        check_config_store_schema(engine)
        session_maker = session_factory(engine)
        health_server = _start_health_server(session_maker, runtime_config)
        if health_server is not None:
            resources.callback(health_server.close)
        config_store = LiveConfigStore(session_maker)
        live_runtime = config_store.get_runtime()
        plugin_registry = create_plugin_registry(
            allowlist=(
                load_plugin_lock_file(runtime_config.plugin_lock_file)
                if runtime_config.plugin_lock_file
                else None
            )
        )
        secret_provider = load_secret_provider(
            runtime_config.secret_provider,
            registry=plugin_registry,
        )

        resources.callback(secret_provider.close)
        identity = _identity_from_runtime(live_runtime, secret_provider=secret_provider)
        catalog_registry = LiveConfigCatalogRegistry(
            config_store,
            secret_provider=secret_provider,
            plugin_registry=plugin_registry,
            path_enforcer=PathRuleEnforcer(live_runtime.path_rules),
            max_cached_providers=runtime_config.max_cached_catalog_providers,
            provider_wait_seconds=runtime_config.catalog_provider_wait_seconds,
        )

        resources.callback(catalog_registry.close)
        row_transform = DuckDBRowTransformAdapter(
            duckdb_memory_limit=runtime_config.duckdb_memory_limit,
            max_input_batch_bytes=runtime_config.max_input_batch_bytes,
            max_output_batch_bytes=runtime_config.max_output_batch_bytes,
        )
        ticket_codec = HmacTicketCodecAdapter(
            runtime_config.ticket_secret, previous_secrets=runtime_config.ticket_previous_secrets
        )
        ticket_store = SqlAlchemyTicketStore(session_maker)
        cleanup = TicketCleanupWorker(
            ticket_store, interval_seconds=runtime_config.ticket_cleanup_interval_seconds
        )
        cleanup.start()
        resources.callback(cleanup.close)
        ticket_settings = live_runtime.ticket
        reads = ReadService(
            execution_supervisor=ExecutionSupervisor(
                max_active_streams=runtime_config.max_active_streams
            ),
            projection_compiler=DuckDBProjectionCompiler(),
            identity=identity,
            access_context=catalog_registry,
            row_transform=row_transform,
            ticket_codec=ticket_codec,
            ticket_store=ticket_store,
            task_codec=SourceTaskCodec(plugin_registry),
            settings=ReadSettings(
                ticket_ttl_seconds=int(ticket_settings.get("ttl_seconds", 300)),
                max_tickets=int(ticket_settings.get("max_tickets", 1)),
                max_ticket_exchanges=int(ticket_settings.get("max_exchanges", 1)),
                max_ticket_payload_bytes=runtime_config.max_ticket_payload_bytes,
                max_stream_seconds=runtime_config.max_stream_seconds,
            ),
        )

        server = DataAccessFlightService(
            location=runtime_config.location,
            reads=reads,
            tls_certificates=_tls_certificates(
                cert=runtime_config.tls_cert, key=runtime_config.tls_key
            ),
            verify_client=runtime_config.tls_verify_client,
            root_certificates=_tls_root_certificates(runtime_config.tls_client_ca),
            health_check=lambda: _live_runtime_readiness(session_maker, runtime_config),
        )
        resources.callback(server.shutdown)
        server.serve()


def _identity_from_runtime(
    runtime: LiveRuntime,
    *,
    secret_provider: SecretProvider,
) -> IdentityPort:
    providers_raw = _provider_records(runtime.auth_chain.get("providers", []))
    if not providers_raw:
        raise ValueError("Live runtime auth_chain must define at least one provider")

    enabled = [provider for provider in providers_raw if bool(provider.get("enabled", True))]
    if not enabled:
        raise ValueError("Live runtime auth_chain has no enabled providers")
    if len(enabled) != 1:
        raise ValueError("Live runtime must define exactly one enabled OIDC provider")
    return _load_identity_provider(enabled[0], secret_provider=secret_provider)


def _live_runtime_readiness(
    session_maker: sessionmaker[Session],
    runtime_config: DataPlaneRuntimeConfig,
) -> dict[str, object]:
    return live_runtime_readiness(LiveConfigStore(session_maker))


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
        resolve_secret_refs(
            raw.get("args", {}), provider=secret_provider, expected_scope="identity"
        ),
    )
    return OidcJwksIdentityProvider(**cast(Any, args))


def _provider_records(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [cast(dict[str, object], item) for item in value if isinstance(item, dict)]


class TicketCleanupWorker:
    """A periodic cleanup worker whose serving process owns stop and join."""

    def __init__(self, ticket_store: TicketStorePort, *, interval_seconds: int) -> None:
        if interval_seconds <= 0:
            raise ValueError("ticket cleanup interval must be positive")
        self._ticket_store = ticket_store
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="dal-obscura-ticket-cleanup")

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread.ident is not None:
            self._thread.join()

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                deleted = self._ticket_store.cleanup_expired(now=int(time.time()))
                if deleted:
                    LOGGER.info("ticket_cleanup", extra={"deleted": deleted})
            except Exception:
                # Cleanup failures may contain database credentials or internal paths.
                LOGGER.warning("ticket_cleanup_failed")


class _HealthServerOwner:
    def __init__(
        self, server: uvicorn.Server, thread: threading.Thread, health_socket: socket.socket
    ) -> None:
        self._server = server
        self._thread = thread
        self._socket = health_socket

    def close(self) -> None:
        self._server.should_exit = True
        try:
            self._thread.join()
        finally:
            self._socket.close()


def _start_health_server(
    session_maker: sessionmaker[Session],
    runtime_config: DataPlaneRuntimeConfig,
) -> _HealthServerOwner | None:
    if runtime_config.health_port is None:
        return None
    health_socket = _bind_health_socket(runtime_config.health_host, runtime_config.health_port)

    def readiness() -> dict[str, object]:
        return _live_runtime_readiness(session_maker, runtime_config)

    try:
        app = create_health_app(readiness=readiness)
        config = uvicorn.Config(
            app,
            host=runtime_config.health_host,
            port=runtime_config.health_port,
            log_level=runtime_config.log_level.lower(),
            ws="none",
        )
        config.load()
    except BaseException:
        health_socket.close()
        raise
    server = uvicorn.Server(config)
    thread = threading.Thread(
        target=lambda: server.run(sockets=[health_socket]), name="dal-obscura-health"
    )
    try:
        thread.start()
    except BaseException:
        health_socket.close()
        raise
    return _HealthServerOwner(server, thread, health_socket)
