from __future__ import annotations

import importlib
import logging
from typing import cast

import pyarrow.flight as flight

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    ensure_config_store_schema,
    session_factory,
)
from dal_obscura.data_plane.application.access_flow import AccessFlow
from dal_obscura.data_plane.application.ports.identity import IdentityPort
from dal_obscura.data_plane.application.use_cases.get_schema import GetSchemaUseCase
from dal_obscura.data_plane.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from dal_obscura.data_plane.infrastructure.adapters.identity_composite import (
    CompositeIdentityProvider,
)
from dal_obscura.data_plane.infrastructure.adapters.published_config import (
    PublishedConfigAuthorizer,
    PublishedConfigCatalogRegistry,
    PublishedConfigStore,
    PublishedRuntime,
)
from dal_obscura.data_plane.infrastructure.adapters.runtime_config import (
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
from dal_obscura.logging_config import LoggingConfig, setup_logging

LOGGER = logging.getLogger(__name__)


def main() -> None:
    """CLI entry point that wires the data plane from published control-plane state."""
    runtime_config = load_data_plane_runtime_config()
    setup_logging(LoggingConfig(level=runtime_config.log_level, json=runtime_config.json_logs))
    LOGGER.info("Starting dal-obscura data plane")

    engine = create_engine_from_url(runtime_config.database_url)
    ensure_config_store_schema(engine)
    session_maker = session_factory(engine)
    session = session_maker()
    config_store = PublishedConfigStore(session, cell_id=runtime_config.cell_id)
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
    row_transform = DuckDBRowTransformAdapter(masking)
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
    try:
        server.serve()
    finally:
        session.close()


def _identity_from_runtime(
    runtime: PublishedRuntime,
    *,
    secret_provider: SecretProvider,
) -> IdentityPort:
    providers_raw = _provider_records(runtime.auth_chain.get("providers", []))
    if not providers_raw:
        raise ValueError("Published runtime auth_chain must define at least one provider")

    providers = [
        _load_identity_provider(provider, secret_provider=secret_provider)
        for provider in sorted(providers_raw, key=lambda item: int(item.get("ordinal", 0)))
        if bool(provider.get("enabled", True))
    ]
    if not providers:
        raise ValueError("Published runtime auth_chain has no enabled providers")
    if len(providers) == 1:
        return providers[0]
    return CompositeIdentityProvider(providers)


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
    module_path = str(raw["module"])
    args = cast(
        dict[str, object],
        resolve_secret_refs(raw.get("args", {}), provider=secret_provider),
    )
    provider_cls = _load_class(module_path)
    provider = provider_cls(**args)
    authenticate = getattr(provider, "authenticate", None)
    if not callable(authenticate):
        raise ValueError(f"Auth provider {module_path!r} must define authenticate(request)")
    return cast(IdentityPort, provider)


def _load_class(module_path: str) -> type:
    module_name, class_name = module_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    provider_cls = getattr(module, class_name, None)
    if not isinstance(provider_cls, type):
        raise ValueError(f"Auth provider {module_path!r} must be a class")
    return provider_cls


def _provider_records(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [cast(dict[str, object], item) for item in value if isinstance(item, dict)]
