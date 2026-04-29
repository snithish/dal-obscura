from __future__ import annotations

import importlib
import logging
import os
from collections.abc import Mapping
from typing import cast

from dal_obscura.application.ports.identity import IdentityPort
from dal_obscura.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.application.use_cases.get_schema import GetSchemaUseCase
from dal_obscura.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.control_plane.infrastructure.db import create_engine_from_url, session_factory
from dal_obscura.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from dal_obscura.infrastructure.adapters.identity_composite import CompositeIdentityProvider
from dal_obscura.infrastructure.adapters.published_config import (
    PublishedConfigAuthorizer,
    PublishedConfigCatalogRegistry,
    PublishedConfigStore,
    PublishedRuntime,
)
from dal_obscura.infrastructure.adapters.runtime_config import load_data_plane_runtime_config
from dal_obscura.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter
from dal_obscura.interfaces.flight.server import DataAccessFlightService
from dal_obscura.logging_config import LoggingConfig, setup_logging

LOGGER = logging.getLogger(__name__)


def main() -> None:
    """CLI entry point that wires the data plane from published control-plane state."""
    runtime_config = load_data_plane_runtime_config()
    setup_logging(LoggingConfig(level=runtime_config.log_level, json=runtime_config.json_logs))
    LOGGER.info("Starting dal-obscura data plane")

    engine = create_engine_from_url(runtime_config.database_url)
    session = session_factory(engine)()
    config_store = PublishedConfigStore(session, cell_id=runtime_config.cell_id)
    published_runtime = config_store.get_runtime()

    identity = _identity_from_runtime(published_runtime)
    authorizer = PublishedConfigAuthorizer(config_store)
    catalog_registry = PublishedConfigCatalogRegistry(config_store)
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(masking)
    ticket_codec = HmacTicketCodecAdapter(runtime_config.ticket_secret)
    ticket_settings = published_runtime.ticket

    get_schema = GetSchemaUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=catalog_registry,
        masking=masking,
    )
    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=catalog_registry,
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=int(ticket_settings.get("ttl_seconds", 300)),
        max_tickets=int(ticket_settings.get("max_tickets", 1)),
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=authorizer,
        masking=masking,
        row_transform=row_transform,
        ticket_codec=ticket_codec,
    )
    server = DataAccessFlightService(
        location=runtime_config.location,
        get_schema_use_case=get_schema,
        plan_access_use_case=plan_access,
        fetch_stream_use_case=fetch_stream,
    )
    try:
        server.serve()
    finally:
        session.close()


def _identity_from_runtime(runtime: PublishedRuntime) -> IdentityPort:
    providers_raw = _provider_records(runtime.auth_chain.get("providers", []))
    if not providers_raw:
        raise ValueError("Published runtime auth_chain must define at least one provider")

    providers = [
        _load_identity_provider(provider)
        for provider in sorted(providers_raw, key=lambda item: int(item.get("ordinal", 0)))
        if bool(provider.get("enabled", True))
    ]
    if not providers:
        raise ValueError("Published runtime auth_chain has no enabled providers")
    if len(providers) == 1:
        return providers[0]
    return CompositeIdentityProvider(providers)


def _load_identity_provider(raw: dict[str, object]) -> IdentityPort:
    module_path = str(raw["module"])
    args = cast(dict[str, object], _resolve_secret_refs(raw.get("args", {})))
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


def _resolve_secret_refs(value: object) -> object:
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        secret_key = mapping.get("key")
        if set(mapping) == {"key"} and isinstance(secret_key, str):
            return _resolve_env_secret(secret_key)
        return {str(key): _resolve_secret_refs(nested) for key, nested in mapping.items()}
    if isinstance(value, list):
        return [_resolve_secret_refs(item) for item in value]
    return value


def _resolve_env_secret(key: str) -> str:
    value = os.getenv(key)
    if value is None or not value:
        raise ValueError(f"Secret {key!r} could not be resolved from the environment")
    return value


def _provider_records(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [cast(dict[str, object], item) for item in value if isinstance(item, dict)]
