"""Built-in SQL Iceberg catalog using the same public SDK as installed plugins."""

from __future__ import annotations

from datetime import datetime, timezone
from time import monotonic
from typing import Any

from dal_obscura_plugin_api import (
    CatalogConfig,
    DiscoveryPage,
    ExecutionContext,
    PluginDescriptor,
    TableHandle,
    TableIdentifier,
)

from dal_obscura import __version__
from dal_obscura.sources.discovery import (
    DEFAULT_MAX_NAMESPACES,
    DEFAULT_MAX_TABLES,
    _bounded_provider_items,
    _identifier_to_name,
    _list_tables,
    _namespace_tuple,
    _walk_namespaces,
)
from dal_obscura.sources.plugin_runtime import _close_plugin_preserving_error, _table_identifier

_ICEBERG_CATALOG_ID = "iceberg.sql"
_ICEBERG_FORMAT_ID = "iceberg"
_BUILTIN_DISTRIBUTION = "dal-obscura"
_BUILTIN_VERSION = __version__
DESCRIPTOR = PluginDescriptor(
    kind="catalog",
    plugin_id=_ICEBERG_CATALOG_ID,
    api_version="2",
    config_version=1,
    distribution=_BUILTIN_DISTRIBUTION,
    version=_BUILTIN_VERSION,
    display_name="Iceberg SQL catalog",
    capabilities=frozenset({"nested_schema", "snapshot_reads", "splittable_scan"}),
    output_formats=frozenset({_ICEBERG_FORMAT_ID}),
    handle_versions=frozenset({1}),
    config_schema={
        "defaults": {"type": "sql"},
        "fields": [
            {"name": "uri", "type": "string", "required": True, "secret": False},
            {"name": "warehouse", "type": "string", "required": False, "secret": False},
            {"name": "user", "type": "string", "required": False, "secret": False},
            {
                "name": "password",
                "type": "secret_reference",
                "required": False,
                "secret": True,
            },
        ],
    },
)


class SqlCatalog:
    descriptor = DESCRIPTOR

    def __init__(self, config: CatalogConfig, context: ExecutionContext) -> None:
        self.config = config
        self._closed = False
        self._listing_context: ExecutionContext | None = None
        self._listing: tuple[str, ...] = ()
        context.check_active()
        if config.plugin_id != self.descriptor.plugin_id:
            raise ValueError("SQL catalog received an incompatible plugin ID")
        options = dict(config.options)
        self._catalog = _load_iceberg_catalog(
            _provider_catalog_name(config.instance_id, options), _catalog_options(options)
        )
        try:
            context.check_active()
        except BaseException:
            _close_plugin_preserving_error(self)
            raise

    def _provider(self, context: ExecutionContext):
        context.check_active()
        if self._closed:
            raise ValueError("SQL catalog is closed")
        return self._catalog

    def resolve_table(self, identifier: TableIdentifier, context: ExecutionContext) -> TableHandle:
        table = self._provider(context).load_table((*identifier.namespace, identifier.name))
        context.check_active()
        location = table.metadata_location
        if not isinstance(location, str) or not location.strip():
            raise ValueError("Iceberg catalog returned no metadata location")
        snapshot = getattr(getattr(table, "metadata", None), "current_snapshot_id", None)
        return TableHandle(
            catalog_plugin_id=self.descriptor.plugin_id,
            catalog_instance_id=self.config.instance_id,
            catalog_revision=self.config.revision,
            identifier=identifier,
            format_plugin_id="iceberg",
            handle_version=1,
            snapshot_id=None if snapshot is None else str(snapshot),
            metadata={"metadata_location": location},
        )

    def list_namespaces(self, context: ExecutionContext, *, namespace=()):
        provider = self._provider(context)
        values = provider.list_namespaces(namespace)
        return tuple(
            _namespace_tuple(value)
            for value in _bounded_provider_items(
                values,
                limit=DEFAULT_MAX_NAMESPACES,
                kind="namespace",
                cancel_check=context.cancel_check,
                deadline_at=_deadline_at(context),
            )
        )

    def list_tables(
        self, context: ExecutionContext, *, continuation=None, limit=500
    ) -> DiscoveryPage:
        provider = self._provider(context)
        if not 1 <= limit <= 500:
            raise ValueError("Invalid SQL discovery page size")
        if context is not self._listing_context:
            deadline_at = _deadline_at(context)
            identifiers = set()
            for namespace in _walk_namespaces(
                provider,
                max_namespaces=DEFAULT_MAX_NAMESPACES,
                cancel_check=context.cancel_check,
                deadline_at=deadline_at,
            ):
                for identifier in _bounded_provider_items(
                    _list_tables(provider, namespace),
                    limit=DEFAULT_MAX_TABLES - len(identifiers),
                    kind="table",
                    cancel_check=context.cancel_check,
                    deadline_at=deadline_at,
                ):
                    context.check_active()
                    identifiers.add(_identifier_to_name(identifier))
            self._listing = tuple(sorted(identifiers))
            self._listing_context = context
        names = self._listing
        if continuation is not None:
            if not isinstance(continuation, str) or continuation not in names:
                raise ValueError("Invalid SQL discovery cursor")
            names = [name for name in names if name > continuation]
        page = names[:limit]
        return DiscoveryPage(
            tuple(_table_identifier(name) for name in page),
            page[-1] if len(names) > limit else None,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._listing_context = None
        self._listing = ()
        catalog, self._catalog = self._catalog, None
        close = getattr(catalog, "close", None)
        if callable(close):
            close()


def _deadline_at(context: ExecutionContext) -> float:
    return monotonic() + (context.deadline - datetime.now(timezone.utc)).total_seconds()


def _provider_catalog_name(logical_name: str, options: dict[str, Any]) -> str:
    if "catalog_name" in options:
        raise ValueError("Unsupported catalog option: catalog_name")
    provider_name = options.get("provider_catalog_name")
    return str(provider_name) if provider_name else logical_name


def _catalog_options(options: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(options)
    cleaned.pop("provider_catalog_name", None)
    return cleaned


def _load_iceberg_catalog(catalog_name: str, catalog_options: dict[str, Any]) -> Any:
    from pyiceberg.catalog import load_catalog

    try:
        return load_catalog(catalog_name, **catalog_options)
    except Exception as exc:
        raise ValueError(f"Failed to load catalog {catalog_name!r}: {exc}") from exc
