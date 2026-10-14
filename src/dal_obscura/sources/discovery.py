from __future__ import annotations

from datetime import UTC, datetime, timedelta
from threading import BoundedSemaphore

from dal_obscura_plugin_api import (
    CatalogConfig,
    CatalogPlugin,
    DiscoveryPage,
    ExecutionContext,
    TableIdentifier,
)

from dal_obscura.plugins import PluginRegistry

ICEBERG_CATALOG_ID = "iceberg.catalog"

DEFAULT_MAX_PAGE_ENTRIES = 500
DEFAULT_DEADLINE_SECONDS = 30.0
DEFAULT_MAX_ACTIVE_DISCOVERIES = 8
_DISCOVERY_SLOTS = BoundedSemaphore(DEFAULT_MAX_ACTIVE_DISCOVERIES)


def discover_public_catalog_tables(
    catalog_name: str,
    plugin_id: str,
    options: dict[str, object],
    *,
    revision: int = 0,
    plugin_registry: PluginRegistry | None,
    continuation: str | None = None,
    limit: int = 100,
) -> DiscoveryPage:
    """Read one bounded page, without enumerating unrelated namespaces."""

    if type(limit) is not int or not 1 <= limit <= DEFAULT_MAX_PAGE_ENTRIES:
        raise ValueError("Invalid catalog discovery page size")
    _validate_continuation(continuation)
    if plugin_registry is None:
        raise ValueError("Provider IO requires a startup plugin registry")
    if not _DISCOVERY_SLOTS.acquire(blocking=False):
        raise RuntimeError("Catalog discovery capacity is exhausted; retry later")
    plugin: CatalogPlugin | None = None
    try:
        factory = plugin_registry.load("catalog", plugin_id)
        context = ExecutionContext(
            deadline=datetime.now(UTC) + timedelta(seconds=DEFAULT_DEADLINE_SECONDS),
            correlation_id=f"catalog-discovery-{catalog_name}",
        )
        plugin = factory(
            CatalogConfig(
                plugin_id=plugin_id,
                instance_id=catalog_name,
                revision=revision,
                options=dict(options),
            ),
            context,
        )
        required = ("list_namespaces", "list_tables", "close")
        if not all(callable(getattr(plugin, name, None)) for name in required):
            raise ValueError("Catalog plugin does not implement the required lifecycle")
        context.check_active()
        page = plugin.list_tables(context, continuation=continuation, limit=limit)
        context.check_active()
        entries = _public_page_entries(page, limit=limit)
        from dal_obscura_plugin_api.identifiers import identifier_name

        names: set[str] = set()
        for identifier in entries:
            if not isinstance(identifier, TableIdentifier):
                raise ValueError("Catalog plugin returned an invalid table identifier")
            name = identifier_name(identifier)
            if name in names:
                raise ValueError("Catalog plugin returned duplicate table identifiers")
            names.add(name)
        _validate_continuation(page.continuation)
        if page.continuation is not None and page.continuation == continuation:
            raise ValueError("Catalog plugin returned a non-progressing continuation")
        return page
    finally:
        from dal_obscura_plugin_api.execution import close_preserving_error

        try:
            close_preserving_error(plugin)
        finally:
            _DISCOVERY_SLOTS.release()


def _validate_continuation(token: str | None) -> None:
    if token is not None and (
        not isinstance(token, str)
        or not token
        or len(token) > 4_096
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in token)
    ):
        raise ValueError("Catalog plugin returned an invalid continuation token")


def _public_page_entries(page: object, *, limit: int) -> tuple[TableIdentifier, ...]:
    if not isinstance(page, DiscoveryPage):
        raise ValueError("Catalog plugin returned an invalid discovery page")
    entries = page.entries
    if not isinstance(entries, tuple):
        raise ValueError("Catalog plugin returned an invalid discovery page")
    if len(entries) > limit:
        raise ValueError("Catalog plugin returned too many page entries")
    return entries
