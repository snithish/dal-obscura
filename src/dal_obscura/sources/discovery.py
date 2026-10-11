from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime, timedelta
from threading import BoundedSemaphore
from time import monotonic
from typing import cast

from dal_obscura_plugin_api import (
    CatalogConfig,
    CatalogPlugin,
    DiscoveryPage,
    ExecutionContext,
    TableIdentifier,
)
from pyiceberg.catalog import Catalog

from dal_obscura.sources.plugins import PluginRegistry

ICEBERG_CATALOG_ID = "iceberg.catalog"

Namespace = tuple[str, ...]
DEFAULT_MAX_NAMESPACES = 1_000
DEFAULT_MAX_TABLES = 10_000
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
        from dal_obscura.sources.builtins import create_builtin_plugin_registry

        plugin_registry = create_builtin_plugin_registry()
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
        from dal_obscura.sources.plugin_runtime import _identifier_name

        names: set[str] = set()
        for identifier in entries:
            if not isinstance(identifier, TableIdentifier):
                raise ValueError("Catalog plugin returned an invalid table identifier")
            name = _identifier_name(identifier)
            if name in names:
                raise ValueError("Catalog plugin returned duplicate table identifiers")
            names.add(name)
        _validate_continuation(page.continuation)
        if page.continuation is not None and page.continuation == continuation:
            raise ValueError("Catalog plugin returned a non-progressing continuation")
        return page
    finally:
        from dal_obscura.sources.plugin_runtime import _close_plugin_preserving_error

        try:
            _close_plugin_preserving_error(plugin)
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


def _walk_namespaces(
    catalog: Catalog,
    *,
    max_namespaces: int = DEFAULT_MAX_NAMESPACES,
    deadline_at: float | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> list[Namespace]:
    namespaces: list[Namespace] = []
    pending: deque[Namespace] = deque([()])
    seen: set[Namespace] = set()
    while pending:
        namespace = pending.popleft()
        if namespace in seen:
            continue
        _check_budget(deadline_at, cancel_check)
        if len(seen) >= max_namespaces:
            raise ValueError(f"Catalog discovery exceeded the {max_namespaces}-namespace limit")
        seen.add(namespace)
        namespaces.append(namespace)
        for child in _bounded_provider_items(
            catalog.list_namespaces(namespace),
            limit=max_namespaces - len(seen),
            kind="namespace",
            deadline_at=deadline_at,
            cancel_check=cancel_check,
        ):
            pending.append(_namespace_tuple(child))
    return namespaces


def _list_tables(catalog: Catalog, namespace: Namespace) -> Iterable[object]:
    try:
        return catalog.list_tables(namespace)
    except ValueError as exc:
        # PyIceberg SQL catalogs reject the root namespace instead of returning
        # an empty result. Tables still appear under the namespaces discovered
        # by _walk_namespaces; retain other provider errors.
        if not namespace and str(exc) == "Empty namespace identifier":
            return ()
        raise


def _namespace_tuple(namespace: object) -> Namespace:
    if isinstance(namespace, str):
        parts = tuple(namespace.split("."))
        if any(not _valid_identifier_segment(part) for part in parts):
            raise ValueError("Catalog provider returned an invalid namespace")
        return parts
    if isinstance(namespace, tuple):
        if any(not _valid_identifier_segment(part) for part in namespace):
            raise ValueError("Catalog provider returned an invalid namespace")
        return cast(tuple[str, ...], namespace)
    if isinstance(namespace, list):
        if any(not _valid_identifier_segment(part) for part in namespace):
            raise ValueError("Catalog provider returned an invalid namespace")
        return cast(tuple[str, ...], tuple(namespace))
    raise ValueError("Catalog provider returned an invalid namespace")


def _identifier_to_name(identifier: object) -> str:
    from dal_obscura.sources.plugin_runtime import _identifier_name, _table_identifier

    if isinstance(identifier, str):
        try:
            return _identifier_name(_table_identifier(identifier))
        except ValueError as exc:
            raise ValueError("Catalog provider returned an invalid table identifier") from exc
    if isinstance(identifier, (tuple, list)):
        if any(not _valid_identifier_segment(part) for part in identifier):
            raise ValueError("Catalog provider returned an invalid table identifier")
        parts = cast(tuple[str, ...], tuple(identifier))
        if not parts:
            raise ValueError("Catalog provider returned an invalid table identifier")
        return _identifier_name(TableIdentifier(namespace=parts[:-1], name=parts[-1]))
    raise ValueError("Catalog provider returned an invalid table identifier")


def _valid_identifier_segment(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and len(value) <= 256
        and all(ord(char) >= 0x20 and ord(char) != 0x7F for char in value)
    )


def _bounded_provider_items(
    values: Iterable[object],
    *,
    limit: int,
    kind: str,
    deadline_at: float | None,
    cancel_check: Callable[[], bool] | None,
) -> Iterator[object]:
    if limit < 0:
        raise ValueError(f"Catalog discovery exceeded the {kind} limit")
    for index, value in enumerate(values):
        _check_budget(deadline_at, cancel_check)
        if index >= limit:
            raise ValueError(f"Catalog discovery exceeded the {kind} limit")
        yield value


def _check_budget(deadline_at: float | None, cancel_check: Callable[[], bool] | None) -> None:
    if cancel_check is not None and cancel_check():
        raise RuntimeError("Catalog discovery cancelled")
    if deadline_at is not None and monotonic() >= deadline_at:
        raise TimeoutError("Catalog discovery deadline exceeded")
