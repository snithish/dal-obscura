from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime, timedelta, timezone
from threading import BoundedSemaphore
from time import monotonic
from typing import Any, cast

from dal_obscura_plugin_api import CatalogFactory, DiscoveryPage, TableIdentifier

ICEBERG_CATALOG_ID = "iceberg.sql"

CatalogTable = dict[str, object]
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
    options: dict[str, Any],
    *,
    revision: int = 0,
    plugin_registry: Any,
) -> list[CatalogTable]:
    """Discover tables through one admitted public catalog plugin."""

    if plugin_registry is None:
        from dal_obscura.sources.builtins import create_builtin_plugin_registry

        plugin_registry = create_builtin_plugin_registry()
    if not _DISCOVERY_SLOTS.acquire(blocking=False):
        raise RuntimeError("Catalog discovery capacity is exhausted; retry later")
    plugin: Any | None = None
    try:
        factory = plugin_registry.load("catalog", plugin_id)
        if not callable(factory):
            raise ValueError("Admitted catalog factory is invalid")
        from dal_obscura_plugin_api import CatalogConfig, ExecutionContext

        context = ExecutionContext(
            deadline=datetime.now(timezone.utc) + timedelta(seconds=DEFAULT_DEADLINE_SECONDS),
            correlation_id=f"catalog-discovery-{catalog_name}",
        )
        plugin = cast(CatalogFactory, factory)(
            CatalogConfig(
                plugin_id=plugin_id,
                instance_id=catalog_name,
                revision=revision,
                options=dict(options),
            ),
            context,
        )
        deadline_at = monotonic() + DEFAULT_DEADLINE_SECONDS
        _validate_public_catalog_lifecycle(plugin, context, deadline_at)
        continuation: str | None = None
        seen: set[str] = set()
        tables: list[CatalogTable] = []
        for _ in range(DEFAULT_MAX_NAMESPACES):
            _check_budget(deadline_at, context.cancel_check)
            page = plugin.list_tables(context, continuation=continuation, limit=500)
            entries = _public_page_entries(page)
            for identifier in entries:
                if not isinstance(identifier, TableIdentifier):
                    raise ValueError("Catalog plugin returned an invalid table identifier")
                from dal_obscura.sources.plugin_runtime import _identifier_name

                name = _identifier_name(identifier)
                tables.append({"backend": plugin_id, "name": name, "table_identifier": name})
                if len(tables) > DEFAULT_MAX_TABLES:
                    raise ValueError("Catalog discovery exceeded the table limit")
            token = page.continuation
            if token is None:
                return tables
            if (
                not isinstance(token, str)
                or not token
                or len(token) > 4_096
                or any(ord(char) < 0x20 or ord(char) == 0x7F for char in token)
                or token in seen
            ):
                raise ValueError("Catalog plugin returned an invalid continuation token")
            seen.add(token)
            continuation = token
        raise ValueError("Catalog discovery exceeded the page limit")
    finally:
        close = getattr(plugin, "close", None)
        try:
            if callable(close):
                close()
        finally:
            _DISCOVERY_SLOTS.release()


def _validate_public_catalog_lifecycle(
    plugin: Any,
    context: Any,
    deadline_at: float,
) -> None:
    required = ("list_namespaces", "list_tables", "close")
    if not all(callable(getattr(plugin, name, None)) for name in required):
        raise ValueError("Catalog plugin does not implement the required lifecycle")
    context.check_active()
    namespaces = plugin.list_namespaces(context, namespace=())
    for index, namespace in enumerate(namespaces):
        _check_budget(deadline_at, context.cancel_check)
        if index >= DEFAULT_MAX_NAMESPACES:
            raise ValueError("Catalog discovery exceeded the namespace limit")
        if not isinstance(namespace, (tuple, list)) or not namespace:
            raise ValueError("Catalog plugin returned an invalid namespace")
        if any(
            not isinstance(segment, str) or not _valid_identifier_segment(segment)
            for segment in namespace
        ):
            raise ValueError("Catalog plugin returned an invalid namespace")


def _public_page_entries(page: object) -> tuple[TableIdentifier, ...]:
    if not isinstance(page, DiscoveryPage):
        raise ValueError("Catalog plugin returned an invalid discovery page")
    entries = page.entries
    if not isinstance(entries, tuple):
        raise ValueError("Catalog plugin returned an invalid discovery page")
    if len(entries) > DEFAULT_MAX_PAGE_ENTRIES:
        raise ValueError("Catalog plugin returned too many page entries")
    return entries


def _walk_namespaces(
    catalog: Any,
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
            _list_namespaces(catalog, namespace),
            limit=max_namespaces - len(seen),
            kind="namespace",
            deadline_at=deadline_at,
            cancel_check=cancel_check,
        ):
            pending.append(_namespace_tuple(child))
    return namespaces


def _list_namespaces(catalog: Any, namespace: Namespace) -> Iterable[object]:
    return catalog.list_namespaces(namespace)


def _list_tables(catalog: Any, namespace: Namespace) -> Iterable[object]:
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
