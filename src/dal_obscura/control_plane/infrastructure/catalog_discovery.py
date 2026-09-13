from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime, timedelta, timezone
from threading import BoundedSemaphore
from time import monotonic
from typing import Any, cast

from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import CatalogType

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)

CatalogTable = dict[str, object]
LoadCatalogFn = Any
Namespace = tuple[str, ...]
DEFAULT_MAX_NAMESPACES = 1_000
DEFAULT_MAX_TABLES = 10_000
DEFAULT_DEADLINE_SECONDS = 30.0
DEFAULT_MAX_ACTIVE_DISCOVERIES = 8
_DISCOVERY_SLOTS = BoundedSemaphore(DEFAULT_MAX_ACTIVE_DISCOVERIES)


def discover_catalog_tables(
    catalog_name: str,
    module: str,
    options: dict[str, Any],
) -> list[CatalogTable]:
    """Lists tables for a configured catalog.

    Example:
        ```python
        tables = discover_catalog_tables("analytics", "iceberg", {"uri": "sqlite:///catalog.db"})
        ```
    """

    # Keep the public discoverer bounded before materializing provider results.
    # The older registry path collected a complete listing and capped it only
    # afterwards, allowing an untrusted catalog to consume arbitrary memory.
    if module == ICEBERG_CATALOG_MODULE:
        return discover_iceberg_tables(
            catalog_name,
            dict(options),
            max_tables=DEFAULT_MAX_TABLES,
            max_namespaces=DEFAULT_MAX_NAMESPACES,
            deadline_at=monotonic() + DEFAULT_DEADLINE_SECONDS,
        )
    _catalog_type(module)
    raise ValueError(f"Unsupported catalog module: {module}")


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
        raise ValueError("An admitted plugin registry is required for external discovery")
    factory = plugin_registry.load("catalog", plugin_id)
    if not callable(factory):
        raise ValueError("Admitted catalog factory is invalid")
    from dal_obscura_plugin_api import CatalogConfig, ExecutionContext, TableIdentifier

    context = ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(seconds=DEFAULT_DEADLINE_SECONDS),
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
    try:
        deadline_at = monotonic() + DEFAULT_DEADLINE_SECONDS
        _validate_public_catalog_lifecycle(plugin, context, deadline_at)
        continuation: str | None = None
        seen: set[str] = set()
        tables: list[CatalogTable] = []
        for _ in range(DEFAULT_MAX_NAMESPACES):
            _check_budget(deadline_at, context.cancel_check)
            page = plugin.list_tables(context, continuation=continuation, limit=500)
            if not hasattr(page, "entries") or not hasattr(page, "continuation"):
                raise ValueError("Catalog plugin returned an invalid discovery page")
            for identifier in page.entries:
                if not isinstance(identifier, TableIdentifier):
                    raise ValueError("Catalog plugin returned an invalid table identifier")
                name = ".".join((*identifier.namespace, identifier.name))
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
        if callable(close):
            close()


def _validate_public_catalog_lifecycle(
    plugin: Any,
    context: Any,
    deadline_at: float,
) -> None:
    required = ("validate_config", "list_namespaces", "list_tables", "close")
    if not all(callable(getattr(plugin, name, None)) for name in required):
        raise ValueError("Catalog plugin does not implement the required lifecycle")
    _check_budget(deadline_at, context.cancel_check)
    plugin.validate_config(context)
    _check_budget(deadline_at, context.cancel_check)
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


def _catalog_type(module: str) -> CatalogType:
    if module == ICEBERG_CATALOG_MODULE:
        return "iceberg"
    raise ValueError(f"Unsupported catalog module: {module}")


def discover_iceberg_tables(
    catalog_name: str,
    options: dict[str, Any],
    *,
    load_catalog_fn: LoadCatalogFn | None = None,
    max_namespaces: int = DEFAULT_MAX_NAMESPACES,
    max_tables: int = DEFAULT_MAX_TABLES,
    deadline_at: float | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> list[CatalogTable]:
    """Admit one bounded discovery operation and release its slot on every exit."""

    if not _DISCOVERY_SLOTS.acquire(blocking=False):
        raise RuntimeError("Catalog discovery capacity is exhausted; retry later")
    try:
        return _discover_iceberg_tables(
            catalog_name,
            options,
            load_catalog_fn=load_catalog_fn,
            max_namespaces=max_namespaces,
            max_tables=max_tables,
            deadline_at=deadline_at,
            cancel_check=cancel_check,
        )
    finally:
        _DISCOVERY_SLOTS.release()


def _discover_iceberg_tables(
    catalog_name: str,
    options: dict[str, Any],
    *,
    load_catalog_fn: LoadCatalogFn | None = None,
    max_namespaces: int = DEFAULT_MAX_NAMESPACES,
    max_tables: int = DEFAULT_MAX_TABLES,
    deadline_at: float | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> list[CatalogTable]:
    """Lists every table reachable through a PyIceberg catalog.

    Example:
        ```python
        tables = discover_iceberg_tables("analytics", {"uri": "http://localhost:8181"})
        ```
    """

    if max_namespaces <= 0 or max_tables <= 0:
        raise ValueError("Catalog discovery limits must be positive")
    loader = load_catalog_fn or _load_catalog
    catalog = loader(catalog_name, **options)
    table_names: set[str] = set()
    for namespace in _walk_namespaces(
        catalog,
        max_namespaces=max_namespaces,
        deadline_at=deadline_at,
        cancel_check=cancel_check,
    ):
        remaining_tables = max_tables - len(table_names)
        for identifier in _bounded_provider_items(
            _list_tables(catalog, namespace),
            limit=remaining_tables,
            kind="table",
            deadline_at=deadline_at,
            cancel_check=cancel_check,
        ):
            table_names.add(_identifier_to_name(identifier))
    return [
        {
            "backend": "iceberg",
            "name": table_name,
            "table_identifier": table_name,
        }
        for table_name in sorted(table_names)
    ]


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
    try:
        if namespace:
            return catalog.list_namespaces(namespace)
        return catalog.list_namespaces()
    except TypeError:
        return catalog.list_namespaces(namespace)


def _list_tables(catalog: Any, namespace: Namespace) -> Iterable[object]:
    try:
        return catalog.list_tables(namespace)
    except Exception:
        if namespace:
            raise
        return []


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
    if isinstance(identifier, str):
        parts = tuple(identifier.split("."))
        if any(not _valid_identifier_segment(part) for part in parts):
            raise ValueError("Catalog provider returned an invalid table identifier")
        return identifier
    if isinstance(identifier, tuple):
        if any(not _valid_identifier_segment(part) for part in identifier):
            raise ValueError("Catalog provider returned an invalid table identifier")
        return ".".join(cast(tuple[str, ...], identifier))
    if isinstance(identifier, list):
        if any(not _valid_identifier_segment(part) for part in identifier):
            raise ValueError("Catalog provider returned an invalid table identifier")
        return ".".join(cast(list[str], identifier))
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


def _load_catalog(catalog_name: str, **options: Any) -> Any:
    from pyiceberg.catalog import load_catalog

    return load_catalog(catalog_name, **options)
