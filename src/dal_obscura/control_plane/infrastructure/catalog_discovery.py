from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Iterator
from threading import BoundedSemaphore
from time import monotonic
from typing import Any

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
        return tuple(part for part in namespace.split(".") if part)
    if isinstance(namespace, tuple):
        return tuple(str(part) for part in namespace)
    if isinstance(namespace, list):
        return tuple(str(part) for part in namespace)
    return (str(namespace),)


def _identifier_to_name(identifier: object) -> str:
    if isinstance(identifier, str):
        return identifier
    if isinstance(identifier, tuple):
        return ".".join(str(part) for part in identifier)
    if isinstance(identifier, list):
        return ".".join(str(part) for part in identifier)
    return str(identifier)


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
