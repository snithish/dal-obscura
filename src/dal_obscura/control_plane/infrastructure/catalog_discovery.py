from __future__ import annotations

from collections import deque
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
    for namespace in _walk_namespaces(catalog, max_namespaces=max_namespaces):
        for identifier in _list_tables(catalog, namespace):
            table_names.add(_identifier_to_name(identifier))
            if len(table_names) > max_tables:
                raise ValueError(f"Catalog discovery exceeded the {max_tables}-table limit")
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
) -> list[Namespace]:
    namespaces: list[Namespace] = []
    pending = deque([()])
    seen: set[Namespace] = set()
    while pending:
        namespace = pending.popleft()
        if namespace in seen:
            continue
        if len(seen) >= max_namespaces:
            raise ValueError(f"Catalog discovery exceeded the {max_namespaces}-namespace limit")
        seen.add(namespace)
        namespaces.append(namespace)
        for child in _list_namespaces(catalog, namespace):
            pending.append(_namespace_tuple(child))
    return namespaces


def _list_namespaces(catalog: Any, namespace: Namespace) -> list[object]:
    try:
        if namespace:
            return list(catalog.list_namespaces(namespace))
        return list(catalog.list_namespaces())
    except TypeError:
        return list(catalog.list_namespaces(namespace))


def _list_tables(catalog: Any, namespace: Namespace) -> list[object]:
    try:
        return list(catalog.list_tables(namespace))
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


def _load_catalog(catalog_name: str, **options: Any) -> Any:
    from pyiceberg.catalog import load_catalog

    return load_catalog(catalog_name, **options)
