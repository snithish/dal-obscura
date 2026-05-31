from __future__ import annotations

from collections.abc import Callable
from typing import Any

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)

CatalogTable = dict[str, object]
LoadCatalogFn = Callable[..., Any]
Namespace = tuple[str, ...]


def discover_catalog_tables(
    catalog_name: str,
    module: str,
    options: dict[str, Any],
) -> list[CatalogTable]:
    if module != ICEBERG_CATALOG_MODULE:
        raise ValueError("Catalog discovery currently supports Iceberg catalogs.")
    return discover_iceberg_tables(catalog_name, options)


def discover_iceberg_tables(
    catalog_name: str,
    options: dict[str, Any],
    *,
    load_catalog_fn: LoadCatalogFn | None = None,
) -> list[CatalogTable]:
    loader = load_catalog_fn or _load_catalog
    catalog = loader(catalog_name, **options)
    table_names = sorted(
        {
            _identifier_to_name(identifier)
            for namespace in _walk_namespaces(catalog)
            for identifier in _list_tables(catalog, namespace)
        }
    )
    return [
        {
            "backend": "iceberg",
            "name": table_name,
            "table_identifier": table_name,
        }
        for table_name in table_names
    ]


def _walk_namespaces(catalog: Any) -> list[Namespace]:
    namespaces: list[Namespace] = []
    pending: list[Namespace] = [()]
    seen: set[Namespace] = set()
    while pending:
        namespace = pending.pop(0)
        if namespace in seen:
            continue
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
