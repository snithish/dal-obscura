from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
STATIC_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.StaticCatalog"
)
UNITY_CATALOG_MODULE = "dal_obscura.data_plane.infrastructure.adapters.unity_catalog.UnityCatalog"

CatalogTable = dict[str, object]
LoadCatalogFn = Callable[..., Any]
Namespace = tuple[str, ...]


def discover_catalog_tables(
    catalog_name: str,
    module: str,
    options: dict[str, Any],
) -> list[CatalogTable]:
    if module == ICEBERG_CATALOG_MODULE:
        return discover_iceberg_tables(catalog_name, options)
    if module == STATIC_CATALOG_MODULE:
        return discover_static_catalog_tables(options)
    if module == UNITY_CATALOG_MODULE:
        return discover_unity_catalog_tables(options)
    raise ValueError(
        "Catalog discovery currently supports Iceberg, static, and Unity Catalog catalogs."
    )


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


def discover_unity_catalog_tables(options: dict[str, Any]) -> list[CatalogTable]:
    base_url = _normalize_unity_base_url(str(options["base_url"]))
    token = _optional_str(options.get("token"))
    headers = {"authorization": f"Bearer {token}"} if token else {}
    params: dict[str, str] = {}
    uc_catalog = _optional_str(options.get("uc_catalog"))
    if uc_catalog is not None:
        params["catalog_name"] = uc_catalog
    with httpx.Client(timeout=float(options.get("timeout_seconds", 10.0))) as client:
        response = client.get(f"{base_url}/tables", headers=headers, params=params)
        response.raise_for_status()
        payload = response.json()
    tables = payload.get("tables", []) if isinstance(payload, dict) else []
    discovered = [
        _unity_table_to_catalog_table(raw, uc_catalog=uc_catalog)
        for raw in tables
        if isinstance(raw, dict)
    ]
    return sorted(discovered, key=lambda item: str(item["name"]))


def discover_static_catalog_tables(options: dict[str, Any]) -> list[CatalogTable]:
    targets = options.get("targets", {})
    if isinstance(targets, dict):
        iterable = targets.items()
    elif isinstance(targets, list):
        iterable = (
            (str(item.get("name") or item.get("target") or ""), item)
            for item in targets
            if isinstance(item, dict)
        )
    else:
        iterable = ()

    rows: list[CatalogTable] = []
    for name, raw in iterable:
        if not isinstance(raw, dict):
            continue
        target_name = str(name).strip()
        backend = str(raw.get("backend") or raw.get("format") or "").strip().lower()
        table_identifier = str(
            raw.get("table") or raw.get("table_identifier") or target_name
        ).strip()
        if target_name and backend and table_identifier:
            rows.append(
                {
                    "backend": backend,
                    "name": target_name,
                    "table_identifier": table_identifier,
                }
            )
    return sorted(rows, key=lambda item: str(item["name"]))


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


def _unity_table_to_catalog_table(
    raw: dict[str, object],
    *,
    uc_catalog: str | None,
) -> CatalogTable:
    full_name = str(raw.get("full_name") or raw.get("name") or "")
    name = full_name
    if uc_catalog is not None and full_name.startswith(f"{uc_catalog}."):
        name = full_name[len(uc_catalog) + 1 :]
    backend = _unity_backend(str(raw.get("data_source_format") or ""))
    return {"backend": backend, "name": name, "table_identifier": name}


def _unity_backend(format_name: str) -> str:
    backend = format_name.strip().lower()
    if backend in {"delta", "parquet", "csv", "json", "orc", "avro", "text"}:
        return backend
    return backend or "unknown"


def _normalize_unity_base_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if normalized.endswith("/api/2.1/unity-catalog"):
        return normalized
    return f"{normalized}/api/2.1/unity-catalog"


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
