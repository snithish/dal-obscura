from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    ServiceConfig,
)
from tests.support.iceberg import iceberg_sql_catalog_options

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)


def iceberg_catalog_config(
    tmp_path: Path,
    name: str,
    warehouse_name: str,
) -> CatalogConfig:
    return CatalogConfig(
        name=name,
        module=ICEBERG_CATALOG_MODULE,
        options=iceberg_sql_catalog_options(tmp_path, name, warehouse_name),
    )


def service_config_from_raw(raw_service_config: dict[str, object]) -> ServiceConfig:
    catalogs_raw = raw_service_config.get("catalogs", {})
    if not isinstance(catalogs_raw, dict):
        raise TypeError("catalogs must be an object")

    catalogs: dict[str, CatalogConfig] = {}
    for name, raw in catalogs_raw.items():
        if not isinstance(name, str) or not isinstance(raw, dict):
            continue
        raw_config = cast(dict[str, Any], raw)
        catalogs[name] = CatalogConfig(
            name=name,
            module=str(raw_config["module"]),
            options=_dict(raw_config.get("options")),
        )
    return ServiceConfig(catalogs=catalogs)


def _dict(raw: object) -> dict[str, object]:
    return dict(cast(dict[str, object], raw)) if isinstance(raw, dict) else {}
