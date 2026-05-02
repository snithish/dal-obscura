from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogTargetConfig,
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
    *,
    targets: dict[str, CatalogTargetConfig] | None = None,
) -> CatalogConfig:
    return CatalogConfig(
        name=name,
        module=ICEBERG_CATALOG_MODULE,
        options=iceberg_sql_catalog_options(tmp_path, name, warehouse_name),
        targets=targets or {},
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
            targets=_targets(raw_config.get("targets")),
        )
    return ServiceConfig(catalogs=catalogs, paths=())


def _targets(raw: object) -> dict[str, CatalogTargetConfig]:
    if not isinstance(raw, dict):
        return {}
    targets: dict[str, CatalogTargetConfig] = {}
    for target_name, target_raw in raw.items():
        if isinstance(target_name, str) and isinstance(target_raw, dict):
            target_config = cast(dict[str, object], target_raw)
            targets[target_name] = CatalogTargetConfig(
                backend=None
                if target_config.get("backend") is None
                else str(target_config.get("backend")),
                table=None
                if target_config.get("table") is None
                else str(target_config.get("table")),
            )
    return targets


def _dict(raw: object) -> dict[str, object]:
    return dict(cast(dict[str, object], raw)) if isinstance(raw, dict) else {}
