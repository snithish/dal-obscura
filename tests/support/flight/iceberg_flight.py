from __future__ import annotations

from typing import Any, cast

from dal_obscura.adapters.providers.catalogs import (
    CatalogConfig,
    ServiceConfig,
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
            options=_dict(raw_config.get("options")),
        )
    return ServiceConfig(catalogs=catalogs)


def _dict(raw: object) -> dict[str, object]:
    return dict(cast(dict[str, object], raw)) if isinstance(raw, dict) else {}
