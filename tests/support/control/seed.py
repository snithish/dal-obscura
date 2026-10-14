from __future__ import annotations

from typing import Any

from dal_obscura.control.runtime import ControlContext
from dal_obscura.storage import assets as _db_assets
from dal_obscura.storage import catalogs as _db_catalogs


def create_catalog(
    context: ControlContext,
    name: str,
    plugin_id: str,
    options: dict[str, Any],
) -> dict[str, str]:
    catalog_id = _db_catalogs.upsert_catalog(
        context.session, name=name, plugin_id=plugin_id, options=options
    )
    return {"id": str(catalog_id), "name": name}


def create_asset(
    context: ControlContext,
    catalog: str,
    target: str,
    backend: str,
    table_identifier: str | None,
    options: dict[str, Any],
) -> dict[str, str]:
    asset_id = _db_assets.upsert_asset(
        context.session,
        catalog=catalog,
        target=target,
        backend=backend,
        table_identifier=table_identifier,
        options=options,
    )
    return {"id": str(asset_id), "catalog": catalog, "target": target}
