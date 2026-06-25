from __future__ import annotations

from typing import Any, cast

from dal_obscura.control_plane.infrastructure.catalog_discovery import discover_catalog_tables
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore

CatalogDiscoverer = Any


def list_workspace_catalogs(store: PublicationStore) -> list[dict[str, object]]:
    context = store.get_default_workspace_context()
    if context is None:
        return []
    return store.list_workspace_catalogs(context)


def discover_workspace_catalog_tables(
    store: PublicationStore,
    name: str,
    *,
    discover: CatalogDiscoverer = discover_catalog_tables,
) -> dict[str, object]:
    context = _required_workspace_context(store)
    catalog = store.get_workspace_catalog(context, name)
    catalog_options = cast(dict[str, Any], catalog["options"])
    tables = discover(
        str(catalog["name"]),
        str(catalog["module"]),
        catalog_options,
    )
    governed_targets = {
        value
        for asset in store.list_workspace_assets(context)
        if asset["catalog"] == catalog["name"]
        for value in (asset["name"], asset["table_identifier"])
        if isinstance(value, str) and value
    }
    return {
        "catalog": catalog["name"],
        "tables": [
            {
                **table,
                "target": table["name"],
                "governed": table["name"] in governed_targets
                or table["table_identifier"] in governed_targets,
            }
            for table in tables
        ],
    }


def upsert_workspace_catalog(
    store: PublicationStore,
    name: str,
    module: str,
    options: dict[str, Any],
) -> dict[str, str]:
    context = store.ensure_default_workspace_context()
    catalog_id = store.upsert_catalog(
        cell_id=context.cell_id,
        tenant_id=context.tenant_id,
        name=name,
        module=module,
        options=options,
    )
    return {"id": str(catalog_id), "name": name}


def _required_workspace_context(store: PublicationStore):
    context = store.get_default_workspace_context()
    if context is None:
        raise LookupError("No workspace has been configured")
    return context
