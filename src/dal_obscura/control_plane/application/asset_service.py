from __future__ import annotations

from typing import Any
from uuid import UUID

from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def list_workspace_assets(store: PublicationStore) -> list[dict[str, object]]:
    context = store.get_default_workspace_context()
    if context is None:
        return []
    return store.list_workspace_assets(context)


def get_workspace_asset(store: PublicationStore, asset_id: UUID) -> dict[str, object]:
    return store.get_workspace_asset(asset_id)


def upsert_workspace_asset(
    store: PublicationStore,
    catalog: str,
    target: str,
    backend: str,
    table_identifier: str | None,
    options: dict[str, Any],
) -> dict[str, str]:
    context = _required_workspace_context(store)
    asset_id = store.upsert_asset(
        cell_id=context.cell_id,
        tenant_id=context.tenant_id,
        catalog=catalog,
        target=target,
        backend=backend,
        table_identifier=table_identifier,
        options=options,
    )
    return {"id": str(asset_id), "catalog": catalog, "target": target}


def replace_asset_owners(
    store: PublicationStore,
    asset_id: UUID,
    owners: list[str],
) -> list[str]:
    return store.replace_asset_owners(asset_id=asset_id, owners=owners)


def replace_asset_schema_fields(
    store: PublicationStore,
    asset_id: UUID,
    fields: list[dict[str, Any]],
) -> list[dict[str, object]]:
    return store.replace_asset_schema_fields(asset_id=asset_id, fields=fields)


def _required_workspace_context(store: PublicationStore):
    context = store.get_default_workspace_context()
    if context is None:
        raise LookupError("No workspace has been configured")
    return context
