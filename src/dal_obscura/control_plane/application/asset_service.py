"""Workspace asset service functions.

Example:
    ```python
    assets = list_workspace_assets(store)
    ```
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control_plane.application.policy_service import ensure_asset_capability
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def list_workspace_assets(
    store: PublicationStore,
    actor: ControlPlaneActor | None = None,
) -> list[dict[str, object]]:
    """Lists governed assets in the default workspace.

    Example:
        ```python
        assets = list_workspace_assets(store)
        ```
    """

    context = store.get_default_workspace_context()
    if context is None:
        return []
    if actor is None or actor.platform_admin:
        return store.list_workspace_assets(context)
    return store.list_workspace_assets_for_principals(context, actor.owner_principals())


def list_workspace_assets_page(
    store: PublicationStore,
    actor: ControlPlaneActor,
    *,
    limit: int,
    cursor: str | None = None,
    search: str | None = None,
) -> dict[str, object]:
    """Returns a bounded, cursor-paginated asset inventory for one actor."""

    context = store.get_default_workspace_context()
    if context is None:
        return {"items": [], "next_cursor": None}
    principals = None if actor.platform_admin else actor.owner_principals()
    try:
        page = store.list_workspace_assets_page(
            context,
            limit=limit,
            cursor=cursor,
            search=search,
            principals=principals,
        )
    except ValueError as exc:
        raise ValidationFailure(str(exc)) from exc
    return {"items": page.items, "next_cursor": page.next_cursor}


def get_workspace_asset(
    store: PublicationStore,
    asset_id: UUID,
    actor: ControlPlaneActor | None = None,
) -> dict[str, object]:
    """Returns one governed asset by id.

    Example:
        ```python
        asset = get_workspace_asset(store, asset_id)
        ```
    """

    asset = store.get_workspace_asset(asset_id)
    if actor is not None:
        ensure_asset_capability(store, asset_id, actor, "read")
    return asset


def upsert_workspace_asset(
    store: PublicationStore,
    catalog: str,
    target: str,
    backend: str,
    table_identifier: str | None,
    options: dict[str, Any],
) -> dict[str, str]:
    """Creates or updates a governed asset binding.

    Example:
        ```python
        result = upsert_workspace_asset(store, "analytics", "orders", "iceberg", None, {})
        ```
    """

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
    actor: ControlPlaneActor | None = None,
) -> list[str]:
    """Replaces owners for one governed asset.

    Example:
        ```python
        owners = replace_asset_owners(store, asset_id, ["alice", "group:analytics"])
        ```
    """

    if actor is not None and not actor.platform_admin:
        raise AuthorizationFailure("Only platform admins may replace asset owners.")
    normalized = store.replace_asset_owners(asset_id=asset_id, owners=owners)
    if actor is not None:
        store.record_asset_audit_event(
            asset_id=asset_id,
            actor_principal=actor.principal,
            action="asset.owners.replace",
            details={"owner_count": len(normalized)},
        )
    return normalized


def list_asset_grants(store: PublicationStore, asset_id: UUID) -> list[dict[str, str]]:
    return store.list_asset_grants(asset_id)


def replace_asset_grants(
    store: PublicationStore,
    asset_id: UUID,
    grants: list[dict[str, str]],
    actor: ControlPlaneActor | None = None,
) -> list[dict[str, str]]:
    allowed = {"read", "edit", "publish", "grant"}
    if any(str(grant.get("capability")) not in allowed for grant in grants):
        raise ValidationFailure("Unsupported asset capability")
    normalized = store.replace_asset_grants(asset_id=asset_id, grants=grants)
    if actor is not None:
        store.record_asset_audit_event(
            asset_id=asset_id,
            actor_principal=actor.principal,
            action="asset.grants.replace",
            details={"grant_count": len(normalized)},
        )
    return normalized


def replace_asset_schema_fields(
    store: PublicationStore,
    asset_id: UUID,
    fields: list[dict[str, Any]],
) -> list[dict[str, object]]:
    """Replaces the schema-field metadata for one governed asset.

    Example:
        ```python
        fields = replace_asset_schema_fields(store, asset_id, [{"name": "id"}])
        ```
    """

    return store.replace_asset_schema_fields(asset_id=asset_id, fields=fields)


def _required_workspace_context(store: PublicationStore):
    context = store.get_default_workspace_context()
    if context is None:
        raise LookupError("No workspace has been configured")
    return context
