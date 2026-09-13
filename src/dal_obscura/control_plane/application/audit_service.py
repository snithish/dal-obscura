"""Scoped, redacted control-plane audit queries."""

from __future__ import annotations

from uuid import UUID

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.application.policy_service import ensure_asset_capability
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def list_audit_events(
    store: PublicationStore,
    *,
    actor: ControlPlaneActor,
    asset_id: UUID | None = None,
    limit: int = 100,
) -> list[dict[str, object]]:
    """Lists only audit events within the actor's visible asset scope."""

    context = store.get_default_workspace_context()
    if context is None:
        return []
    if asset_id is not None:
        ensure_asset_capability(store, asset_id, actor, "read")
    principals = None if actor.platform_admin else actor.owner_principals()
    return store.list_audit_events_page(
        context,
        asset_id=asset_id,
        principals=principals,
        limit=limit,
    ).items


def list_audit_events_page(
    store: PublicationStore,
    *,
    actor: ControlPlaneActor,
    asset_id: UUID | None = None,
    limit: int = 100,
    cursor: str | None = None,
) -> dict[str, object]:
    """Returns bounded audit events with database-enforced scope and a keyset cursor."""

    context = store.get_default_workspace_context()
    if context is None:
        return {"items": [], "next_cursor": None}
    if asset_id is not None:
        ensure_asset_capability(store, asset_id, actor, "read")
    principals = None if actor.platform_admin else actor.owner_principals()
    try:
        page = store.list_audit_events_page(
            context,
            asset_id=asset_id,
            principals=principals,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise ValidationFailure(str(exc)) from exc
    return {"items": page.items, "next_cursor": page.next_cursor}
