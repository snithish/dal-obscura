"""Scoped, redacted control-plane audit queries."""

from __future__ import annotations

from uuid import UUID

from dal_obscura.control_plane.application.access import ControlPlaneActor
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
        asset_ids = {str(asset_id)}
    elif actor.platform_admin:
        asset_ids = None
    else:
        asset_ids = {
            str(asset["id"])
            for asset in store.list_workspace_assets_for_principals(
                context,
                actor.owner_principals(),
            )
        }
    return store.list_audit_events(context, asset_ids=asset_ids, limit=limit)
