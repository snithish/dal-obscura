"""Scoped, redacted control-plane audit queries."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy.orm import Session

from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.control.errors import ValidationFailure
from dal_obscura.control.policy_service import ensure_asset_capability
from dal_obscura.storage import audit as _db_audit
from dal_obscura.storage import workspace as _db_workspace


def list_audit_events_page(
    store: Session,
    *,
    actor: ControlPlaneActor,
    asset_id: UUID | None = None,
    actor_filter: str | None = None,
    action: str | None = None,
    resource_type: str | None = None,
    outcome: str | None = None,
    correlation_id: str | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    limit: int = 100,
    cursor: str | None = None,
) -> dict[str, object]:
    """Returns bounded audit events with database-enforced scope and a keyset cursor."""

    context = _db_workspace.get_workspace(store)
    if context is None:
        return {"items": [], "next_cursor": None}
    if asset_id is not None:
        ensure_asset_capability(store, asset_id, actor, "read")
    principals = None if actor.platform_admin else actor.owner_principals()
    try:
        page = _db_audit.list_audit_events_page(
            store,
            asset_id=asset_id,
            principals=principals,
            actor=actor_filter,
            action=action,
            resource_type=resource_type,
            outcome=outcome,
            correlation_id=correlation_id,
            created_after=created_after,
            created_before=created_before,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise ValidationFailure(str(exc)) from exc
    return {"items": page.items, "next_cursor": page.next_cursor}
