"""Scoped, redacted control-plane audit queries."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from dal_obscura.application.governance.assets.access import ensure_asset_capability
from dal_obscura.application.governance.errors import ValidationFailure
from dal_obscura.application.governance.ports import (
    GovernanceUnitOfWorkFactory,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor


class AuditQueries:
    def __init__(self, work_factory: GovernanceUnitOfWorkFactory) -> None:
        self._work_factory = work_factory

    def list_audit_events_page(
        self,
        *,
        actor: ControlPlaneActor,
        correlation_id: str | None,
        asset_id: UUID | None = None,
        actor_filter: str | None = None,
        action: str | None = None,
        resource_type: str | None = None,
        outcome: str | None = None,
        correlation_filter: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, object]:
        with self._work_factory() as work:
            context = work.workspace.get_workspace()
            if context is None:
                return {"items": [], "next_cursor": None}
            if asset_id is not None:
                ensure_asset_capability(work.assets, asset_id, actor, "read")
            principals = None if actor.platform_admin else actor.owner_principals()
            try:
                page = work.audit.list_audit_events_page(
                    asset_id=asset_id,
                    principals=principals,
                    actor=actor_filter,
                    action=action,
                    resource_type=resource_type,
                    outcome=outcome,
                    correlation_id=correlation_filter,
                    created_after=created_after,
                    created_before=created_before,
                    limit=limit,
                    cursor=cursor,
                )
            except ValueError as exc:
                raise ValidationFailure(str(exc)) from exc
            return {"items": page.items, "next_cursor": page.next_cursor}
