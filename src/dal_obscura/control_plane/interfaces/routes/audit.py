"""Redacted workspace audit routes."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import AuditEventPageResponse


def router(deps: ControlPlaneDeps) -> APIRouter:
    api = APIRouter()

    @api.get("/v1/audit/events/page", response_model=AuditEventPageResponse)
    def list_audit_events_page(
        asset_id: UUID | None = None,
        actor_filter: str | None = Query(default=None, alias="actor", max_length=200),
        action: str | None = Query(default=None, max_length=200),
        resource_type: str | None = Query(default=None, max_length=48),
        outcome: str | None = Query(default=None, max_length=24),
        correlation_id: str | None = Query(default=None, max_length=96),
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = Query(default=100, ge=1, le=200),
        cursor: str | None = Query(default=None, max_length=512),
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AuditEventPageResponse:
        return deps.with_service(
            lambda service: service.list_audit_events_page(
                actor=actor,
                asset_id=asset_id,
                actor_filter=actor_filter,
                action=action,
                resource_type=resource_type,
                outcome=outcome,
                correlation_id=correlation_id,
                created_after=created_after,
                created_before=created_before,
                limit=limit,
                cursor=cursor,
            )
        )

    return api
