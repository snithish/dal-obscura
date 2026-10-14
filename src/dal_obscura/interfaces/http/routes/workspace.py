"""Workspace summary routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.control.workspace import service as workspace_service
from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.interfaces.http.routes.schemas import (
    WorkspaceObservationsResponse,
    WorkspaceSummaryResponse,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds workspace summary routes."""

    api = APIRouter()

    @api.get("/v1/workspace/summary", response_model=WorkspaceSummaryResponse)
    def get_workspace_summary(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> WorkspaceSummaryResponse:
        return deps.with_transaction(
            lambda service: workspace_service.get_workspace_summary(service.session, actor)
        )

    @api.get("/v1/workspace/observations", response_model=WorkspaceObservationsResponse)
    def get_workspace_observations(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> WorkspaceObservationsResponse:
        return deps.with_transaction(
            lambda service: workspace_service.get_workspace_observations(service.session, actor)
        )

    return api
