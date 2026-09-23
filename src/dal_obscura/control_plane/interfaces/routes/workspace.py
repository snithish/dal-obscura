"""Workspace summary routes.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    WorkspaceObservationsResponse,
    WorkspaceSummaryResponse,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds workspace summary routes.

    Example:
        ```python
        workspace_router = router(deps)
        ```
    """

    api = APIRouter()

    @api.get("/v1/workspace/summary", response_model=WorkspaceSummaryResponse)
    def get_workspace_summary(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> WorkspaceSummaryResponse:
        return deps.with_service(lambda service: service.get_workspace_summary(actor))

    @api.get("/v1/workspace/observations", response_model=WorkspaceObservationsResponse)
    def get_workspace_observations(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> WorkspaceObservationsResponse:
        return deps.with_service(lambda service: service.get_workspace_observations(actor))

    return api
