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


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds workspace summary routes.

    Example:
        ```python
        workspace_router = router(deps)
        ```
    """

    api = APIRouter()

    @api.get("/v1/workspace/summary")
    def get_workspace_summary(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.get_workspace_summary(actor))

    return api
