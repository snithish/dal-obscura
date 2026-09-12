"""Workspace summary routes.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import PublicationActivationRequest


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

    @api.get("/v1/workspace/observations")
    def get_workspace_observations(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.get_workspace_observations(actor))

    @api.get("/v1/workspace/publications")
    def list_workspace_publications(
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> object:
        del actor
        return deps.with_service(lambda service: service.list_workspace_publications())

    @api.post("/v1/workspace/publications")
    def create_workspace_publication(
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> object:
        del actor
        return deps.with_service(lambda service: service.create_workspace_publication())

    @api.post("/v1/workspace/publications/{publication_id}/activate")
    def activate_workspace_publication(
        publication_id: UUID,
        request: PublicationActivationRequest | None = None,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> object:
        del actor
        return deps.with_service(
            lambda service: service.activate_workspace_publication(
                publication_id,
                expected_publication_id=(
                    None if request is None else request.expected_publication_id
                ),
            )
        )

    return api
