"""Workspace summary routes.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    PublicationActivationRequest,
    PublicationActivationResponse,
    WorkspaceObservationsResponse,
    WorkspacePublicationCreateResponse,
    WorkspacePublicationResponse,
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

    @api.get(
        "/v1/workspace/publications",
        response_model=list[WorkspacePublicationResponse],
    )
    def list_workspace_publications(
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> list[WorkspacePublicationResponse]:
        del actor
        return deps.with_service(lambda service: service.list_workspace_publications())

    @api.post("/v1/workspace/publications", response_model=WorkspacePublicationCreateResponse)
    def create_workspace_publication(
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> WorkspacePublicationCreateResponse:
        return deps.with_service(lambda service: service.create_workspace_publication(actor))

    @api.post(
        "/v1/workspace/publications/{publication_id}/activate",
        response_model=PublicationActivationResponse,
    )
    def activate_workspace_publication(
        publication_id: UUID,
        request: PublicationActivationRequest | None = None,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> PublicationActivationResponse:
        if request is None or "expected_publication_id" not in request.model_fields_set:
            raise HTTPException(
                status_code=428,
                detail="Active publication precondition is required; reread before activating.",
            )
        return deps.with_service(
            lambda service: service.activate_workspace_publication(
                publication_id,
                expected_publication_id=(
                    None if request is None else request.expected_publication_id
                ),
                actor=actor,
            )
        )

    return api
