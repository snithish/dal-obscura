"""Workspace summary routes."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from dal_obscura.interfaces.http.request_context import current_request_id
from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.interfaces.http.routes.settings_schemas import AttributeProviderResponse
from dal_obscura.interfaces.http.routes.workspace_schemas import (
    WorkspaceObservationsResponse,
    WorkspaceSummaryResponse,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds workspace summary routes."""

    api = APIRouter()

    @api.get("/v1/workspace/summary", response_model=WorkspaceSummaryResponse)
    def get_workspace_summary(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> WorkspaceSummaryResponse:
        return WorkspaceSummaryResponse.model_validate(
            deps.workspace_commands.get_workspace_summary(
                actor=actor, correlation_id=current_request_id()
            )
        )

    @api.get("/v1/workspace/observations", response_model=WorkspaceObservationsResponse)
    def get_workspace_observations(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> WorkspaceObservationsResponse:
        return WorkspaceObservationsResponse.model_validate(
            deps.workspace_commands.get_workspace_observations(
                actor=actor, correlation_id=current_request_id()
            )
        )

    @api.get(
        "/v1/assets/{asset_id}/identity-attributes", response_model=list[AttributeProviderResponse]
    )
    def list_identity_attributes(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> list[dict[str, object]]:
        return deps.workspace_commands.list_identity_attributes(
            asset_id, actor=actor, correlation_id=current_request_id()
        )

    return api
