from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    PolicyPreviewRequest,
    PolicyRulesRequest,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    api = APIRouter()

    @api.get("/v1/assets/{asset_id}/policy-rules", dependencies=[Depends(deps.require_actor)])
    def list_policy_rules(asset_id: UUID) -> object:
        return deps.with_service(lambda service: service.list_policy_rules(asset_id))

    @api.put("/v1/assets/{asset_id}/policy-rules")
    def replace_policy_rules(
        asset_id: UUID,
        request: PolicyRulesRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.replace_policy_rules(
                asset_id=asset_id,
                rules=request.rules,
                actor=actor,
            )
        ) or {"asset_id": str(asset_id)}

    @api.post("/v1/assets/{asset_id}/policy-preview")
    def preview_asset_policy(
        asset_id: UUID,
        request: PolicyPreviewRequest,
        _actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.preview_asset_policy(
                asset_id=asset_id,
                principal=request.principal,
                groups=request.groups,
                claims=request.claims,
            )
        )

    @api.post("/v1/assets/{asset_id}/policy-versions")
    def create_asset_policy_version(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.create_asset_policy_version(
                asset_id=asset_id,
                actor=actor,
            )
        )

    @api.get("/v1/publications/draft", dependencies=[Depends(deps.require_actor)])
    def get_workspace_publication_draft() -> object:
        return deps.with_service(lambda service: service.get_workspace_draft())

    @api.get("/v1/publications", dependencies=[Depends(deps.require_actor)])
    def list_workspace_publications() -> object:
        return deps.with_service(lambda service: service.list_workspace_publications())

    @api.post("/v1/publications", dependencies=[Depends(deps.require_admin)])
    def create_workspace_publication() -> object:
        return deps.with_service(lambda service: service.create_workspace_publication())

    @api.post(
        "/v1/publications/{publication_id}/activate",
        dependencies=[Depends(deps.require_admin)],
    )
    def activate_workspace_publication(publication_id: UUID) -> object:
        return deps.with_service(
            lambda service: service.activate_workspace_publication(publication_id)
        )

    return api
