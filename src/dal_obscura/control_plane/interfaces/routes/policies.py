"""Policy rule, preview, and policy-version routes.

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
from dal_obscura.control_plane.interfaces.routes.schemas import (
    PolicyPreviewRequest,
    PolicyRulesRequest,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds asset policy routes.

    Example:
        ```python
        policy_router = router(deps)
        ```
    """

    api = APIRouter()

    @api.get("/v1/assets/{asset_id}/policy-rules")
    def list_policy_rules(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.list_policy_rules(asset_id, actor=actor))

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
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.preview_asset_policy(
                asset_id=asset_id,
                principal=request.principal,
                groups=request.groups,
                claims=request.claims,
                actor=actor,
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

    @api.get("/v1/policy-versions")
    def list_policy_version_history(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.list_policy_version_history(actor=actor))

    return api
