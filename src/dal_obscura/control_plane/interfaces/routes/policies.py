"""Policy rule, preview, and policy-version routes.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    PolicyDraftRequest,
    PolicyEvaluationRequest,
    PolicyPreviewRequest,
    PolicyRestoreRequest,
    PolicyRulesRequest,
    PolicyVersionPublishRequest,
)


def router(deps: ControlPlaneDeps) -> APIRouter:  # noqa: C901
    """Builds asset policy routes.

    Example:
        ```python
        policy_router = router(deps)
        ```
    """

    api = APIRouter()

    @api.get("/v1/assets/{asset_id}/draft")
    def get_policy_draft(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.get_policy_draft(asset_id, actor))

    @api.put("/v1/assets/{asset_id}/draft")
    def save_policy_draft(
        asset_id: UUID,
        request: PolicyDraftRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.save_policy_draft(
                asset_id,
                actor,
                expected_revision=request.expected_revision,
                rules=request.rules,
            )
        )

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
                requested_columns=request.columns or None,
            )
        )

    @api.post("/v1/assets/{asset_id}/policy-evaluate")
    def evaluate_asset_policy(
        asset_id: UUID,
        request: PolicyEvaluationRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.evaluate_asset_policy(
                asset_id,
                actor,
                principal=request.principal,
                groups=request.groups,
                claims=request.claims,
                rows=request.rows,
            )
        )

    @api.post("/v1/assets/{asset_id}/policy-review")
    def review_asset_policy(
        asset_id: UUID,
        request: PolicyEvaluationRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.review_asset_policy(
                asset_id,
                actor,
                principal=request.principal,
                groups=request.groups,
                claims=request.claims,
                rows=request.rows,
            )
        )

    @api.post("/v1/assets/{asset_id}/policy-versions")
    def create_asset_policy_version(
        asset_id: UUID,
        request: PolicyVersionPublishRequest | None = None,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.create_asset_policy_version(
                asset_id=asset_id,
                actor=actor,
                expected_draft_revision=(
                    None if request is None else request.expected_draft_revision
                ),
                expected_publication_id=(
                    None if request is None else request.expected_publication_id
                ),
                review_token=None if request is None else request.review_token,
            )
        )

    @api.get("/v1/assets/{asset_id}/policy-versions")
    def list_asset_policy_version_history(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.list_asset_policy_version_history(asset_id, actor=actor)
        )

    @api.get("/v1/assets/{asset_id}/policy-versions/{policy_version}")
    def get_asset_policy_version(
        asset_id: UUID,
        policy_version: int,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.get_asset_policy_version(
                asset_id,
                policy_version,
                actor=actor,
            )
        )

    @api.post("/v1/assets/{asset_id}/policy-versions/{policy_version}/restore")
    def restore_policy_version(
        asset_id: UUID,
        policy_version: int,
        request: PolicyRestoreRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.restore_policy_version(
                asset_id,
                policy_version,
                actor=actor,
                expected_revision=request.expected_revision,
            )
        )

    @api.get("/v1/policy-versions")
    def list_policy_version_history(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.list_policy_version_history(actor=actor))

    @api.get("/v1/audit/events")
    def list_audit_events(
        asset_id: UUID | None = None,
        limit: int = Query(default=100, ge=1, le=200),
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.list_audit_events(
                actor=actor,
                asset_id=asset_id,
                limit=limit,
            )
        )

    return api
