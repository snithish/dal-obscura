"""Direct live policy authoring and evaluation routes."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    PolicyEvaluationRequest,
    PolicyEvaluationResponse,
    PolicyMutationResponse,
    PolicyRulesRequest,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Build policy routes over canonical live configuration."""

    api = APIRouter()

    @api.put("/v1/assets/{asset_id}/policy", response_model=PolicyMutationResponse)
    def replace_asset_policy(
        asset_id: UUID,
        request: PolicyRulesRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> PolicyMutationResponse:
        result = deps.with_service(
            lambda service: service.replace_policy_rules(
                asset_id,
                [rule.model_dump(exclude_unset=True) for rule in request.rules],
                actor=actor,
                expected_revision=request.expected_revision,
                revoke_existing_tokens=request.revoke_existing_tokens,
            )
        )
        return PolicyMutationResponse(asset_id=str(asset_id), **result)

    @api.post(
        "/v1/assets/{asset_id}/policy-evaluate",
        response_model=PolicyEvaluationResponse,
    )
    def evaluate_asset_policy(
        asset_id: UUID,
        request: PolicyEvaluationRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> PolicyEvaluationResponse:
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

    return api
