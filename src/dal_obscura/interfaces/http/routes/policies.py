"""Direct live policy authoring and evaluation routes."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from dal_obscura.interfaces.http.request_context import current_request_id
from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.interfaces.http.routes.policy_schemas import (
    PolicyEvaluationRequest,
    PolicyEvaluationResponse,
    PolicyMutationResponse,
    PolicyRulesRequest,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Build policy routes over canonical live configuration."""

    api = APIRouter()

    @api.put("/v1/assets/{asset_id}/policy", response_model=PolicyMutationResponse)
    def replace_asset_policy(
        asset_id: UUID,
        request: PolicyRulesRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> PolicyMutationResponse:
        result = deps.policies_commands.replace_asset_policy(
            asset_id,
            [rule.model_dump(exclude_unset=True) for rule in request.rules],
            expected_revision=request.expected_revision,
            revoke_existing_tokens=request.revoke_existing_tokens,
            actor=actor,
            correlation_id=current_request_id(),
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
        payload = deps.policies_commands.evaluate_asset_policy(
            asset_id,
            principal=request.principal,
            groups=request.groups,
            claims=request.claims,
            rows=request.rows,
            provider_ordinal=request.provider_ordinal,
            actor=actor,
            correlation_id=current_request_id(),
        )
        return PolicyEvaluationResponse.model_validate(payload)

    return api
