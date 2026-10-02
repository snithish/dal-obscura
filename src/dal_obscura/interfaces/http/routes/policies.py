"""Direct live policy authoring and evaluation routes."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

import dal_obscura.control.identity_attributes as _commands_identity_attributes
from dal_obscura.control import evaluation_service, policy_service
from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.interfaces.http.routes.schemas import (
    AttributeProviderResponse,
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
        result = deps.with_transaction(
            lambda service: policy_service.replace_policy_rules(
                service.session,
                asset_id,
                [rule.model_dump(exclude_unset=True) for rule in request.rules],
                actor=actor,
                expected_revision=request.expected_revision,
                revoke_existing_tokens=request.revoke_existing_tokens,
            )
        )
        return PolicyMutationResponse(asset_id=str(asset_id), **result)

    @api.get(
        "/v1/assets/{asset_id}/identity-attributes", response_model=list[AttributeProviderResponse]
    )
    def list_identity_attributes(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> list[dict[str, object]]:
        return deps.with_transaction(
            lambda service: _commands_identity_attributes.list_attribute_catalog(
                service.session, asset_id, actor
            )
        )

    @api.post(
        "/v1/assets/{asset_id}/policy-evaluate",
        response_model=PolicyEvaluationResponse,
    )
    def evaluate_asset_policy(
        asset_id: UUID,
        request: PolicyEvaluationRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> PolicyEvaluationResponse:
        return deps.with_transaction(
            lambda service: evaluation_service.evaluate_asset_policy(
                service.session,
                asset_id,
                actor,
                principal=request.principal,
                groups=request.groups,
                claims=request.claims,
                rows=request.rows,
                provider_ordinal=request.provider_ordinal,
                egress_allowlist=service.catalog_egress_allowlist,
                plugin_registry=service.plugin_registry,
                secret_provider=service.secret_provider,
            )
        )

    return api
