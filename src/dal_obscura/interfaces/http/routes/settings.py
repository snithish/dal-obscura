"""Runtime and authentication settings routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from dal_obscura.interfaces.http.request_context import current_request_id
from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.interfaces.http.routes.schemas import (
    AttributeMappingPreviewRequest,
    AttributeMappingPreviewResponse,
    AuthProviderResponse,
    AuthProviderRevisionResponse,
    AuthProvidersRequest,
    RuntimeSettingsRequest,
    RuntimeSettingsResponse,
)
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds workspace settings routes."""

    api = APIRouter()
    admin_actor = Depends(deps.require_admin)

    @api.get(
        "/v1/settings/runtime",
        response_model=RuntimeSettingsResponse | None,
        dependencies=[Depends(deps.require_admin)],
    )
    def get_workspace_runtime_settings(
        actor: ControlPlaneActor = admin_actor,
    ) -> RuntimeSettingsResponse | None:
        result = deps.workspace_commands.get_workspace_runtime_settings(
            actor=actor, correlation_id=current_request_id()
        )
        return RuntimeSettingsResponse.model_validate(result) if result is not None else None

    @api.get(
        "/v1/settings/auth-providers",
        response_model=list[AuthProviderResponse],
        dependencies=[Depends(deps.require_admin)],
    )
    def list_workspace_auth_providers(
        actor: ControlPlaneActor = admin_actor,
    ) -> list[AuthProviderResponse]:
        return [
            AuthProviderResponse.model_validate(item)
            for item in deps.workspace_commands.list_workspace_auth_providers(
                actor=actor, correlation_id=current_request_id()
            )
        ]

    @api.get(
        "/v1/settings/auth-providers/revision",
        response_model=AuthProviderRevisionResponse,
        dependencies=[Depends(deps.require_admin)],
    )
    def workspace_auth_provider_revision(
        actor: ControlPlaneActor = admin_actor,
    ) -> AuthProviderRevisionResponse:
        return AuthProviderRevisionResponse(
            revision=deps.workspace_commands.workspace_auth_provider_revision(
                actor=actor, correlation_id=current_request_id()
            )
        )

    @api.put(
        "/v1/settings/runtime",
        response_model=RuntimeSettingsResponse,
        dependencies=[Depends(deps.require_admin)],
    )
    def upsert_workspace_runtime_settings(
        request: RuntimeSettingsRequest,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> RuntimeSettingsResponse:
        return RuntimeSettingsResponse.model_validate(
            deps.workspace_commands.upsert_workspace_runtime_settings(
                ttl=request.ticket_ttl_seconds,
                max_tickets=request.max_tickets,
                max_ticket_exchanges=request.max_ticket_exchanges,
                path_rules=request.path_rules,
                expected_revision=request.expected_revision,
                actor=actor,
                correlation_id=current_request_id(),
            )
        )

    @api.put(
        "/v1/settings/auth-providers",
        response_model=list[AuthProviderResponse],
        dependencies=[Depends(deps.require_admin)],
    )
    def replace_workspace_auth_providers(
        request: AuthProvidersRequest,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> list[AuthProviderResponse]:
        return [
            AuthProviderResponse.model_validate(item)
            for item in deps.workspace_commands.replace_workspace_auth_providers(
                request.providers,
                expected_revision=request.expected_revision,
                actor=actor,
                correlation_id=current_request_id(),
            )
        ]

    @api.post(
        "/v1/settings/auth-providers/{ordinal}/attribute-preview",
        response_model=AttributeMappingPreviewResponse,
        dependencies=[Depends(deps.require_admin)],
    )
    def preview_attribute_mapping(
        ordinal: int,
        request: AttributeMappingPreviewRequest,
        actor: ControlPlaneActor = admin_actor,
    ) -> dict[str, object]:
        return deps.workspace_commands.preview_attribute_mapping(
            ordinal,
            request.claims,
            request.provider_args,
            actor=actor,
            correlation_id=current_request_id(),
        )

    return api
