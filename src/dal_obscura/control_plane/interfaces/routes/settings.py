"""Runtime and authentication settings routes.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

from typing import cast

from fastapi import APIRouter, Depends

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    AuthProviderResponse,
    AuthProviderRevisionResponse,
    AuthProvidersRequest,
    RuntimeSettingsRequest,
    RuntimeSettingsResponse,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds workspace settings routes.

    Example:
        ```python
        settings_router = router(deps)
        ```
    """

    api = APIRouter()

    @api.get(
        "/v1/settings/runtime",
        response_model=RuntimeSettingsResponse | None,
        dependencies=[Depends(deps.require_admin)],
    )
    def get_workspace_runtime_settings() -> RuntimeSettingsResponse | None:
        return cast(
            RuntimeSettingsResponse | None,
            deps.with_service(lambda service: service.get_workspace_runtime_settings()),
        )

    @api.get(
        "/v1/settings/auth-providers",
        response_model=list[AuthProviderResponse],
        dependencies=[Depends(deps.require_admin)],
    )
    def list_workspace_auth_providers() -> list[AuthProviderResponse]:
        return cast(
            list[AuthProviderResponse],
            deps.with_service(lambda service: service.list_workspace_auth_providers()),
        )

    @api.get(
        "/v1/settings/auth-providers/revision",
        response_model=AuthProviderRevisionResponse,
        dependencies=[Depends(deps.require_admin)],
    )
    def workspace_auth_provider_revision() -> AuthProviderRevisionResponse:
        return cast(
            AuthProviderRevisionResponse,
            deps.with_service(
                lambda service: {"revision": service.workspace_auth_provider_revision()}
            ),
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
        result = deps.with_service(
            lambda service: service.upsert_workspace_runtime_settings(
                ttl=request.ticket_ttl_seconds,
                max_tickets=request.max_tickets,
                max_ticket_exchanges=request.max_ticket_exchanges,
                path_rules=request.path_rules,
                expected_revision=request.expected_revision,
                actor=actor,
            )
        )
        return cast(
            RuntimeSettingsResponse,
            result
            or {
                "ticket_ttl_seconds": request.ticket_ttl_seconds,
                "max_tickets": request.max_tickets,
                "max_ticket_exchanges": request.max_ticket_exchanges,
                "path_rules": request.path_rules,
                "revision": 0,
            },
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
        return cast(
            list[AuthProviderResponse],
            deps.with_service(
                lambda service: service.replace_workspace_auth_providers(
                    providers=request.providers,
                    expected_revision=request.expected_revision,
                    actor=actor,
                )
            )
            or [],
        )

    return api
