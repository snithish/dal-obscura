"""Runtime and authentication settings routes.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    AuthProvidersRequest,
    RuntimeSettingsRequest,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds workspace settings routes.

    Example:
        ```python
        settings_router = router(deps)
        ```
    """

    api = APIRouter()

    @api.get("/v1/settings/runtime", dependencies=[Depends(deps.require_admin)])
    def get_workspace_runtime_settings() -> object:
        return deps.with_service(lambda service: service.get_workspace_runtime_settings())

    @api.get("/v1/settings/auth-providers", dependencies=[Depends(deps.require_admin)])
    def list_workspace_auth_providers() -> object:
        return deps.with_service(lambda service: service.list_workspace_auth_providers())

    @api.put("/v1/settings/runtime", dependencies=[Depends(deps.require_admin)])
    def upsert_workspace_runtime_settings(
        request: RuntimeSettingsRequest,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.upsert_workspace_runtime_settings(
                ttl=request.ticket_ttl_seconds,
                max_tickets=request.max_tickets,
                max_ticket_exchanges=request.max_ticket_exchanges,
                expected_revision=request.expected_revision,
                actor=actor,
            )
        ) or {
            "ticket_ttl_seconds": request.ticket_ttl_seconds,
            "max_tickets": request.max_tickets,
            "max_ticket_exchanges": request.max_ticket_exchanges,
            "revision": 0,
        }

    @api.put("/v1/settings/auth-providers", dependencies=[Depends(deps.require_admin)])
    def replace_workspace_auth_providers(
        request: AuthProvidersRequest,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> object:
        return (
            deps.with_service(
                lambda service: service.replace_workspace_auth_providers(
                    providers=request.providers,
                    expected_revision=request.expected_revision,
                    actor=actor,
                )
            )
            or []
        )

    return api
