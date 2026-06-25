from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    AuthProvidersRequest,
    RuntimeSettingsRequest,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    api = APIRouter()

    @api.get("/v1/settings/runtime", dependencies=[Depends(deps.require_actor)])
    def get_workspace_runtime_settings() -> object:
        return deps.with_service(lambda service: service.get_workspace_runtime_settings())

    @api.get("/v1/settings/auth-providers", dependencies=[Depends(deps.require_actor)])
    def list_workspace_auth_providers() -> object:
        return deps.with_service(lambda service: service.list_workspace_auth_providers())

    @api.get("/v1/cells/{cell_id}/runtime-settings", dependencies=[Depends(deps.require_actor)])
    def get_runtime_settings(cell_id: UUID) -> object:
        return deps.with_service(lambda service: service.get_runtime_settings(cell_id))

    @api.get("/v1/cells/{cell_id}/auth-providers", dependencies=[Depends(deps.require_actor)])
    def list_auth_providers(cell_id: UUID) -> object:
        return deps.with_service(lambda service: service.list_auth_providers(cell_id))

    @api.put(
        "/v1/tenants/{tenant_id}/cells/{cell_id}/runtime-settings",
        dependencies=[Depends(deps.require_admin)],
    )
    def upsert_runtime_settings(
        tenant_id: UUID,
        cell_id: UUID,
        request: RuntimeSettingsRequest,
    ) -> object:
        del tenant_id
        return deps.with_service(
            lambda service: service.upsert_runtime_settings(
                cell_id=cell_id,
                ttl=request.ticket_ttl_seconds,
                max_tickets=request.max_tickets,
                max_ticket_exchanges=request.max_ticket_exchanges,
            )
        ) or {"cell_id": str(cell_id)}

    @api.put("/v1/settings/runtime", dependencies=[Depends(deps.require_admin)])
    def upsert_workspace_runtime_settings(request: RuntimeSettingsRequest) -> object:
        return deps.with_service(
            lambda service: service.upsert_workspace_runtime_settings(
                ttl=request.ticket_ttl_seconds,
                max_tickets=request.max_tickets,
                max_ticket_exchanges=request.max_ticket_exchanges,
            )
        ) or {
            "ticket_ttl_seconds": request.ticket_ttl_seconds,
            "max_tickets": request.max_tickets,
            "max_ticket_exchanges": request.max_ticket_exchanges,
        }

    @api.put("/v1/cells/{cell_id}/auth-providers", dependencies=[Depends(deps.require_admin)])
    def replace_auth_providers(cell_id: UUID, request: AuthProvidersRequest) -> object:
        return deps.with_service(
            lambda service: service.replace_auth_providers(
                cell_id=cell_id,
                providers=request.providers,
            )
        ) or {"cell_id": str(cell_id)}

    @api.put("/v1/settings/auth-providers", dependencies=[Depends(deps.require_admin)])
    def replace_workspace_auth_providers(request: AuthProvidersRequest) -> object:
        return deps.with_service(
            lambda service: service.replace_workspace_auth_providers(
                providers=request.providers,
            )
        ) or {"providers": request.providers}

    return api
