from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Request

from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    CellRequest,
    CellTenantRequest,
    TenantCellAssignmentRequest,
    TenantCellRequest,
    TenantRequest,
    request_payload,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    api = APIRouter()

    @api.get("/v1/workspace/summary", dependencies=[Depends(deps.require_actor)])
    def get_workspace_summary() -> object:
        return deps.with_service(lambda service: service.get_workspace_summary())

    @api.get("/v1/tenants", dependencies=[Depends(deps.require_actor)])
    def list_tenants() -> object:
        return deps.with_service(lambda service: service.list_tenants())

    @api.get("/v1/cells", dependencies=[Depends(deps.require_actor)])
    def list_cells() -> object:
        return deps.with_service(lambda service: service.list_cells())

    @api.get("/v1/tenants/{tenant_id}/cells", dependencies=[Depends(deps.require_actor)])
    def list_tenant_cells(tenant_id: UUID) -> object:
        return deps.with_service(lambda service: service.list_cells_for_tenant(tenant_id))

    @api.get("/v1/cell-tenant-assignments", dependencies=[Depends(deps.require_actor)])
    def list_cell_tenant_assignments() -> object:
        return deps.with_service(lambda service: service.list_cell_tenant_assignments())

    @api.post("/v1/tenants", dependencies=[Depends(deps.require_admin)])
    async def create_tenant(request: Request) -> object:
        payload = TenantRequest.model_validate(await request_payload(request))
        return deps.with_service(
            lambda service: service.create_tenant(
                slug=payload.slug,
                display_name=payload.display_name,
            )
        )

    @api.post("/v1/cells", dependencies=[Depends(deps.require_admin)])
    async def create_cell(request: Request) -> object:
        payload = CellRequest.model_validate(await request_payload(request))
        return deps.with_service(
            lambda service: service.create_cell(name=payload.name, region=payload.region)
        )

    @api.post("/v1/tenants/{tenant_id}/cells", dependencies=[Depends(deps.require_admin)])
    async def create_tenant_cell(tenant_id: UUID, request: Request) -> object:
        payload = TenantCellRequest.model_validate(await request_payload(request))
        return deps.with_service(
            lambda service: service.create_cell_for_tenant(
                tenant_id=tenant_id,
                name=payload.name,
                region=payload.region,
                shard_key=payload.shard_key,
            )
        )

    @api.put("/v1/cells/{cell_id}/tenants/{tenant_id}", dependencies=[Depends(deps.require_admin)])
    def assign_tenant(cell_id: UUID, tenant_id: UUID, request: CellTenantRequest) -> object:
        return deps.with_service(
            lambda service: service.assign_tenant(
                cell_id=cell_id,
                tenant_id=tenant_id,
                shard_key=request.shard_key,
            )
        ) or {"cell_id": str(cell_id), "tenant_id": str(tenant_id)}

    @api.post(
        "/v1/tenants/{tenant_id}/cell-assignments",
        dependencies=[Depends(deps.require_admin)],
    )
    async def assign_cell_to_tenant(tenant_id: UUID, request: Request) -> object:
        payload = TenantCellAssignmentRequest.model_validate(await request_payload(request))
        deps.with_service(
            lambda service: service.assign_tenant(
                cell_id=payload.cell_id,
                tenant_id=tenant_id,
                shard_key=payload.shard_key,
            )
        )
        return deps.with_service(lambda service: service.list_cells_for_tenant(tenant_id))

    return api
