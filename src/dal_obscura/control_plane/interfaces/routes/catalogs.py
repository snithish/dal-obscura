from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Request

from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import CatalogRequest, request_payload


def router(deps: ControlPlaneDeps) -> APIRouter:
    api = APIRouter()

    @api.get("/v1/catalogs", dependencies=[Depends(deps.require_actor)])
    def list_workspace_catalogs() -> object:
        return deps.with_service(lambda service: service.list_workspace_catalogs())

    @api.get("/v1/catalogs/{name}/tables", dependencies=[Depends(deps.require_actor)])
    def discover_workspace_catalog_tables(name: str) -> object:
        return deps.with_service(lambda service: service.discover_workspace_catalog_tables(name))

    @api.get("/v1/cells/{cell_id}/catalogs", dependencies=[Depends(deps.require_actor)])
    def list_catalogs(cell_id: UUID) -> object:
        return deps.with_service(lambda service: service.list_catalogs(cell_id))

    @api.put(
        "/v1/tenants/{tenant_id}/cells/{cell_id}/catalogs/{name}",
        dependencies=[Depends(deps.require_admin)],
    )
    async def upsert_catalog(
        tenant_id: UUID,
        cell_id: UUID,
        name: str,
        request: Request,
    ) -> object:
        payload = CatalogRequest.model_validate(await request_payload(request))
        return deps.with_service(
            lambda service: service.upsert_catalog(
                cell_id=cell_id,
                tenant_id=tenant_id,
                name=name,
                module=payload.module,
                options=payload.options,
            )
        )

    @api.put("/v1/catalogs/{name}", dependencies=[Depends(deps.require_admin)])
    async def upsert_workspace_catalog(name: str, request: Request) -> object:
        payload = CatalogRequest.model_validate(await request_payload(request))
        return deps.with_service(
            lambda service: service.upsert_workspace_catalog(
                name=name,
                module=payload.module,
                options=payload.options,
            )
        )

    return api
