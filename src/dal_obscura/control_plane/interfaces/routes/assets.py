from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    AssetOwnersRequest,
    AssetRequest,
    AssetSchemaFieldsRequest,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    api = APIRouter()

    @api.get("/v1/assets", dependencies=[Depends(deps.require_actor)])
    def list_workspace_assets() -> object:
        return deps.with_service(lambda service: service.list_workspace_assets())

    @api.get("/v1/cells/{cell_id}/assets", dependencies=[Depends(deps.require_actor)])
    def list_assets(cell_id: UUID) -> object:
        return deps.with_service(lambda service: service.list_assets(cell_id))

    @api.get("/v1/assets/{asset_id}", dependencies=[Depends(deps.require_actor)])
    def get_workspace_asset(asset_id: UUID) -> object:
        return deps.with_service(lambda service: service.get_workspace_asset(asset_id))

    @api.put("/v1/assets/{asset_id}/owners", dependencies=[Depends(deps.require_admin)])
    def replace_asset_owners(asset_id: UUID, request: AssetOwnersRequest) -> object:
        owners = deps.with_service(
            lambda service: service.replace_asset_owners(asset_id=asset_id, owners=request.owners)
        )
        return {"asset_id": str(asset_id), "owners": owners}

    @api.put("/v1/assets/{asset_id}/schema-fields", dependencies=[Depends(deps.require_admin)])
    def replace_asset_schema_fields(
        asset_id: UUID,
        request: AssetSchemaFieldsRequest,
    ) -> object:
        fields = deps.with_service(
            lambda service: service.replace_asset_schema_fields(
                asset_id=asset_id,
                fields=[field.model_dump() for field in request.fields],
            )
        )
        return {"asset_id": str(asset_id), "fields": fields}

    @api.put(
        "/v1/tenants/{tenant_id}/cells/{cell_id}/assets/{catalog}/{target}",
        dependencies=[Depends(deps.require_admin)],
    )
    def upsert_asset(
        tenant_id: UUID,
        cell_id: UUID,
        catalog: str,
        target: str,
        request: AssetRequest,
    ) -> object:
        return deps.with_service(
            lambda service: service.upsert_asset(
                cell_id=cell_id,
                tenant_id=tenant_id,
                catalog=catalog,
                target=target,
                backend=request.backend,
                table_identifier=request.table_identifier,
                options=request.options,
            )
        )

    @api.put("/v1/assets/{catalog}/{target}", dependencies=[Depends(deps.require_admin)])
    def upsert_workspace_asset(catalog: str, target: str, request: AssetRequest) -> object:
        return deps.with_service(
            lambda service: service.upsert_workspace_asset(
                catalog=catalog,
                target=target,
                backend=request.backend,
                table_identifier=request.table_identifier,
                options=request.options,
            )
        )

    return api
