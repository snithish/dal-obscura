"""Asset routes for the workspace API.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    AssetGrantsRequest,
    AssetOwnersRequest,
    AssetRequest,
    AssetSchemaFieldsRequest,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds governed asset routes.

    Example:
        ```python
        asset_router = router(deps)
        ```
    """

    api = APIRouter()

    @api.get("/v1/assets")
    def list_workspace_assets(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.list_workspace_assets(actor))

    @api.get("/v1/assets/{asset_id}")
    def get_workspace_asset(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.get_workspace_asset(asset_id, actor))

    @api.get("/v1/assets/{asset_id}/schema")
    def get_asset_schema(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.get_asset_schema(asset_id, actor))

    @api.put("/v1/assets/{asset_id}/owners", dependencies=[Depends(deps.require_admin)])
    def replace_asset_owners(asset_id: UUID, request: AssetOwnersRequest) -> object:
        owners = deps.with_service(
            lambda service: service.replace_asset_owners(asset_id=asset_id, owners=request.owners)
        )
        return {"asset_id": str(asset_id), "owners": owners}

    @api.get("/v1/assets/{asset_id}/grants")
    def list_asset_grants(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: _authorized_asset_grants(service, asset_id, actor))

    @api.put("/v1/assets/{asset_id}/grants")
    def replace_asset_grants(
        asset_id: UUID,
        request: AssetGrantsRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: _replace_authorized_asset_grants(
                service,
                asset_id,
                request,
                actor,
            )
        )

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


def _authorized_asset_grants(
    service,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> object:
    _ensure_grant_manager(service, asset_id, actor)
    return service.list_asset_grants(asset_id)


def _replace_authorized_asset_grants(
    service,
    asset_id: UUID,
    request: AssetGrantsRequest,
    actor: ControlPlaneActor,
) -> object:
    _ensure_grant_manager(service, asset_id, actor)
    grants = [item.model_dump() for item in request.grants]
    return {"asset_id": str(asset_id), "grants": service.replace_asset_grants(asset_id, grants)}


def _ensure_grant_manager(service, asset_id: UUID, actor: ControlPlaneActor) -> None:
    service.ensure_asset_capability(asset_id, actor, "grant")
