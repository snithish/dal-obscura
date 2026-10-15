"""Asset routes for the workspace API."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query

from dal_obscura.interfaces.http.request_context import current_request_id
from dal_obscura.interfaces.http.routes.asset_schemas import (
    AssetAccessResponse,
    AssetDetailResponse,
    AssetGrantResponse,
    AssetGrantsRequest,
    AssetGrantsResponse,
    AssetInventoryPageResponse,
    AssetInventoryResponse,
    AssetMutationResponse,
    AssetOwnersRequest,
    AssetOwnersResponse,
    AssetRequest,
    AssetSchemaFieldsRequest,
    AssetSchemaFieldsResponse,
    AssetSchemaResponse,
    AssetTokenRevocationResponse,
)
from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds governed asset routes."""

    api = APIRouter()

    @api.get("/v1/assets", response_model=list[AssetInventoryResponse])
    def list_workspace_assets(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> list[AssetInventoryResponse]:
        return [
            AssetInventoryResponse.model_validate(item)
            for item in deps.assets_commands.list_workspace_assets(
                actor=actor, correlation_id=current_request_id()
            )
        ]

    @api.get("/v1/assets/page", response_model=AssetInventoryPageResponse)
    def list_workspace_assets_page(
        limit: int = Query(default=50, ge=1, le=200),
        cursor: str | None = Query(default=None, max_length=512),
        search: str | None = Query(default=None, max_length=200),
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetInventoryPageResponse:
        return AssetInventoryPageResponse.model_validate(
            deps.assets_commands.list_workspace_assets_page(
                limit=limit,
                cursor=cursor,
                search=search,
                actor=actor,
                correlation_id=current_request_id(),
            )
        )

    @api.get("/v1/assets/{asset_id}", response_model=AssetDetailResponse)
    def get_workspace_asset(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetDetailResponse:
        return AssetDetailResponse.model_validate(
            deps.assets_commands.get_workspace_asset(
                asset_id, actor=actor, correlation_id=current_request_id()
            )
        )

    @api.get(
        "/v1/assets/{asset_id}/access",
        response_model=AssetAccessResponse,
        response_model_exclude_none=True,
    )
    def get_asset_access(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetAccessResponse:
        return AssetAccessResponse.model_validate(
            deps.assets_commands.get_asset_access(
                asset_id, actor=actor, correlation_id=current_request_id()
            )
        )

    @api.get("/v1/assets/{asset_id}/schema", response_model=AssetSchemaResponse)
    def get_asset_schema(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetSchemaResponse:
        return AssetSchemaResponse.model_validate(
            deps.assets_commands.get_asset_schema(
                asset_id, actor=actor, correlation_id=current_request_id()
            )
        )

    @api.post(
        "/v1/assets/{asset_id}/tickets/revoke",
        response_model=AssetTokenRevocationResponse,
    )
    def revoke_asset_tickets(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetTokenRevocationResponse:
        result = deps.policies_commands.revoke_asset_tickets(
            asset_id, actor=actor, correlation_id=current_request_id()
        )
        return AssetTokenRevocationResponse(asset_id=str(asset_id), **result)

    @api.put("/v1/assets/{asset_id}/owners", response_model=AssetOwnersResponse)
    def replace_asset_owners(
        asset_id: UUID,
        request: AssetOwnersRequest,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> AssetOwnersResponse:
        owners = deps.assets_commands.replace_asset_owners(
            asset_id,
            request.owners,
            expected_revision=request.expected_revision,
            actor=actor,
            correlation_id=current_request_id(),
        )
        return AssetOwnersResponse(asset_id=str(asset_id), owners=owners)

    @api.get("/v1/assets/{asset_id}/grants", response_model=list[AssetGrantResponse])
    def list_asset_grants(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> list[AssetGrantResponse]:
        return [
            AssetGrantResponse.model_validate(item)
            for item in deps.assets_commands.list_asset_grants(
                asset_id, actor=actor, correlation_id=current_request_id()
            )
        ]

    @api.put("/v1/assets/{asset_id}/grants", response_model=AssetGrantsResponse)
    def replace_asset_grants(
        asset_id: UUID,
        request: AssetGrantsRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetGrantsResponse:
        return AssetGrantsResponse.model_validate(
            deps.assets_commands.replace_asset_grants(
                asset_id,
                [item.model_dump() for item in request.grants],
                expected_revision=request.expected_revision,
                actor=actor,
                correlation_id=current_request_id(),
            )
        )

    @api.put(
        "/v1/assets/{asset_id}/schema-fields",
        response_model=AssetSchemaFieldsResponse,
    )
    def replace_asset_schema_fields(
        asset_id: UUID,
        request: AssetSchemaFieldsRequest,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> AssetSchemaFieldsResponse:
        fields = deps.assets_commands.replace_asset_schema_fields(
            asset_id,
            [field.model_dump() for field in request.fields],
            expected_revision=request.expected_revision,
            actor=actor,
            correlation_id=current_request_id(),
        )
        return AssetSchemaFieldsResponse.model_validate(
            {"asset_id": str(asset_id), "fields": fields}
        )

    @api.put(
        "/v1/assets/{catalog}/{target}",
        response_model=AssetMutationResponse,
    )
    def upsert_workspace_asset(
        catalog: str,
        target: str,
        request: AssetRequest,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> AssetMutationResponse:
        return AssetMutationResponse.model_validate(
            deps.assets_commands.upsert_workspace_asset(
                catalog=catalog,
                target=target,
                backend=request.backend,
                table_identifier=request.table_identifier,
                options=request.options,
                expected_revision=request.expected_revision,
                actor=actor,
                correlation_id=current_request_id(),
            )
        )

    return api
