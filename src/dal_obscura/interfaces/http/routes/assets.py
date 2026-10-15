"""Asset routes for the workspace API."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from dal_obscura.adapters.sql import assets as _db_assets
from dal_obscura.application.governance.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control.assets import access as asset_access
from dal_obscura.control.assets import schema as schema_service
from dal_obscura.control.assets import service as asset_service
from dal_obscura.control.policies import service as policy_service
from dal_obscura.control.runtime import ControlContext
from dal_obscura.interfaces.http.request_context import current_request_id
from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.interfaces.http.routes.schemas import (
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
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds governed asset routes."""

    api = APIRouter()

    @api.get("/v1/assets", response_model=list[AssetInventoryResponse])
    def list_workspace_assets(
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> list[AssetInventoryResponse]:
        return deps.with_transaction(
            lambda service: asset_service.list_workspace_assets(service.session, actor)
        )

    @api.get("/v1/assets/page", response_model=AssetInventoryPageResponse)
    def list_workspace_assets_page(
        limit: int = Query(default=50, ge=1, le=200),
        cursor: str | None = Query(default=None, max_length=512),
        search: str | None = Query(default=None, max_length=200),
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetInventoryPageResponse:
        return deps.with_transaction(
            lambda service: asset_service.list_workspace_assets_page(
                service.session, actor, limit=limit, cursor=cursor, search=search
            )
        )

    @api.get("/v1/assets/{asset_id}", response_model=AssetDetailResponse)
    def get_workspace_asset(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetDetailResponse:
        return deps.with_transaction(
            lambda service: asset_service.get_workspace_asset(service.session, asset_id, actor)
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
        payload = deps.with_transaction(
            lambda service: asset_service.get_asset_access(service.session, asset_id, actor)
        )
        return AssetAccessResponse.model_validate(payload)

    @api.get("/v1/assets/{asset_id}/schema", response_model=AssetSchemaResponse)
    def get_asset_schema(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetSchemaResponse:
        return _discover_asset_schema(deps, asset_id, actor)

    @api.post(
        "/v1/assets/{asset_id}/tickets/revoke",
        response_model=AssetTokenRevocationResponse,
    )
    def revoke_asset_tickets(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetTokenRevocationResponse:
        result = deps.with_transaction(
            lambda service: policy_service.revoke_asset_tickets(
                service.session, asset_id, actor=actor, correlation_id=current_request_id()
            )
        )
        return AssetTokenRevocationResponse(asset_id=str(asset_id), **result)

    @api.put("/v1/assets/{asset_id}/owners", response_model=AssetOwnersResponse)
    def replace_asset_owners(
        asset_id: UUID,
        request: AssetOwnersRequest,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> AssetOwnersResponse:
        owners = deps.with_transaction(
            lambda service: asset_service.replace_asset_owners(
                service.session,
                asset_id,
                request.owners,
                expected_revision=request.expected_revision,
                actor=actor,
                correlation_id=current_request_id(),
            )
        )
        return AssetOwnersResponse(asset_id=str(asset_id), owners=owners)

    @api.get("/v1/assets/{asset_id}/grants", response_model=list[AssetGrantResponse])
    def list_asset_grants(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> list[AssetGrantResponse]:
        return deps.with_transaction(
            lambda service: _authorized_asset_grants(service, asset_id, actor)
        )

    @api.put("/v1/assets/{asset_id}/grants", response_model=AssetGrantsResponse)
    def replace_asset_grants(
        asset_id: UUID,
        request: AssetGrantsRequest,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetGrantsResponse:
        return deps.with_transaction(
            lambda service: _replace_authorized_asset_grants(
                service,
                asset_id,
                request,
                actor,
            )
        )

    @api.put(
        "/v1/assets/{asset_id}/schema-fields",
        response_model=AssetSchemaFieldsResponse,
        dependencies=[Depends(deps.require_admin)],
    )
    def replace_asset_schema_fields(
        asset_id: UUID,
        request: AssetSchemaFieldsRequest,
    ) -> AssetSchemaFieldsResponse:
        fields = deps.with_transaction(
            lambda service: asset_service.replace_asset_schema_fields(
                service.session,
                asset_id,
                [field.model_dump() for field in request.fields],
                expected_revision=request.expected_revision,
            )
        )
        return AssetSchemaFieldsResponse(asset_id=str(asset_id), fields=fields)

    @api.put(
        "/v1/assets/{catalog}/{target}",
        response_model=AssetMutationResponse,
        dependencies=[Depends(deps.require_admin)],
    )
    def upsert_workspace_asset(
        catalog: str, target: str, request: AssetRequest
    ) -> AssetMutationResponse:
        return deps.with_transaction(
            lambda service: asset_service.upsert_workspace_asset(
                service.session,
                catalog=catalog,
                target=target,
                backend=request.backend,
                table_identifier=request.table_identifier,
                options=request.options,
                expected_revision=request.expected_revision,
                plugin_registry=service.plugin_registry,
            )
        )

    return api


def _authorized_asset_grants(
    service: ControlContext,
    asset_id: UUID,
    actor: ControlPlaneActor,
) -> object:
    _ensure_grant_manager(service, asset_id, actor)
    return asset_service.list_asset_grants(service.session, asset_id)


def _replace_authorized_asset_grants(
    service: ControlContext,
    asset_id: UUID,
    request: AssetGrantsRequest,
    actor: ControlPlaneActor,
) -> object:
    # Lock before reading delegated authority or the current grant set.  The
    # replacement service takes the same lock before writing, so authorization
    # and the CAS mutation share one asset revision.
    _db_assets.lock_asset_for_update(service.session, asset_id)
    _ensure_grant_manager(service, asset_id, actor)
    grants = [item.model_dump() for item in request.grants]
    if not actor.platform_admin:
        existing_grants = {
            (item["principal"], item["capability"])
            for item in asset_service.list_asset_grants(service.session, asset_id)
        }
        if any(
            item["capability"] == "grant"
            and (item["principal"], item["capability"]) not in existing_grants
            for item in grants
        ):
            raise AuthorizationFailure(
                "Only platform admins may delegate grant-management capability."
            )
        _reject_self_escalation(service, asset_id, actor, grants)
    return {
        "asset_id": str(asset_id),
        "grants": asset_service.replace_asset_grants(
            service.session,
            asset_id,
            grants,
            expected_revision=request.expected_revision,
            actor=actor,
            correlation_id=current_request_id(),
        ),
    }


def _ensure_grant_manager(
    service: ControlContext, asset_id: UUID, actor: ControlPlaneActor
) -> None:
    asset_access.ensure_asset_capability(service.session, asset_id, actor, "grant")


def _reject_self_escalation(
    service: ControlContext,
    asset_id: UUID,
    actor: ControlPlaneActor,
    grants: list[dict[str, str]],
) -> None:
    """Prevents delegated grant managers from adding authority to themselves."""

    principals = actor.owner_principals()
    for grant in grants:
        if grant["principal"] not in principals:
            continue
        try:
            asset_access.ensure_asset_capability(
                service.session, asset_id, actor, grant["capability"]
            )
        except AuthorizationFailure as exc:
            raise AuthorizationFailure(
                "Cannot grant yourself a capability you do not already hold."
            ) from exc


def _discover_asset_schema(
    deps: ControlPlaneDeps, asset_id: UUID, actor: ControlPlaneActor
) -> AssetSchemaResponse:
    captured = deps.with_transaction(
        lambda service: schema_service.capture_asset_schema(
            service.session,
            asset_id,
            actor,
            egress_allowlist=service.catalog_egress_allowlist,
            plugin_registry=service.plugin_registry,
        )
    )
    try:
        payload = schema_service.get_asset_schema(
            captured,
            egress_allowlist=deps.catalog_egress_allowlist,
            plugin_registry=deps.plugin_registry,
            secret_provider=deps.secret_provider,
        )
    except ValidationFailure as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    deps.with_transaction(
        lambda service: schema_service.ensure_asset_schema_current(service.session, captured, actor)
    )
    return AssetSchemaResponse.model_validate(payload)
