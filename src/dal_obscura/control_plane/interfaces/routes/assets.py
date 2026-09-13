"""Asset routes for the workspace API.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import AuthorizationFailure
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
    AssetAccessResponse,
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

    @api.get("/v1/assets/page")
    def list_workspace_assets_page(
        limit: int = Query(default=50, ge=1, le=200),
        cursor: str | None = Query(default=None, max_length=512),
        search: str | None = Query(default=None, max_length=200),
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(
            lambda service: service.list_workspace_assets_page(
                actor,
                limit=limit,
                cursor=cursor,
                search=search,
            )
        )

    @api.get("/v1/assets/{asset_id}")
    def get_workspace_asset(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.get_workspace_asset(asset_id, actor))

    @api.get(
        "/v1/assets/{asset_id}/access",
        response_model=AssetAccessResponse,
        response_model_exclude_none=True,
    )
    def get_asset_access(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> AssetAccessResponse:
        return deps.with_service(lambda service: service.get_asset_access(asset_id, actor))

    @api.get("/v1/assets/{asset_id}/schema")
    def get_asset_schema(
        asset_id: UUID,
        actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        return deps.with_service(lambda service: service.get_asset_schema(asset_id, actor))

    @api.put("/v1/assets/{asset_id}/owners")
    def replace_asset_owners(
        asset_id: UUID,
        request: AssetOwnersRequest,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> object:
        owners = deps.with_service(
            lambda service: service.replace_asset_owners(
                asset_id=asset_id,
                owners=request.owners,
                expected_revision=request.expected_revision,
                actor=actor,
            )
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
                expected_revision=request.expected_revision,
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
                expected_revision=request.expected_revision,
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
    # Lock before reading delegated authority or the current grant set.  The
    # replacement service takes the same lock before writing, so authorization
    # and the CAS mutation share one asset generation.
    service.lock_asset_for_publication(asset_id)
    _ensure_grant_manager(service, asset_id, actor)
    grants = [item.model_dump() for item in request.grants]
    if not actor.platform_admin:
        existing_grants = {
            (item["principal"], item["capability"])
            for item in service.list_asset_grants(asset_id)
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
        "grants": service.replace_asset_grants(
            asset_id,
            grants,
            expected_revision=request.expected_revision,
            actor=actor,
        ),
    }


def _ensure_grant_manager(service, asset_id: UUID, actor: ControlPlaneActor) -> None:
    service.ensure_asset_capability(asset_id, actor, "grant")


def _reject_self_escalation(
    service,
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
            service.ensure_asset_capability(asset_id, actor, grant["capability"])
        except AuthorizationFailure as exc:
            raise AuthorizationFailure(
                "Cannot grant yourself a capability you do not already hold."
            ) from exc
