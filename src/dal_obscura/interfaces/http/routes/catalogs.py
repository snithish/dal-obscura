"""Catalog routes for the workspace API."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from dal_obscura.application.governance.catalogs.discovery import MAX_CURSOR_LENGTH
from dal_obscura.application.governance.catalogs.service import CatalogPublicationInput
from dal_obscura.interfaces.http.request_context import current_request_id
from dal_obscura.interfaces.http.routes.catalog_schemas import (
    CatalogDiagnosticResponse,
    CatalogInventoryResponse,
    CatalogMutationResponse,
    CatalogPublicationBatch,
    CatalogRequest,
    CatalogTablesResponse,
)
from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.kernel.identity.actor import ControlPlaneActor


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds workspace catalog routes."""

    api = APIRouter()
    admin_actor = Depends(deps.require_admin)

    @api.post(
        "/v1/catalogs/publish",
        response_model=list[CatalogMutationResponse],
    )
    def publish_workspace_catalogs(
        request: CatalogPublicationBatch,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> list[CatalogMutationResponse]:
        # One transaction owns all configuration changes and their audit records.
        # A deterministic lock order prevents opposite-order releases deadlocking.
        catalogs: dict[str, CatalogPublicationInput] = {
            name: {
                "plugin_id": item.plugin_id,
                "options": item.options,
                "expected_revision": item.expected_revision,
            }
            for name, item in request.catalogs.items()
        }
        return [
            CatalogMutationResponse.model_validate(item)
            for item in deps.catalogs_commands.publish_workspace_catalogs(
                catalogs, actor=actor, correlation_id=current_request_id()
            )
        ]

    @api.get(
        "/v1/catalogs",
        dependencies=[Depends(deps.require_admin)],
        response_model=list[CatalogInventoryResponse],
    )
    def list_workspace_catalogs(
        actor: ControlPlaneActor = admin_actor,
    ) -> list[CatalogInventoryResponse]:
        return [
            CatalogInventoryResponse.model_validate(item)
            for item in deps.catalogs_commands.list_workspace_catalogs(
                actor=actor, correlation_id=current_request_id()
            )
        ]

    @api.get(
        "/v1/catalogs/{name}/tables",
        dependencies=[Depends(deps.require_admin)],
        response_model=CatalogTablesResponse,
    )
    def discover_workspace_catalog_tables(
        name: str,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        cursor: Annotated[str | None, Query(min_length=1, max_length=MAX_CURSOR_LENGTH)] = None,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> CatalogTablesResponse:
        return CatalogTablesResponse.model_validate(
            deps.catalogs_commands.discover_workspace_catalog_tables(
                name, limit=limit, cursor=cursor, actor=actor, correlation_id=current_request_id()
            )
        )

    @api.get(
        "/v1/catalogs/{name}/diagnostics",
        dependencies=[Depends(deps.require_admin)],
        response_model=CatalogDiagnosticResponse,
    )
    def diagnose_workspace_catalog(
        name: str,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> CatalogDiagnosticResponse:
        return CatalogDiagnosticResponse.model_validate(
            deps.catalogs_commands.diagnose_workspace_catalog(
                name, actor=actor, correlation_id=current_request_id()
            )
        )

    @api.put(
        "/v1/catalogs/{name}",
        response_model=CatalogMutationResponse,
        dependencies=[Depends(deps.require_admin)],
    )
    def upsert_workspace_catalog(
        name: str,
        request: CatalogRequest,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> CatalogMutationResponse:
        return CatalogMutationResponse.model_validate(
            deps.catalogs_commands.upsert_workspace_catalog(
                name,
                plugin_id=request.plugin_id,
                options=request.options,
                expected_revision=request.expected_revision,
                actor=actor,
                correlation_id=current_request_id(),
            )
        )

    return api
