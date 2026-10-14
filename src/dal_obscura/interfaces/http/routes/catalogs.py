"""Catalog routes for the workspace API."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.control.catalogs import service as catalog_service
from dal_obscura.control.catalogs.discovery import MAX_CURSOR_LENGTH
from dal_obscura.control.errors import ValidationFailure
from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.interfaces.http.routes.schemas import (
    CatalogDiagnosticResponse,
    CatalogInventoryResponse,
    CatalogMutationResponse,
    CatalogPublicationBatch,
    CatalogRequest,
    CatalogTablesResponse,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds workspace catalog routes."""

    api = APIRouter()

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
        return deps.with_transaction(
            lambda service: [
                catalog_service.upsert_workspace_catalog(
                    service.session,
                    name=name,
                    plugin_id=request.catalogs[name].plugin_id,
                    options=request.catalogs[name].options,
                    expected_revision=request.catalogs[name].expected_revision,
                    egress_allowlist=service.catalog_egress_allowlist,
                    actor_principal=actor.identity_key(),
                    plugin_registry=service.plugin_registry,
                )
                for name in sorted(request.catalogs)
            ]
        )

    @api.get(
        "/v1/catalogs",
        dependencies=[Depends(deps.require_admin)],
        response_model=list[CatalogInventoryResponse],
    )
    def list_workspace_catalogs() -> list[CatalogInventoryResponse]:
        return deps.with_transaction(
            lambda service: catalog_service.list_workspace_catalogs(service.session)
        )

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
        captured = deps.with_transaction(
            lambda service: catalog_service.capture_catalog_discovery(
                service.session,
                name,
                egress_allowlist=service.catalog_egress_allowlist,
                plugin_registry=service.plugin_registry,
            )
        )
        identity = actor.identity_key()
        scope = (str(captured.catalog_id), captured.config.revision, identity, limit)
        try:
            continuation = deps.discovery_cursors.resolve(cursor, scope=scope) if cursor else None
            page = catalog_service.discover_workspace_catalog_tables(
                captured,
                session_key=identity,
                plugin_registry=deps.plugin_registry,
                secret_provider=deps.secret_provider,
                continuation=continuation,
                limit=limit,
            )
            next_cursor = (
                deps.discovery_cursors.issue(page.continuation, scope=scope)
                if page.continuation is not None
                else None
            )
        except ValidationFailure as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        tables = deps.with_transaction(
            lambda service: catalog_service.catalog_discovery_tables(
                service.session, captured, page
            )
        )
        return CatalogTablesResponse(
            catalog=name, revision=captured.config.revision, tables=tables, next_cursor=next_cursor
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
        captured = deps.with_transaction(
            lambda service: catalog_service.capture_catalog_discovery(
                service.session,
                name,
                egress_allowlist=service.catalog_egress_allowlist,
                plugin_registry=service.plugin_registry,
            )
        )
        result = catalog_service.diagnose_workspace_catalog(
            captured,
            session_key=actor.identity_key(),
            plugin_registry=deps.plugin_registry,
            secret_provider=deps.secret_provider,
        )
        deps.with_transaction(
            lambda service: catalog_service.ensure_catalog_discovery_current(
                service.session, captured
            )
        )
        return CatalogDiagnosticResponse.model_validate(result)

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
        return deps.with_transaction(
            lambda service: catalog_service.upsert_workspace_catalog(
                service.session,
                name=name,
                plugin_id=request.plugin_id,
                options=request.options,
                expected_revision=request.expected_revision,
                egress_allowlist=service.catalog_egress_allowlist,
                actor_principal="system" if actor is None else actor.identity_key(),
                plugin_registry=service.plugin_registry,
            )
        )

    return api
