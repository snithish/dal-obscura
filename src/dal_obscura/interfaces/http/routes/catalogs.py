"""Catalog routes for the workspace API.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from dal_obscura.control import catalog_service
from dal_obscura.control.access import ControlPlaneActor
from dal_obscura.interfaces.http.routes.deps import ControlPlaneDeps
from dal_obscura.interfaces.http.routes.schemas import (
    CatalogDiagnosticResponse,
    CatalogInventoryResponse,
    CatalogMutationResponse,
    CatalogRequest,
    CatalogTablesResponse,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds workspace catalog routes.

    Example:
        ```python
        catalog_router = router(deps)
        ```
    """

    api = APIRouter()

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
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> CatalogTablesResponse:
        return deps.with_transaction(
            lambda service: catalog_service.discover_workspace_catalog_tables(
                service.session,
                name,
                egress_allowlist=service.catalog_egress_allowlist,
                session_key=actor.identity_key() if actor is not None else None,
                plugin_registry=service.plugin_registry,
                secret_provider=service.secret_provider,
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
        return deps.with_transaction(
            lambda service: catalog_service.diagnose_workspace_catalog(
                service.session,
                name,
                egress_allowlist=service.catalog_egress_allowlist,
                session_key=actor.identity_key() if actor is not None else None,
                plugin_registry=service.plugin_registry,
                secret_provider=service.secret_provider,
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
