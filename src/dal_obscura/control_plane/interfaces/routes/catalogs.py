"""Catalog routes for the workspace API.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import (
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
        return deps.with_service(lambda service: service.list_workspace_catalogs())

    @api.get(
        "/v1/catalogs/{name}/tables",
        dependencies=[Depends(deps.require_admin)],
        response_model=CatalogTablesResponse,
    )
    def discover_workspace_catalog_tables(
        name: str,
        actor: ControlPlaneActor = Depends(deps.require_admin),  # noqa: B008
    ) -> CatalogTablesResponse:
        return deps.with_service(
            lambda service: service.discover_workspace_catalog_tables(name, actor=actor)
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
        return deps.with_service(
            lambda service: service.diagnose_workspace_catalog(name, actor=actor)
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
        return deps.with_service(
            lambda service: service.upsert_workspace_catalog(
                name=name,
                plugin_id=request.plugin_id,
                options=request.options,
                expected_revision=request.expected_revision,
                actor=actor,
            )
        )

    return api
