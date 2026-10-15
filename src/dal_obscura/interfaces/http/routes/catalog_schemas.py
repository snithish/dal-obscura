"""HTTP catalog request and response contracts."""

from __future__ import annotations

from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    Field,
)

from dal_obscura.application.governance.catalogs.service import MAX_CATALOG_PUBLICATION_SIZE
from dal_obscura.interfaces.http.routes.schemas import (
    StrictModel,
)


class CatalogInventoryResponse(BaseModel):
    """Workspace catalog summary safe for management clients."""

    id: UUID
    name: str
    plugin_id: str
    options: dict[str, Any]
    status: str
    revision: int
    discovered_table_count: int = 0
    governed_asset_count: int = 0


class CatalogMutationResponse(BaseModel):
    """Stable identity returned after creating or updating a catalog."""

    id: str
    name: str


class DiscoveredTableResponse(BaseModel):
    backend: str
    name: str
    table_identifier: str
    target: str
    governed: bool


class CatalogTablesResponse(BaseModel):
    """One provider page; cursors are caller/revision bound and expire after ten minutes."""

    catalog: str
    revision: int
    tables: list[DiscoveredTableResponse] = Field(max_length=500)
    next_cursor: str | None


class CatalogDiagnosticResponse(BaseModel):
    """Bounded catalog connectivity diagnostic."""

    catalog: str
    status: Literal["ready", "unavailable"]
    message: str
    checked_at: str
    table_count: int | None = None
    sample_tables: list[str] | None = None


class CatalogRequest(StrictModel):
    """Catalog configuration request.

    Example:
        ```python
        CatalogRequest(plugin_id="iceberg.catalog", options={"uri": "sqlite:///catalog.db"})
        ```
    """

    plugin_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_.-]{0,63}$")
    options: dict[str, Any] = Field(default_factory=dict)
    expected_revision: int | None = Field(default=None, ge=0)


class CatalogPublicationRequest(CatalogRequest):
    expected_revision: int = Field(ge=0, strict=True)


class CatalogPublicationBatch(StrictModel):
    catalogs: dict[
        Annotated[str, Field(min_length=1, max_length=255)], CatalogPublicationRequest
    ] = Field(min_length=1, max_length=MAX_CATALOG_PUBLICATION_SIZE)
