"""HTTP workspace request and response contracts."""

from __future__ import annotations

from pydantic import (
    BaseModel,
)


class WorkspaceSummaryResponse(BaseModel):
    """Permission-scoped workspace counts."""

    catalog_count: int
    asset_count: int
    unowned_asset_count: int
    missing_policy_count: int
    runtime_configured: bool
    enabled_auth_provider_count: int


class DataPlaneObservationResponse(BaseModel):
    """Explicit data-plane health observation state."""

    status: str
    reason: str


class WorkspaceObservationsResponse(BaseModel):
    """Control-plane observations kept separate from Flight health."""

    available: bool
    observed_at: str
    source: str
    data_plane: DataPlaneObservationResponse
