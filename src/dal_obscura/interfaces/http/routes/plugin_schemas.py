"""HTTP plugin diagnostic response contracts."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import (
    BaseModel,
)


class PluginDescriptorResponse(BaseModel):
    """Admitted plugin descriptor exposed to operator tooling."""

    kind: Literal["catalog", "table_format", "secret_backend"]
    plugin_id: str
    api_version: str
    config_version: int
    distribution: str
    version: str
    display_name: str
    capabilities: list[str]
    output_formats: list[str]
    handle_versions: list[int]
    config_schema: dict[str, Any]
    status: Literal["admitted", "incompatible"]


class PluginPairResponse(BaseModel):
    """One catalog/table-format compatibility result."""

    catalog_plugin_id: str
    format_plugin_id: str
    capabilities: list[str]
    handle_versions: list[int]
    status: Literal["admitted", "incompatible"]


class PluginStateResponse(BaseModel):
    """Allowlisted plugin admission status without factory imports."""

    kind: Literal["catalog", "table_format", "secret_backend"]
    plugin_id: str
    status: Literal["enabled", "not_installed", "incompatible"]
    reason: str | None = None


class PluginListResponse(BaseModel):
    """Bounded admitted plugin registry response."""

    plugins: list[PluginDescriptorResponse]
    pairs: list[PluginPairResponse]
    states: list[PluginStateResponse]
