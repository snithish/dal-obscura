"""Detached persistence results used by governance operations."""

from dataclasses import dataclass
from uuid import UUID

from dal_obscura_plugin_api import CatalogConfig


@dataclass(frozen=True)
class WorkspaceState:
    id: int
    auth_provider_revision: int


@dataclass(frozen=True)
class AssetPage:
    """Cursor-paginated asset listing."""

    items: list[dict[str, object]]
    next_cursor: str | None


@dataclass(frozen=True)
class AuditEventPage:
    """Cursor-paginated audit events."""

    items: list[dict[str, object]]
    next_cursor: str | None


@dataclass(frozen=True)
class CatalogDiscovery:
    catalog_id: UUID
    config: CatalogConfig


@dataclass(frozen=True)
class AssetSchemaDiscovery:
    """Captured authorized binding; provider IO holds no SQL transaction."""

    asset_id: UUID
    revision: int
    name: str
    backend: str
    table_identifier: str
    catalog: CatalogDiscovery
