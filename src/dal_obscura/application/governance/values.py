"""Detached persistence results used by governance operations."""

from dataclasses import dataclass


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
