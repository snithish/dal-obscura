from __future__ import annotations

from typing import Protocol

from dal_obscura.domain.catalog.ports import TableFormat


class CatalogRegistryPort(Protocol):
    """Resolves a tenant-scoped logical target into a table format."""

    def describe(self, catalog: str | None, target: str, *, tenant_id: str) -> TableFormat: ...
