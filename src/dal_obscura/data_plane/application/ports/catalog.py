from __future__ import annotations

from typing import Protocol

from dal_obscura.common.catalog.ports import TableFormat


class CatalogRegistryPort(Protocol):
    """Resolves a governed logical target into a table format."""

    def describe(
        self,
        catalog: str | None,
        target: str,
    ) -> TableFormat: ...
