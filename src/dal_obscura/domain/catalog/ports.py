from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class ResolvedTable:
    """Standardized dataset metadata returned by Catalogs to isolate them from execution."""

    catalog_name: str
    table_name: str
    format: str
    table_object: Any


class CatalogPlugin(Protocol):
    """Catalog behavior defining dataset lookup and format identification."""

    @property
    def name(self) -> str:
        """Name of the catalog registered in the configuration."""
        ...

    def get_table(self, target: str) -> ResolvedTable:
        """Resolves a target name to a format and its native table object."""
        ...
