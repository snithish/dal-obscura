from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, kw_only=True)
class ResolvedTable(ABC):
    """Standardized dataset metadata returned by Catalogs to isolate them from execution."""

    catalog_name: str
    table_name: str
    format: str


class CatalogPlugin(Protocol):
    """Catalog behavior defining dataset lookup and format identification."""

    @property
    def name(self) -> str:
        """Name of the catalog registered in the configuration."""
        ...

    def get_table(self, target: str) -> ResolvedTable:
        """Resolves a target name to a format and its native table object."""
        ...
