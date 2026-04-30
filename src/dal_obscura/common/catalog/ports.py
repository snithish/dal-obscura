from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from dal_obscura.common.query_planning.models import PlanRequest
    from dal_obscura.common.table_format.ports import InputPartition, Plan


@dataclass(frozen=True, kw_only=True)
class TableFormat(ABC):
    """Catalog-resolved executable table format descriptor."""

    catalog_name: str
    table_name: str
    format: str

    @abstractmethod
    def get_schema(self) -> pa.Schema:
        """Extracts the Arrow schema for this table format."""

    @abstractmethod
    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        """Builds an execution plan for this table format."""

    @abstractmethod
    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        """Executes a pre-planned partition and streams Arrow record batches."""


class CatalogPlugin(ABC):
    """Catalog behavior defining dataset lookup and format identification."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the catalog registered in the configuration."""

    @abstractmethod
    def get_table(self, target: str) -> TableFormat:
        """Resolves a target name to a format and its native table object."""
