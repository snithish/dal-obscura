"""Internal catalog lookup and executable scan contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from dal_obscura.read.request import PlanRequest
    from dal_obscura.sources.planning import InputPartition, Plan


@dataclass(frozen=True, kw_only=True)
class CatalogTableListing:
    """Lightweight table metadata returned by catalog discovery.

    Example:
        ```python
        listing = CatalogTableListing(name="default.users", provider_id="iceberg")
        ```
    """

    name: str
    provider_id: str
    table_identifier: str | None = None
    properties: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, kw_only=True)
class TableFormat(ABC):
    """Catalog-resolved executable table format descriptor.

    Example:
        ```python
        schema = table_format.get_schema()
        plan = table_format.plan(request, max_tickets=32)
        for task in plan.tasks:
            task_schema, batches = table_format.execute(task.partition)
        ```
    """

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
    """Catalog behavior defining configured Iceberg dataset lookup."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the catalog registered in the configuration."""

    @abstractmethod
    def resolve_table(self, target: str) -> TableFormat:
        """Resolves a target name to an executable table format."""

    @abstractmethod
    def list_tables(self) -> list[CatalogTableListing]:
        """Lists tables visible through this catalog."""
