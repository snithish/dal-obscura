from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

import pyarrow as pa

from dal_obscura.domain.catalog.ports import ResolvedTable
from dal_obscura.domain.query_planning.models import PlanRequest


@dataclass(frozen=True, kw_only=True)
class InputPartition:
    """A strictly typed unit of work planned by a FormatHandler."""


@dataclass(frozen=True)
class ScanTask:
    """A planned unit of work tailored to a specific dataset format."""

    format: str
    schema: Any
    partition: InputPartition


@dataclass(frozen=True)
class Plan:
    """The complete execution plan covering all split scan tasks."""

    schema: Any
    tasks: list[ScanTask]


class FormatHandler(Protocol):
    """Behavior bridging planning and execution for a specific table format."""

    @property
    def supported_format(self) -> str:
        """The logical dataset format this handler understands, e.g., 'iceberg'."""
        ...

    def get_schema(self, table: ResolvedTable) -> Any:
        """Extracts the generic Arrow schema from the native table object."""
        ...

    def plan(self, table: ResolvedTable, request: PlanRequest, max_tickets: int) -> Plan:
        """Converts native TableObject to abstract execution Tasks."""
        ...

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        """Reads data into an Arrow stream based on the provided task payload,
        returning its schema and batches."""
        ...
