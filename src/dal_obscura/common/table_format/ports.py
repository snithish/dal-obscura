from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from dal_obscura.common.access_control.filters import RowFilter
from dal_obscura.common.catalog.ports import TableFormat


@dataclass(frozen=True, kw_only=True)
class InputPartition:
    """A strictly typed unit of work planned by a TableFormat."""


@dataclass(frozen=True)
class ScanTask:
    """A planned unit of work that carries executable table format context."""

    table_format: TableFormat
    schema: pa.Schema
    partition: InputPartition


@dataclass(frozen=True)
class Plan:
    """The complete execution plan covering all split scan tasks."""

    schema: pa.Schema
    tasks: list[ScanTask]
    full_row_filter: RowFilter | None = None
    backend_pushdown_row_filter: RowFilter | None = None
    residual_row_filter: RowFilter | None = None
