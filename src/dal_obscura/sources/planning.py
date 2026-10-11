"""Execution-planning value objects shared by table format implementations."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from dal_obscura.policy.filters import RowFilter
from dal_obscura.sources.contracts import Source


@dataclass(frozen=True)
class ScanTask:
    """A planned unit of work that carries executable table format context."""

    table_format: Source
    schema: pa.Schema
    partition: object


@dataclass(frozen=True)
class Plan:
    """The complete execution plan covering all split scan tasks."""

    schema: pa.Schema
    tasks: list[ScanTask]
    full_row_filter: RowFilter | None = None
    backend_pushdown_row_filter: RowFilter | None = None
    residual_row_filter: RowFilter | None = None
