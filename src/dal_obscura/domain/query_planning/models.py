from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class PlanRequest:
    """Client request for a dataset plus the projected columns to expose."""

    target: str
    columns: list[str]
    catalog: str | None = None


@dataclass(frozen=True)
class ExecutionProjection:
    """Visible columns plus hidden dependencies required for execution."""

    visible_columns: list[str]
    internal_dependency_columns: list[str]
    execution_columns: list[str]


@dataclass(frozen=True)
class ReadSpec:
    """Metadata extracted from a read payload without executing the read."""

    target: str
    catalog: str | None
    columns: list[str]
    schema: pa.Schema
