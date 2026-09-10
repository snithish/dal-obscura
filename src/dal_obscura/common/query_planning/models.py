from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from dal_obscura.common.access_control.filters import RowFilter


@dataclass(frozen=True)
class PlanRequest:
    """Client request for a dataset plus the projected columns to expose."""

    target: str
    columns: list[str]
    catalog: str | None = None
    row_filter: RowFilter | None = None

    def __post_init__(self) -> None:
        """Validates and detaches the request from caller-owned projection data."""

        if not isinstance(self.target, str) or not self.target.strip():
            raise ValueError("target must be non-empty text")
        if self.catalog is not None and (
            not isinstance(self.catalog, str) or not self.catalog.strip()
        ):
            raise ValueError("catalog must be non-empty text when supplied")
        if not isinstance(self.columns, list) or not self.columns:
            raise ValueError("columns must be a non-empty list")
        if not all(isinstance(column, str) and column.strip() for column in self.columns):
            raise ValueError("columns must contain non-empty text")
        if len(self.columns) != len(set(self.columns)):
            raise ValueError("columns must not contain duplicates")
        if "*" in self.columns and len(self.columns) != 1:
            raise ValueError("wildcard columns request cannot be mixed with explicit fields")
        object.__setattr__(self, "columns", list(self.columns))


@dataclass(frozen=True)
class ExecutionProjection:
    """Visible columns plus hidden dependencies required for execution."""

    visible_columns: list[str]
    internal_dependency_columns: list[str]
    execution_columns: list[str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "visible_columns", list(self.visible_columns))
        object.__setattr__(
            self,
            "internal_dependency_columns",
            list(self.internal_dependency_columns),
        )
        object.__setattr__(self, "execution_columns", list(self.execution_columns))


@dataclass(frozen=True)
class ReadSpec:
    """Metadata extracted from a read payload without executing the read."""

    target: str
    catalog: str | None
    columns: list[str]
    schema: pa.Schema

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", list(self.columns))
