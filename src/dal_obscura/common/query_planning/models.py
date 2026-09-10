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
        """Detach the request from the caller's mutable projection list."""

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
