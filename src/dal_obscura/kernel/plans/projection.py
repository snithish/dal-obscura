from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class ExecutionProjection:
    """Visible columns plus hidden dependencies required for execution."""

    visible_columns: Sequence[str]
    execution_columns: Sequence[str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "visible_columns", tuple(self.visible_columns))
        object.__setattr__(self, "execution_columns", tuple(self.execution_columns))


@dataclass(frozen=True)
class CompiledProjection:
    """SQL and advertised Arrow shape compiled from the same field traversal."""

    select_list: tuple[str, ...]
    output_schema: pa.Schema
