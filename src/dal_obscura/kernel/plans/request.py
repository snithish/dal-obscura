from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dal_obscura.kernel.policy.filters import RowFilter


@dataclass(frozen=True)
class PlanRequest:
    """Client request for a dataset plus the projected columns to expose."""

    target: str
    columns: Sequence[str]
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
        if not isinstance(self.columns, list | tuple) or not self.columns:
            raise ValueError("columns must be a non-empty list")
        if not all(isinstance(column, str) and column.strip() for column in self.columns):
            raise ValueError("columns must contain non-empty text")
        if len(self.columns) != len(set(self.columns)):
            raise ValueError("columns must not contain duplicates")
        if "*" in self.columns and len(self.columns) != 1:
            raise ValueError("wildcard columns request cannot be mixed with explicit fields")
        object.__setattr__(self, "columns", tuple(self.columns))
