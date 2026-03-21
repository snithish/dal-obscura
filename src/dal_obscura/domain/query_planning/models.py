from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DatasetSelector:
    """Logical dataset identity used in authz, planning, and ticket validation."""

    target: str
    catalog: str | None = None


@dataclass(frozen=True)
class PlanRequest:
    """Client request for a dataset plus the projected columns to expose."""

    target: str
    columns: list[str]
    catalog: str | None = None


@dataclass(frozen=True)
class ReadSpec:
    """Metadata extracted from a read payload without executing the read."""

    dataset: DatasetSelector
    columns: list[str]
    schema: Any
