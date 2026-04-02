from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from dal_obscura.domain.access_control.models import MaskRule


@dataclass(frozen=True)
class MaskedSelection:
    """DuckDB-ready projection plus the columns that were masked."""

    select_list: list[str]
    masked_columns: list[str]


class MaskingPort(Protocol):
    """Builds masked projections and the schema those projections expose."""

    def apply(
        self,
        base_schema: pa.Schema,
        columns: Iterable[str],
        masks: Mapping[str, MaskRule],
    ) -> MaskedSelection: ...

    def masked_schema(
        self,
        base_schema: pa.Schema,
        columns: Iterable[str],
        masks: Mapping[str, MaskRule],
    ) -> pa.Schema: ...
