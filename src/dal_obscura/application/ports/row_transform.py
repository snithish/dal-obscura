from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Protocol

import pyarrow as pa

from dal_obscura.domain.access_control.filters import RowFilter
from dal_obscura.domain.access_control.models import MaskRule


class RowTransformPort(Protocol):
    """Applies row-level filtering and masking to streamed backend batches."""

    def apply_filters_and_masks_stream(
        self,
        batches: Iterable[pa.RecordBatch],
        columns: Iterable[str],
        row_filter: RowFilter | None,
        masks: Mapping[str, MaskRule],
    ) -> Iterable[pa.RecordBatch]: ...
