from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Protocol

from dal_obscura.domain.access_control.models import MaskRule


class RowTransformPort(Protocol):
    """Applies row-level filtering and masking to streamed backend batches."""

    def apply_filters_and_masks_stream(
        self,
        batches: Iterable[Any],
        columns: Iterable[str],
        row_filter: str | None,
        masks: Mapping[str, MaskRule],
    ) -> Iterable[Any]: ...
