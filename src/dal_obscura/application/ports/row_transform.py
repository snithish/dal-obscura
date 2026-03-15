from __future__ import annotations

from typing import Any, Iterable, Mapping, Protocol

from dal_obscura.domain.access_control import MaskRule


class RowTransformPort(Protocol):
    def apply_filters_and_masks_stream(
        self,
        batches: Iterable[Any],
        columns: Iterable[str],
        row_filter: str | None,
        masks: Mapping[str, MaskRule],
    ) -> Iterable[Any]: ...
