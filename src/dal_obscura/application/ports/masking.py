from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Protocol

from dal_obscura.domain.access_control import MaskRule


@dataclass(frozen=True)
class MaskedSelection:
    select_list: list[str]
    masked_columns: list[str]


class MaskingPort(Protocol):
    def apply(self, columns: Iterable[str], masks: Mapping[str, MaskRule]) -> MaskedSelection: ...

    def masked_schema(
        self,
        base_schema: Any,
        columns: Iterable[str],
        masks: Mapping[str, MaskRule],
    ) -> Any: ...
