from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, List, Mapping

import pyarrow as pa

from dal_obscura.policy.models import MaskRule


@dataclass(frozen=True)
class MaskedSelection:
    select_list: List[str]
    masked_columns: List[str]


class MaskApplier(ABC):
    @abstractmethod
    def apply(self, columns: Iterable[str], masks: Mapping[str, MaskRule]) -> MaskedSelection: ...

    @abstractmethod
    def masked_schema(
        self, base_schema: pa.Schema, columns: Iterable[str], masks: Mapping[str, MaskRule]
    ) -> pa.Schema: ...
