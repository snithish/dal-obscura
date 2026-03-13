from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class MaskRule:
    type: str
    value: Optional[str] = None


@dataclass(frozen=True)
class AccessRule:
    principals: List[str]
    columns: List[str]
    masks: Dict[str, MaskRule]
    row_filter: Optional[str]


@dataclass(frozen=True)
class DatasetPolicy:
    table: str
    rules: List[AccessRule]


@dataclass(frozen=True)
class Policy:
    version: int
    datasets: List[DatasetPolicy]

    def match_dataset(self, table_identifier: str) -> Optional[DatasetPolicy]:
        for dataset in self.datasets:
            if fnmatch.fnmatch(table_identifier, dataset.table):
                return dataset
        return None


@dataclass(frozen=True)
class Principal:
    id: str
    groups: List[str]
    attributes: Dict[str, str]

    def tokens(self) -> List[str]:
        return [self.id, *[f"group:{g}" for g in self.groups]]
