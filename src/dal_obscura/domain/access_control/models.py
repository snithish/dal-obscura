from __future__ import annotations

import fnmatch
from dataclasses import dataclass

from dal_obscura.domain.query_planning import DatasetSelector


@dataclass(frozen=True)
class MaskRule:
    type: str
    value: object | None = None


@dataclass(frozen=True)
class AccessRule:
    principals: list[str]
    columns: list[str]
    masks: dict[str, MaskRule]
    row_filter: str | None


@dataclass(frozen=True)
class DatasetPolicy:
    target: str
    catalog: str | None
    rules: list[AccessRule]


@dataclass(frozen=True)
class Policy:
    version: int
    datasets: list[DatasetPolicy]

    def match_dataset(self, selector: DatasetSelector) -> DatasetPolicy | None:
        for dataset in self.datasets:
            if dataset.catalog != selector.catalog:
                continue
            if fnmatch.fnmatch(selector.target, dataset.target):
                return dataset
        return None


@dataclass(frozen=True)
class Principal:
    id: str
    groups: list[str]
    attributes: dict[str, str]

    def tokens(self) -> list[str]:
        return [self.id, *[f"group:{group}" for group in self.groups]]


@dataclass(frozen=True)
class AccessDecision:
    allowed_columns: list[str]
    masks: dict[str, MaskRule]
    row_filter: str | None
    policy_version: int
