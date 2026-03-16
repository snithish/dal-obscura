from __future__ import annotations

import fnmatch
from dataclasses import dataclass

from dal_obscura.domain.query_planning import DatasetSelector


@dataclass(frozen=True)
class MaskRule:
    """Mask definition attached to a column or nested field."""

    type: str
    value: object | None = None


@dataclass(frozen=True)
class AccessRule:
    """Single policy rule evaluated for a matching principal token."""

    principals: list[str]
    columns: list[str]
    masks: dict[str, MaskRule]
    row_filter: str | None


@dataclass(frozen=True)
class DatasetPolicy:
    """Policy bundle for one catalog/target pair or wildcard target."""

    target: str
    catalog: str | None
    rules: list[AccessRule]


@dataclass(frozen=True)
class Policy:
    """In-memory representation of the full authorization document."""

    version: int
    datasets: list[DatasetPolicy]

    def match_dataset(self, selector: DatasetSelector) -> DatasetPolicy | None:
        """Returns the first dataset policy whose catalog and target glob match."""
        for dataset in self.datasets:
            if dataset.catalog != selector.catalog:
                continue
            if fnmatch.fnmatch(selector.target, dataset.target):
                return dataset
        return None


@dataclass(frozen=True)
class Principal:
    """Authenticated caller plus any groups and free-form identity attributes."""

    id: str
    groups: list[str]
    attributes: dict[str, str]

    def tokens(self) -> list[str]:
        """Returns tokens used by policy matching, including `group:` prefixes."""
        return [self.id, *[f"group:{group}" for group in self.groups]]


@dataclass(frozen=True)
class AccessDecision:
    """Concrete authorization outcome consumed by planning and fetch paths."""

    allowed_columns: list[str]
    masks: dict[str, MaskRule]
    row_filter: str | None
    policy_version: int
