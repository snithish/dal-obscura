from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class FilterSpec:
    where_sql: Optional[str]


def normalize_filter(where_sql: Optional[str]) -> FilterSpec:
    if where_sql:
        trimmed = where_sql.strip()
        if trimmed:
            return FilterSpec(where_sql=trimmed)
    return FilterSpec(where_sql=None)
