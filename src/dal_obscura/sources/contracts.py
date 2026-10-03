"""Governed source orchestration, distinct from SDK storage plugin contracts."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

if TYPE_CHECKING:
    from dal_obscura.read.request import PlanRequest
    from dal_obscura.sources.planning import Plan


@dataclass(frozen=True, kw_only=True)
class CatalogTableListing:
    name: str
    provider_id: str
    table_identifier: str | None = None
    properties: dict[str, object] = field(default_factory=dict)


class Source(Protocol):
    """Core plans authorization dependencies and owns provider lifetimes."""

    catalog_name: str
    table_name: str
    format: str

    def get_schema(self) -> pa.Schema: ...
    def plan(self, request: PlanRequest, max_tickets: int) -> Plan: ...
    def execute(self, partition: object) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]: ...
