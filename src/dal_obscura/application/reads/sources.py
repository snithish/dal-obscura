"""Runtime source, plan and request-context contracts."""

from __future__ import annotations

from collections.abc import Collection, Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

import pyarrow as pa

from dal_obscura.kernel.plans.request import PlanRequest
from dal_obscura.kernel.plans.snapshot import PublishedAssetSnapshot
from dal_obscura.kernel.policy.authorization import AuthorizationPort
from dal_obscura.kernel.policy.filters import RowFilter


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
    def execute(
        self, partition: object, *, deadline: datetime
    ) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]: ...


@dataclass(frozen=True)
class ScanTask:
    """A planned unit of work that carries executable table format context."""

    table_format: Source
    schema: pa.Schema
    partition: object


@dataclass(frozen=True)
class Plan:
    """The complete execution plan covering all split scan tasks."""

    schema: pa.Schema
    tasks: list[ScanTask]
    full_row_filter: RowFilter | None = None
    backend_pushdown_row_filter: RowFilter | None = None
    residual_row_filter: RowFilter | None = None


class AccessContextUnavailable(RuntimeError):
    """Planning cannot obtain a provider within its configured resource budget."""


@dataclass(frozen=True)
class AccessContext:
    table_format: Source
    authorizer: AuthorizationPort
    schema: pa.Schema


class AccessContextPort(Protocol):
    def open(self, catalog: str | None, target: str) -> AbstractContextManager[AccessContext]:
        """Bind source, policy and admission consistently; lease provider until exit."""
        ...


@dataclass(frozen=True)
class LiveRuntime:
    """Active runtime settings compiled from the control plane."""

    auth_chain: dict[str, Any]
    ticket: dict[str, Any]
    path_rules: list[dict[str, Any]] = field(default_factory=list)


class PublishedSnapshotReader(Protocol):
    def get_asset_and_catalog(self, catalog: str, target: str) -> PublishedAssetSnapshot: ...


class AdmittedPluginSnapshot(Protocol):
    def admitted(self) -> Collection[tuple[str, str]]: ...
