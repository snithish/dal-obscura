"""Small, frozen plugin contracts shared by core and approved adapters."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol

import pyarrow as pa

PluginKind = Literal["catalog", "table_format"]


@dataclass(frozen=True, slots=True)
class PluginDescriptor:
    """Static metadata read by registry and UI before factory import."""

    kind: PluginKind
    plugin_id: str
    api_version: str
    config_version: int
    distribution: str
    version: str
    capabilities: frozenset[str] = frozenset()
    config_schema: Mapping[str, object] = field(default_factory=dict)
    display_name: str = ""


@dataclass(frozen=True, slots=True)
class CatalogConfig:
    """Validated provider options; values may contain scoped secret references."""

    plugin_id: str
    instance_id: str
    revision: int
    options: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TableIdentifier:
    """A structured identifier whose segments never use display-name splitting."""

    namespace: tuple[str, ...]
    name: str

    def __post_init__(self) -> None:
        if not self.name or any(not part for part in (*self.namespace, self.name)):
            raise ValueError("Table identifier segments must be non-empty")


@dataclass(frozen=True, slots=True)
class DiscoveryPage:
    """Bounded catalog listing page with an opaque continuation token."""

    entries: tuple[TableIdentifier, ...]
    continuation: str | None = None


@dataclass(frozen=True, slots=True)
class TableHandle:
    """Opaque resolved table identity safe to bind into a publication."""

    catalog_plugin_id: str
    catalog_instance_id: str
    catalog_revision: int
    identifier: TableIdentifier
    format_plugin_id: str
    handle_version: int
    snapshot_id: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SchemaDescriptor:
    """Authoritative schema plus separately tracked snapshot identity."""

    schema_version: int
    fingerprint: str
    arrow_schema: pa.Schema
    snapshot_id: str | None = None
    stable_ids: bool = False


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """Request-scoped controls supplied by core to a trusted plugin."""

    deadline: datetime
    correlation_id: str
    capabilities: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class PluginError(Exception):
    """Stable adapter failure without provider details at transport boundaries."""

    code: str
    message: str

    def __str__(self) -> str:
        return self.message


class CatalogPlugin(Protocol):
    """Catalog factory instance supplied by an admitted distribution."""

    descriptor: PluginDescriptor

    def list_tables(
        self,
        context: ExecutionContext,
        *,
        continuation: str | None = None,
        limit: int,
    ) -> DiscoveryPage: ...

    def resolve_table(
        self,
        identifier: TableIdentifier,
        context: ExecutionContext,
    ) -> TableHandle: ...


class TableFormatPlugin(Protocol):
    """Format factory instance opened for one catalog-resolved handle."""

    descriptor: PluginDescriptor

    def schema(self, handle: TableHandle, context: ExecutionContext) -> SchemaDescriptor: ...

    def plan(
        self,
        handle: TableHandle,
        schema: SchemaDescriptor,
        context: ExecutionContext,
        *,
        projection: Sequence[str],
        row_filter: str | None,
        max_tasks: int,
    ) -> Sequence[object]: ...

    def execute(
        self,
        task: object,
        context: ExecutionContext,
    ) -> tuple[pa.Schema, Sequence[pa.RecordBatch]]: ...
