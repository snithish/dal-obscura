"""Stable plugin contracts with no dependency on Dal Obscura internals."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
import re
from typing import Literal, Protocol

import pyarrow as pa

PluginKind = Literal["catalog", "table_format"]
_PLUGIN_ID = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")


@dataclass(frozen=True, slots=True)
class PluginDescriptor:
    kind: PluginKind
    plugin_id: str
    api_version: str
    config_version: int
    distribution: str
    version: str
    capabilities: frozenset[str] = frozenset()
    config_schema: Mapping[str, object] = field(default_factory=dict)
    display_name: str = ""

    def __post_init__(self) -> None:
        if not _PLUGIN_ID.fullmatch(self.plugin_id):
            raise ValueError(f"Invalid plugin ID: {self.plugin_id!r}")
        if not self.api_version or len(self.api_version) > 32:
            raise ValueError("Plugin API version must be non-empty and bounded")
        if self.config_version < 1:
            raise ValueError("Plugin config version must be positive")
        if not self.distribution or not self.version:
            raise ValueError("Plugin distribution and version are required")
        if len(self.capabilities) > 64 or any(
            not isinstance(capability, str) or not capability or len(capability) > 64
            for capability in self.capabilities
        ):
            raise ValueError("Plugin capabilities must be bounded non-empty strings")
        if len(self.config_schema) > 64:
            raise ValueError("Plugin config schema is too large")


@dataclass(frozen=True, slots=True)
class CatalogConfig:
    plugin_id: str
    instance_id: str
    revision: int
    options: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not _PLUGIN_ID.fullmatch(self.plugin_id):
            raise ValueError(f"Invalid plugin ID: {self.plugin_id!r}")
        if not self.instance_id or len(self.instance_id) > 128:
            raise ValueError("Catalog instance ID must be non-empty and bounded")
        if self.revision < 0:
            raise ValueError("Catalog revision cannot be negative")


@dataclass(frozen=True, slots=True)
class TableIdentifier:
    namespace: tuple[str, ...]
    name: str

    def __post_init__(self) -> None:
        if not self.name or any(not part for part in (*self.namespace, self.name)):
            raise ValueError("Table identifier segments must be non-empty")


@dataclass(frozen=True, slots=True)
class DiscoveryPage:
    entries: tuple[TableIdentifier, ...]
    continuation: str | None = None


@dataclass(frozen=True, slots=True)
class TableHandle:
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
    schema_version: int
    fingerprint: str
    arrow_schema: pa.Schema
    snapshot_id: str | None = None
    stable_ids: bool = False

    def __post_init__(self) -> None:
        if self.schema_version < 1:
            raise ValueError("Schema version must be positive")
        if not re.fullmatch(r"[0-9a-f]{64}", self.fingerprint):
            raise ValueError("Schema fingerprint must be a SHA-256 hex digest")


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    deadline: datetime
    correlation_id: str
    capabilities: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class PluginError(Exception):
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


class CatalogPlugin(Protocol):
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
