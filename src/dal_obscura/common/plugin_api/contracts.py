"""Small, frozen plugin contracts shared by core and approved adapters."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from math import isfinite
from typing import Literal, Protocol

import pyarrow as pa

PluginKind = Literal["catalog", "table_format"]
_PLUGIN_ID = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")
_MAX_CONFIG_SCHEMA_DEPTH = 8
_MAX_CONFIG_SCHEMA_NODES = 256
_MAX_CONFIG_SCHEMA_STRING = 512
_MAX_IDENTIFIER_SEGMENTS = 32
_MAX_IDENTIFIER_SEGMENT_LENGTH = 256
_FORBIDDEN_CONFIG_KEYS = frozenset(
    {"$ref", "$schema", "remote", "remote_url", "schema_url", "script", "html"}
)


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
        _validate_config_schema(self.config_schema)
        if len(self.display_name) > _MAX_CONFIG_SCHEMA_STRING:
            raise ValueError("Plugin display name is too long")


@dataclass(frozen=True, slots=True)
class CatalogConfig:
    """Validated provider options; values may contain scoped secret references."""

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
    """A structured identifier whose segments never use display-name splitting."""

    namespace: tuple[str, ...]
    name: str

    def __post_init__(self) -> None:
        segments = (*self.namespace, self.name)
        if len(segments) > _MAX_IDENTIFIER_SEGMENTS:
            raise ValueError("Table identifier has too many segments")
        if any(
            not isinstance(part, str)
            or not part
            or len(part) > _MAX_IDENTIFIER_SEGMENT_LENGTH
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in part)
            for part in segments
        ):
            raise ValueError("Table identifier segments must be bounded printable strings")


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

    def __post_init__(self) -> None:
        if self.schema_version < 1:
            raise ValueError("Schema version must be positive")
        if not re.fullmatch(r"[0-9a-f]{64}", self.fingerprint):
            raise ValueError("Schema fingerprint must be a SHA-256 hex digest")


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """Request-scoped controls supplied by core to a trusted plugin."""

    deadline: datetime
    correlation_id: str
    capabilities: frozenset[str] = frozenset()
    cancel_check: Callable[[], bool] | None = None

    def __post_init__(self) -> None:
        if self.deadline.tzinfo is None or self.deadline.utcoffset() is None:
            raise ValueError("Execution deadline must be timezone-aware")
        if not self.correlation_id or len(self.correlation_id) > 96:
            raise ValueError("Execution correlation ID must be non-empty and bounded")
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in self.correlation_id):
            raise ValueError("Execution correlation ID contains control characters")
        if len(self.capabilities) > 64 or any(
            not isinstance(capability, str) or not capability or len(capability) > 64
            for capability in self.capabilities
        ):
            raise ValueError("Execution capabilities must be bounded non-empty strings")
        if self.cancel_check is not None and not callable(self.cancel_check):
            raise ValueError("Execution cancellation check must be callable")


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


def _validate_config_schema(value: object) -> None:  # noqa: C901
    """Bounds descriptor form data and rejects executable/remote references."""

    nodes = 0

    def visit(item: object, depth: int) -> None:  # noqa: C901
        nonlocal nodes
        nodes += 1
        if nodes > _MAX_CONFIG_SCHEMA_NODES:
            raise ValueError("Plugin config schema has too many nodes")
        if depth > _MAX_CONFIG_SCHEMA_DEPTH:
            raise ValueError("Plugin config schema is too deeply nested")
        if isinstance(item, Mapping):
            if len(item) > 64:
                raise ValueError("Plugin config schema object is too large")
            for raw_key, child in item.items():
                if not isinstance(raw_key, str) or not raw_key.strip():
                    raise ValueError("Plugin config schema keys must be non-empty strings")
                if len(raw_key) > _MAX_CONFIG_SCHEMA_STRING:
                    raise ValueError("Plugin config schema key is too long")
                key = raw_key.strip().lower()
                if key in _FORBIDDEN_CONFIG_KEYS or key.endswith("_html"):
                    raise ValueError("Plugin config schema contains a remote or executable field")
                visit(child, depth + 1)
            return
        if isinstance(item, (list, tuple)):
            if len(item) > 64:
                raise ValueError("Plugin config schema array is too large")
            for child in item:
                visit(child, depth + 1)
            return
        if isinstance(item, str):
            if len(item) > _MAX_CONFIG_SCHEMA_STRING:
                raise ValueError("Plugin config schema string is too long")
            lowered = item.strip().lower()
            if "<script" in lowered or "javascript:" in lowered or "data:text/html" in lowered:
                raise ValueError("Plugin config schema contains executable content")
            return
        if item is None or isinstance(item, (bool, int)):
            return
        if isinstance(item, float) and isfinite(item):
            return
        raise ValueError("Plugin config schema must contain JSON-like values")

    visit(value, 0)
