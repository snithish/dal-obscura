"""Stable plugin contracts with no dependency on Dal Obscura internals."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import isfinite
from typing import Literal, Protocol, cast

import pyarrow as pa

from dal_obscura_plugin_api.scan import ScanRequest
from dal_obscura_plugin_api.tasks import ScanTask, _freeze_json, _mutable_json

PluginKind = Literal["catalog", "table_format"]
PLUGIN_API_VERSION = "2"
PLUGIN_CONFIG_VERSION = 1
_PLUGIN_ID = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")
SUPPORTED_CAPABILITIES = frozenset(
    {
        "nested_schema",
        "field_id_stability",
        "snapshot_reads",
        "splittable_scan",
        "projection_pushdown",
        "filter_pushdown",
        "delete_files",
        "cancellation",
    }
)
_MAX_CONFIG_SCHEMA_DEPTH = 8
_MAX_CONFIG_SCHEMA_NODES = 256
_MAX_CONFIG_SCHEMA_STRING = 512
_MAX_IDENTIFIER_SEGMENTS = 32
_MAX_IDENTIFIER_SEGMENT_LENGTH = 256
_MAX_DISCOVERY_PAGE_ENTRIES = 500
_MAX_CONTINUATION_LENGTH = 4_096
_MAX_OPTION_DEPTH = 8
_MAX_OPTION_NODES = 512
_MAX_OPTION_STRING = 4_096
_MAX_OPTION_KEYS = 64
_MAX_HANDLE_METADATA_DEPTH = 12
_MAX_HANDLE_METADATA_NODES = 2_048
_MAX_HANDLE_METADATA_STRING = 1_048_576
_MAX_HANDLE_METADATA_BYTES = 16 * 1_048_576
_MAX_HANDLE_METADATA_KEYS = 128
_FORBIDDEN_CONFIG_KEYS = frozenset(
    {"$ref", "$schema", "remote", "remote_url", "schema_url", "script", "html"}
)


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
    output_formats: frozenset[str] = frozenset()
    handle_versions: frozenset[int] = frozenset({1})

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
        unsupported = set(self.capabilities) - SUPPORTED_CAPABILITIES
        if unsupported:
            raise ValueError(
                "Plugin descriptor contains unsupported capability: "
                + ", ".join(sorted(unsupported))
            )
        if len(self.output_formats) > 64 or any(
            not isinstance(format_id, str) or not _PLUGIN_ID.fullmatch(format_id)
            for format_id in self.output_formats
        ):
            raise ValueError("Plugin output format IDs must be valid, bounded IDs")
        if (
            len(self.handle_versions) > 16
            or not self.handle_versions
            or any(
                not isinstance(version, int) or version < 1 or version > 255
                for version in self.handle_versions
            )
        ):
            raise ValueError("Plugin handle versions must be positive bounded integers")
        _validate_config_schema(self.config_schema)
        object.__setattr__(self, "config_schema", _freeze_json(self.config_schema))
        for name in ("capabilities", "output_formats", "handle_versions"):
            object.__setattr__(self, name, frozenset(getattr(self, name)))
        if len(self.display_name) > _MAX_CONFIG_SCHEMA_STRING:
            raise ValueError("Plugin display name is too long")

    def to_json(self) -> dict[str, object]:
        """Detached, deterministic descriptor metadata for admission and tooling."""
        return {
            "kind": self.kind,
            "plugin_id": self.plugin_id,
            "api_version": self.api_version,
            "config_version": self.config_version,
            "distribution": self.distribution,
            "version": self.version,
            "capabilities": sorted(self.capabilities),
            "output_formats": sorted(self.output_formats),
            "handle_versions": sorted(self.handle_versions),
            "config_schema": _mutable_json(self.config_schema),
            "display_name": self.display_name,
        }


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
        _validate_options(self.options)
        object.__setattr__(self, "options", _freeze_json(self.options))


@dataclass(frozen=True, slots=True)
class TableIdentifier:
    namespace: tuple[str, ...]
    name: str

    def __post_init__(self) -> None:
        if not isinstance(self.namespace, (tuple, list)):
            raise ValueError("Table namespace must contain identifier segments")
        object.__setattr__(self, "namespace", tuple(self.namespace))
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
    entries: tuple[TableIdentifier, ...]
    continuation: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.entries, tuple):
            raise ValueError("Discovery page entries must be a tuple")
        if len(self.entries) > _MAX_DISCOVERY_PAGE_ENTRIES:
            raise ValueError("Discovery page contains too many entries")
        if any(not isinstance(entry, TableIdentifier) for entry in self.entries):
            raise ValueError("Discovery page entries must be table identifiers")
        if self.continuation is not None and (
            not isinstance(self.continuation, str)
            or not self.continuation
            or len(self.continuation) > _MAX_CONTINUATION_LENGTH
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in self.continuation)
        ):
            raise ValueError("Discovery continuation must be bounded printable text")


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

    def __post_init__(self) -> None:
        if not _PLUGIN_ID.fullmatch(self.catalog_plugin_id):
            raise ValueError("Invalid catalog plugin ID")
        if not _PLUGIN_ID.fullmatch(self.format_plugin_id):
            raise ValueError("Invalid table-format plugin ID")
        if not self.catalog_instance_id or len(self.catalog_instance_id) > 128:
            raise ValueError("Catalog instance ID must be non-empty and bounded")
        if type(self.catalog_revision) is not int or self.catalog_revision < 0:
            raise ValueError("Catalog revision must be a nonnegative integer")
        if type(self.handle_version) is not int or self.handle_version < 1:
            raise ValueError("Table handle version must be a positive integer")
        if self.snapshot_id is not None and (
            not self.snapshot_id
            or len(self.snapshot_id) > 256
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in self.snapshot_id)
        ):
            raise ValueError("Table handle snapshot ID must be bounded and printable")
        if not isinstance(self.identifier, TableIdentifier):
            raise ValueError("Table handle identifier is invalid")
        _validate_handle_metadata(self.metadata)
        object.__setattr__(self, "metadata", _freeze_json(self.metadata))

    def to_json(self) -> dict[str, object]:
        """Explicit passive contract; factories and executable objects never cross it."""
        return {
            "catalog_plugin_id": self.catalog_plugin_id,
            "catalog_instance_id": self.catalog_instance_id,
            "catalog_revision": self.catalog_revision,
            "identifier": {
                "namespace": list(self.identifier.namespace),
                "name": self.identifier.name,
            },
            "format_plugin_id": self.format_plugin_id,
            "handle_version": self.handle_version,
            "snapshot_id": self.snapshot_id,
            "metadata": _mutable_json(self.metadata),
        }

    @classmethod
    def from_json(cls, value: object) -> TableHandle:
        if not isinstance(value, dict) or set(value) != {
            "catalog_plugin_id",
            "catalog_instance_id",
            "catalog_revision",
            "identifier",
            "format_plugin_id",
            "handle_version",
            "snapshot_id",
            "metadata",
        }:
            raise ValueError("Invalid table handle JSON")
        wire = cast(dict[str, object], value)
        raw_identifier = wire["identifier"]
        if not isinstance(raw_identifier, dict) or set(raw_identifier) != {"namespace", "name"}:
            raise ValueError("Invalid table handle identifier JSON")
        identifier = cast(dict[str, object], raw_identifier)
        namespace, name = identifier["namespace"], identifier["name"]
        if (
            not isinstance(namespace, list)
            or not all(isinstance(part, str) for part in namespace)
            or not isinstance(name, str)
        ):
            raise ValueError("Invalid table handle identifier JSON")
        for key in ("catalog_plugin_id", "catalog_instance_id", "format_plugin_id"):
            if not isinstance(wire[key], str):
                raise ValueError("Table handle identities must be text")
        for key in ("catalog_revision", "handle_version"):
            if type(wire[key]) is not int:
                raise ValueError("Table handle revisions must be integers")
        snapshot_id = wire["snapshot_id"]
        if snapshot_id is not None and not isinstance(snapshot_id, str):
            raise ValueError("Table handle snapshot identity must be text")
        if not isinstance(wire["metadata"], Mapping):
            raise ValueError("Table handle metadata must be a mapping")
        return cls(
            catalog_plugin_id=cast(str, wire["catalog_plugin_id"]),
            catalog_instance_id=cast(str, wire["catalog_instance_id"]),
            catalog_revision=cast(int, wire["catalog_revision"]),
            identifier=TableIdentifier(namespace=tuple(cast(list[str], namespace)), name=name),
            format_plugin_id=cast(str, wire["format_plugin_id"]),
            handle_version=cast(int, wire["handle_version"]),
            snapshot_id=snapshot_id,
            metadata=cast(Mapping[str, object], wire["metadata"]),
        )


@dataclass(frozen=True, slots=True)
class SchemaDescriptor:
    arrow_schema: pa.Schema
    snapshot_id: str | None = None
    stable_ids: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.arrow_schema, pa.Schema):
            raise ValueError("Schema descriptor requires an Arrow schema")
        if type(self.stable_ids) is not bool:
            raise ValueError("Stable field ID claim must be boolean")
        if self.snapshot_id is not None and (
            not isinstance(self.snapshot_id, str)
            or not self.snapshot_id
            or len(self.snapshot_id) > 256
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in self.snapshot_id)
        ):
            raise ValueError("Schema snapshot identity must be bounded printable text")


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    deadline: datetime
    correlation_id: str
    cancel_check: Callable[[], bool] | None = None

    def __post_init__(self) -> None:
        if self.deadline.tzinfo is None or self.deadline.utcoffset() is None:
            raise ValueError("Execution deadline must be timezone-aware")
        if not self.correlation_id or len(self.correlation_id) > 96:
            raise ValueError("Execution correlation ID must be non-empty and bounded")
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in self.correlation_id):
            raise ValueError("Execution correlation ID contains control characters")
        if self.cancel_check is not None and not callable(self.cancel_check):
            raise ValueError("Execution cancellation check must be callable")

    def check_active(self) -> None:
        """Check before and after provider work, including each iterator pull."""
        if self.deadline <= datetime.now(timezone.utc):
            raise TimeoutError("Plugin execution deadline expired")
        if self.cancel_check is not None and self.cancel_check():
            raise InterruptedError("Plugin execution was cancelled")


class CatalogPlugin(Protocol):
    descriptor: PluginDescriptor

    def list_namespaces(
        self,
        context: ExecutionContext,
        *,
        namespace: tuple[str, ...] = (),
    ) -> tuple[tuple[str, ...], ...]: ...

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

    def close(self) -> None: ...


class CatalogFactory(Protocol):
    """Factory called by core with one validated config and request context."""

    def __call__(
        self,
        config: CatalogConfig,
        context: ExecutionContext,
        /,
    ) -> CatalogPlugin: ...


class TableFormatPlugin(Protocol):
    descriptor: PluginDescriptor

    def schema(self, context: ExecutionContext) -> SchemaDescriptor: ...

    def plan(self, request: ScanRequest, context: ExecutionContext) -> Iterable[ScanTask]:
        """Yield at most request.max_tasks immutable JSON tasks, covering all work.

        The factory binds the handle once. Request.schema contains exact projected
        fields; the format must honor their order and types during execution.
        An empty iterable means no rows. Close planning iterators on early stop.
        """
        ...

    def execute(
        self, task: ScanTask, context: ExecutionContext
    ) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]: ...

    def close(self) -> None: ...


class TableFormatFactory(Protocol):
    """Factory called by core with one catalog-resolved immutable handle."""

    def __call__(
        self,
        handle: TableHandle,
        context: ExecutionContext,
        /,
    ) -> TableFormatPlugin: ...


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


def _validate_options(value: object) -> None:  # noqa: C901
    """Bound provider options before a plugin factory receives them."""

    nodes = 0

    def visit(item: object, depth: int) -> None:  # noqa: C901
        nonlocal nodes
        nodes += 1
        if nodes > _MAX_OPTION_NODES:
            raise ValueError("Catalog options have too many values")
        if depth > _MAX_OPTION_DEPTH:
            raise ValueError("Catalog options are too deeply nested")
        if isinstance(item, Mapping):
            if len(item) > _MAX_OPTION_KEYS:
                raise ValueError("Catalog options have too many keys")
            for key, child in item.items():
                if (
                    not isinstance(key, str)
                    or not key
                    or len(key) > _MAX_OPTION_STRING
                    or any(ord(char) < 0x20 or ord(char) == 0x7F for char in key)
                ):
                    raise ValueError("Catalog option keys must be bounded printable strings")
                visit(child, depth + 1)
            return
        if isinstance(item, (list, tuple)):
            if len(item) > _MAX_OPTION_KEYS:
                raise ValueError("Catalog option arrays are too large")
            for child in item:
                visit(child, depth + 1)
            return
        if isinstance(item, str):
            if len(item) > _MAX_OPTION_STRING or any(
                ord(char) < 0x20 or ord(char) == 0x7F for char in item
            ):
                raise ValueError("Catalog option strings must be bounded printable values")
            return
        if item is None or isinstance(item, (bool, int)):
            return
        if isinstance(item, float) and isfinite(item):
            return
        raise ValueError("Catalog options must contain JSON-like values")

    visit(value, 0)


def _validate_handle_metadata(value: object) -> None:  # noqa: C901
    """Keep ticket-bound plugin metadata inert, bounded, and serializable."""

    nodes = 0
    string_bytes = 0

    def visit(item: object, depth: int) -> None:  # noqa: C901
        nonlocal nodes, string_bytes
        nodes += 1
        if nodes > _MAX_HANDLE_METADATA_NODES:
            raise ValueError("Table handle metadata has too many values")
        if depth > _MAX_HANDLE_METADATA_DEPTH:
            raise ValueError("Table handle metadata is too deeply nested")
        if isinstance(item, Mapping):
            if len(item) > _MAX_HANDLE_METADATA_KEYS:
                raise ValueError("Table handle metadata has too many keys")
            for key, child in item.items():
                if (
                    not isinstance(key, str)
                    or not key
                    or len(key) > 256
                    or any(ord(char) < 0x20 or ord(char) == 0x7F for char in key)
                ):
                    raise ValueError("Table handle metadata keys are invalid")
                visit(child, depth + 1)
            return
        if isinstance(item, (list, tuple)):
            if len(item) > _MAX_HANDLE_METADATA_KEYS:
                raise ValueError("Table handle metadata arrays are too large")
            for child in item:
                visit(child, depth + 1)
            return
        if isinstance(item, str):
            string_bytes += len(item.encode("utf-8"))
            if len(item) > _MAX_HANDLE_METADATA_STRING or string_bytes > _MAX_HANDLE_METADATA_BYTES:
                raise ValueError("Table handle metadata strings are too large")
            if any(ord(char) < 0x20 or ord(char) == 0x7F for char in item):
                raise ValueError("Table handle metadata strings must be printable")
            return
        if item is None or isinstance(item, (bool, int)):
            return
        if isinstance(item, float) and isfinite(item):
            return
        raise ValueError("Table handle metadata must contain JSON-like values")

    if not isinstance(value, Mapping):
        raise ValueError("Table handle metadata must be a mapping")
    visit(value, 0)
