"""Operator-controlled immutable membership catalog for Parquet datasets."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
from dal_obscura_plugin_api import (
    CatalogConfig,
    CatalogPlugin,
    DiscoveryPage,
    ExecutionContext,
    PluginDescriptor,
    TableHandle,
    TableIdentifier,
)

_MAX_MANIFEST_BYTES = 1_048_576
_MAX_TABLES = 10_000
_MAX_FILES_PER_TABLE = 100_000
_MAX_PATH_LENGTH = 1_024

CATALOG_DESCRIPTOR = PluginDescriptor(
    kind="catalog",
    plugin_id="manifest",
    api_version="1",
    config_version=1,
    distribution="dal-obscura-manifest-parquet",
    version="0.1.0",
    capabilities=frozenset({"nested_schema", "splittable_scan"}),
    display_name="Manifest Parquet dataset",
    config_schema={
        "fields": [
            {"name": "manifest_path", "type": "string", "required": True},
            {"name": "root", "type": "string", "required": True},
        ]
    },
)


@dataclass(frozen=True, slots=True)
class _ManifestTable:
    identifier: TableIdentifier
    files: tuple[str, ...]
    schema: pa.Schema
    field_ids: tuple[str, ...]


class ManifestCatalog(CatalogPlugin):
    """Catalog whose table membership and schema come from one pinned manifest."""

    descriptor = CATALOG_DESCRIPTOR

    def __init__(self, config: CatalogConfig, context: ExecutionContext) -> None:
        _check_context(context)
        self._config = config
        options = dict(config.options)
        self._root = _required_root(options.get("root"))
        manifest_path = _required_path(options.get("manifest_path"), "manifest_path")
        self._manifest_path = _safe_child(self._root, manifest_path)
        self._revision, self._manifest_hash, self._tables = _load_manifest(
            self._manifest_path, self._root
        )

    def list_tables(
        self,
        context: ExecutionContext,
        *,
        continuation: str | None = None,
        limit: int,
    ) -> DiscoveryPage:
        _check_context(context)
        if limit <= 0 or limit > _MAX_TABLES:
            raise ValueError("catalog page limit is invalid")
        names = sorted(
            self._tables,
            key=lambda table: (*table.identifier.namespace, table.identifier.name),
        )
        start = 0
        if continuation is not None:
            try:
                start = names.index(continuation) + 1
            except ValueError as exc:
                raise ValueError("catalog continuation is invalid") from exc
        page = names[start : start + limit]
        next_token = page[-1] if start + limit < len(names) else None
        return DiscoveryPage(tuple(item.identifier for item in page), next_token)

    def resolve_table(self, identifier: TableIdentifier, context: ExecutionContext) -> TableHandle:
        _check_context(context)
        key = _identifier_key(identifier)
        table = next(
            (item for item in self._tables if _identifier_key(item.identifier) == key),
            None,
        )
        if table is None:
            raise KeyError("table is not present in the operator manifest")
        metadata = {
            "manifest_path": str(self._manifest_path),
            "root": str(self._root),
            "manifest_hash": self._manifest_hash,
            "revision": self._revision,
            "files": table.files,
            "schema_ipc": base64.b64encode(table.schema.serialize().to_pybytes()).decode("ascii"),
            "field_ids": table.field_ids,
            "schema_identities": _schema_identities(table.schema, table.field_ids),
        }
        return TableHandle(
            catalog_plugin_id=self.descriptor.plugin_id,
            catalog_instance_id=self._config.instance_id,
            catalog_revision=self._config.revision,
            identifier=identifier,
            format_plugin_id="parquet.dataset",
            handle_version=1,
            snapshot_id=self._revision,
            metadata=metadata,
        )

    def close(self) -> None:
        return None


def manifest_factory(config: CatalogConfig, context: ExecutionContext) -> ManifestCatalog:
    """Open one manifest catalog from a typed public SDK config."""

    if config.plugin_id != CATALOG_DESCRIPTOR.plugin_id:
        raise ValueError("manifest factory received an incompatible plugin ID")
    return ManifestCatalog(config, context)


def _load_manifest(  # noqa: C901
    path: Path, root: Path
) -> tuple[str, str, tuple[_ManifestTable, ...]]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ValueError("manifest is unreadable") from exc
    if len(raw) > _MAX_MANIFEST_BYTES:
        raise ValueError("manifest exceeds the byte limit")
    digest = hashlib.sha256(raw).hexdigest()
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("manifest is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("manifest must be an object")
    revision = payload.get("revision")
    tables = payload.get("tables")
    if not isinstance(revision, str) or not revision or len(revision) > 128:
        raise ValueError("manifest revision is invalid")
    if not isinstance(tables, dict) or not tables or len(tables) > _MAX_TABLES:
        raise ValueError("manifest tables are invalid or exceed the limit")
    parsed: list[_ManifestTable] = []
    for raw_identifier, raw_table in tables.items():
        if not isinstance(raw_identifier, str) or not isinstance(raw_table, dict):
            raise ValueError("manifest table entry is invalid")
        identifier = _parse_identifier(raw_identifier)
        raw_files = raw_table.get("files")
        raw_schema = raw_table.get("schema_ipc")
        raw_field_ids = raw_table.get("field_ids", [])
        if (
            not isinstance(raw_files, list)
            or not raw_files
            or len(raw_files) > _MAX_FILES_PER_TABLE
            or any(not isinstance(item, str) for item in raw_files)
        ):
            raise ValueError("manifest table files are invalid")
        files = tuple(
            _safe_child(root, _required_path(item, "table file"))
            .relative_to(root)
            .as_posix()
            for item in raw_files
        )
        if not isinstance(raw_schema, str) or not raw_schema:
            raise ValueError("manifest table schema is missing")
        try:
            schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(raw_schema)))
        except Exception as exc:
            raise ValueError("manifest table schema is invalid") from exc
        if not isinstance(raw_field_ids, list) or any(
            not isinstance(item, str) or not item for item in raw_field_ids
        ):
            raise ValueError("manifest field IDs are invalid")
        if len(raw_field_ids) != len(schema):
            raise ValueError("manifest field IDs must cover every top-level field")
        for file_path in files:
            _safe_child(root, Path(file_path))
        parsed.append(_ManifestTable(identifier, files, schema, tuple(raw_field_ids)))
    return revision, digest, tuple(parsed)


def _parse_identifier(raw: str) -> TableIdentifier:
    parts = tuple(raw.split("."))
    if len(parts) < 1 or any(not part for part in parts):
        raise ValueError("manifest table identifier is invalid")
    return TableIdentifier(namespace=parts[:-1], name=parts[-1])


def _identifier_key(identifier: TableIdentifier) -> str:
    return ".".join((*identifier.namespace, identifier.name))


def _schema_identities(
    schema: pa.Schema,
    field_ids: tuple[str, ...],
) -> tuple[tuple[str, str], ...]:
    """Derive deterministic schema-scoped IDs for nested paths."""

    if len(field_ids) != len(schema):
        raise ValueError("manifest field IDs must cover every top-level field")
    identities: list[tuple[str, str]] = []

    def visit(field: pa.Field, path: tuple[str, ...], anchor: str) -> None:
        path_text = ".".join(path)
        field_id = anchor if len(path) == 1 else "synthetic:" + hashlib.sha256(
            f"{anchor}:{path_text}".encode()
        ).hexdigest()[:32]
        identities.append((path_text, field_id))
        type_ = field.type
        if pa.types.is_struct(type_):
            for child in type_:
                visit(child, (*path, child.name), field_id)
        elif (
            pa.types.is_list(type_)
            or pa.types.is_large_list(type_)
            or pa.types.is_fixed_size_list(type_)
        ):
            # Keep collection paths identical to the core FieldPath contract.
            # The element marker is semantic, so list width/physical encoding
            # must not create a second identity vocabulary.
            visit(type_.value_field, (*path, "$element"), field_id)
        elif pa.types.is_map(type_):
            visit(type_.key_field, (*path, "$key"), field_id)
            visit(type_.item_field, (*path, "$value"), field_id)

    for field, anchor in zip(schema, field_ids, strict=True):
        visit(field, (field.name,), anchor)
    return tuple(identities)


def _required_root(value: object) -> Path:
    path = _required_path(value, "root")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ValueError("catalog root is unavailable") from exc
    if not resolved.is_dir():
        raise ValueError("catalog root must be a directory")
    return resolved


def _required_path(value: object, label: str) -> Path:
    if not isinstance(value, str) or not value or len(value) > _MAX_PATH_LENGTH:
        raise ValueError(f"{label} path is invalid")
    return Path(value)


def _safe_child(root: Path, candidate: Path) -> Path:
    if not candidate.is_absolute():
        candidate = root / candidate
    _reject_symlink_components(root, candidate)
    try:
        candidate.resolve(strict=False).relative_to(root)
    except ValueError as exc:
        raise ValueError("manifest path escapes the configured root") from exc
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise ValueError("manifest path is unavailable") from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError("manifest path escapes the configured root") from exc
    return resolved


def _reject_symlink_components(root: Path, candidate: Path) -> None:
    """Reject link indirection before resolving a governed path."""

    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("manifest path escapes the configured root") from exc
    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise ValueError("manifest path may not traverse a symlink")


def _check_context(context: ExecutionContext) -> None:
    if context.deadline.tzinfo is None or context.deadline.utcoffset() is None:
        raise ValueError("execution deadline must be timezone-aware")
    if context.deadline <= datetime.now(context.deadline.tzinfo):
        raise TimeoutError("execution context deadline has expired")
    if context.cancel_check is not None and context.cancel_check():
        raise RuntimeError("execution context was cancelled")
