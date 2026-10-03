"""Operator-controlled immutable membership catalog for Parquet datasets."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import cast

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
from dal_obscura_plugin_api.storage import StorageRoot

_MAX_MANIFEST_BYTES = 1_048_576
_MAX_TABLES = 10_000
_MAX_FILES_PER_TABLE = 100_000

CATALOG_DESCRIPTOR = PluginDescriptor(
    kind="catalog",
    plugin_id="manifest",
    api_version="2",
    config_version=1,
    distribution="dal-obscura-manifest-parquet",
    version="0.2.0",
    capabilities=frozenset({"nested_schema", "splittable_scan"}),
    output_formats=frozenset({"parquet.dataset"}),
    handle_versions=frozenset({1}),
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


class ManifestCatalog(CatalogPlugin):
    """Catalog whose table membership and schema come from one pinned manifest."""

    descriptor = CATALOG_DESCRIPTOR

    def __init__(self, config: CatalogConfig, context: ExecutionContext) -> None:
        context.check_active()
        self._config = config
        options = dict(config.options)
        if set(options) != {"root", "manifest_path"}:
            raise ValueError("manifest options contain unsupported fields")
        self._storage = StorageRoot(options.get("root"))
        self._root = self._storage.uri
        self._manifest_path = self._storage.member(options.get("manifest_path"))
        self._revision, self._tables = _load_manifest(self._manifest_path, self._storage)

    def list_namespaces(
        self,
        context: ExecutionContext,
        *,
        namespace: tuple[str, ...] = (),
    ) -> tuple[tuple[str, ...], ...]:
        context.check_active()
        if namespace:
            return tuple(
                sorted(
                    {
                        item.identifier.namespace
                        for item in self._tables
                        if item.identifier.namespace[: len(namespace)] == namespace
                    }
                )
            )
        return tuple(sorted({item.identifier.namespace for item in self._tables}))

    def list_tables(
        self,
        context: ExecutionContext,
        *,
        continuation: str | None = None,
        limit: int,
    ) -> DiscoveryPage:
        context.check_active()
        if limit <= 0 or limit > _MAX_TABLES:
            raise ValueError("catalog page limit is invalid")
        tables = sorted(
            self._tables, key=lambda table: (*table.identifier.namespace, table.identifier.name)
        )
        start = 0
        if continuation is not None:
            identifiers = [_identifier_key(item.identifier) for item in tables]
            try:
                start = identifiers.index(continuation) + 1
            except ValueError as exc:
                raise ValueError("catalog continuation is invalid") from exc
        page = tables[start : start + limit]
        next_token = _identifier_key(page[-1].identifier) if start + limit < len(tables) else None
        return DiscoveryPage(tuple(item.identifier for item in page), next_token)

    def resolve_table(self, identifier: TableIdentifier, context: ExecutionContext) -> TableHandle:
        context.check_active()
        key = _identifier_key(identifier)
        table = next(
            (item for item in self._tables if _identifier_key(item.identifier) == key), None
        )
        if table is None:
            raise KeyError("table is not present in the operator manifest")
        metadata = {
            "root": str(self._root),
            "files": table.files,
            "schema_ipc": base64.b64encode(table.schema.serialize().to_pybytes()).decode("ascii"),
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
    path: str, root: StorageRoot
) -> tuple[str, tuple[_ManifestTable, ...]]:
    try:
        raw = root.read(path, limit=_MAX_MANIFEST_BYTES)
    except OSError as exc:
        raise ValueError("manifest is unreadable") from exc
    try:
        payload = json.loads(raw, object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("manifest is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("manifest must be an object")
    if set(payload) != {"revision", "tables"}:
        raise ValueError("manifest contains unsupported fields")
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
        if set(raw_table) - {"namespace", "name", "files", "schema_ipc"}:
            raise ValueError("manifest table contains unsupported fields")
        identifier = _parse_identifier_entry(raw_identifier, raw_table)
        raw_files = raw_table.get("files")
        raw_schema = raw_table.get("schema_ipc")
        if (
            not isinstance(raw_files, list)
            or not raw_files
            or len(raw_files) > _MAX_FILES_PER_TABLE
            or any(not isinstance(item, str) for item in raw_files)
        ):
            raise ValueError("manifest table files are invalid")
        files = tuple(root.relative(root.member(item)) for item in raw_files)
        if not isinstance(raw_schema, str) or not raw_schema:
            raise ValueError("manifest table schema is missing")
        try:
            schema = pa.ipc.read_schema(
                pa.BufferReader(base64.b64decode(raw_schema, validate=True))
            )
        except Exception as exc:
            raise ValueError("manifest table schema is invalid") from exc
        parsed.append(_ManifestTable(identifier, files, schema))
    if len({_identifier_key(item.identifier) for item in parsed}) != len(parsed):
        raise ValueError("manifest contains duplicate table identities")
    return revision, tuple(parsed)


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate keys so table membership cannot be overwritten."""

    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _parse_identifier_entry(raw_identifier: str, raw_table: dict[str, object]) -> TableIdentifier:
    """Parse a manifest table's explicit namespace and name."""

    raw_namespace = raw_table.get("namespace")
    raw_name = raw_table.get("name")
    if not isinstance(raw_namespace, list) or any(
        not isinstance(part, str) for part in raw_namespace
    ):
        raise ValueError("manifest table identifier requires a structured namespace and name")
    if not isinstance(raw_name, str) or not raw_name:
        raise ValueError("manifest table name is invalid")
    return TableIdentifier(
        namespace=tuple(cast(str, part) for part in raw_namespace), name=raw_name
    )


def _identifier_key(identifier: TableIdentifier) -> str:
    return json.dumps([*identifier.namespace, identifier.name], separators=(",", ":"))
