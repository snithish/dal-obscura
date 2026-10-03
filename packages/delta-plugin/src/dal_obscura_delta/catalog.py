"""Explicit, bounded Delta table directory catalog; no filesystem discovery walk."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from dal_obscura_plugin_api import (
    CatalogConfig,
    DiscoveryPage,
    ExecutionContext,
    PluginDescriptor,
    TableHandle,
    TableIdentifier,
)
from dal_obscura_plugin_api.storage import StorageRoot

from dal_obscura_delta.snapshot import open_snapshot

CATALOG_DESCRIPTOR = PluginDescriptor(
    kind="catalog",
    plugin_id="delta.directory",
    api_version="2",
    config_version=1,
    distribution="dal-obscura-delta",
    version="0.2.0",
    capabilities=frozenset(
        {"nested_schema", "snapshot_reads", "splittable_scan", "delete_files", "cancellation"}
    ),
    output_formats=frozenset({"delta"}),
    handle_versions=frozenset({1}),
    display_name="Delta table directory",
    config_schema={
        "fields": [
            {"name": "root", "type": "string", "required": True},
            {"name": "tables_path", "type": "string", "required": True},
        ]
    },
)


class DeltaDirectoryCatalog:
    descriptor = CATALOG_DESCRIPTOR

    def __init__(self, config: CatalogConfig, context: ExecutionContext):
        context.check_active()
        if config.plugin_id != self.descriptor.plugin_id or set(config.options) != {
            "root",
            "tables_path",
        }:
            raise ValueError("Delta catalog requires root and tables_path")
        root, tables_path = config.options["root"], config.options["tables_path"]
        if not isinstance(root, str) or not isinstance(tables_path, str):
            raise ValueError("Invalid Delta catalog configuration")
        self._storage = StorageRoot(root)
        self._root = self._storage.uri
        registry_path = self._storage.member(tables_path)
        raw = self._storage.read(registry_path, limit=1_048_576)
        if len(raw) > 1_048_576:
            raise ValueError("Delta table registry exceeds the byte budget")
        entries = json.loads(raw)
        if not isinstance(entries, list) or len(entries) > 128:
            raise ValueError("Delta table registry must contain at most 128 entries")
        tables = {}
        for entry in entries:
            if not isinstance(entry, Mapping) or set(entry) != {"namespace", "name", "path"}:
                raise ValueError("Invalid Delta table entry")
            entry = cast(Mapping[str, Any], entry)
            identifier = TableIdentifier(entry["namespace"], entry["name"])
            path = entry["path"]
            if not isinstance(path, str) or Path(path).is_absolute() or "://" in path:
                raise ValueError("Delta table path must be relative to root")
            if identifier in tables:
                raise ValueError("Duplicate Delta table identifier")
            tables[identifier] = self._storage.member(path)
        self._tables = tables
        self._identifiers = tuple(sorted(tables, key=lambda i: (*i.namespace, i.name)))
        self._config = config

    def list_namespaces(self, context: ExecutionContext, namespace=()):
        context.check_active()
        prefix = tuple(namespace)
        return tuple(
            sorted(
                {
                    i.namespace[: len(prefix) + 1]
                    for i in self._identifiers
                    if i.namespace[: len(prefix)] == prefix and len(i.namespace) > len(prefix)
                }
            )
        )

    def list_tables(self, context: ExecutionContext, *, continuation=None, limit=100):
        context.check_active()
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("Delta discovery page size must be between 1 and 500")
        offset = 0
        if continuation is not None:
            if (
                not isinstance(continuation, str)
                or not continuation.isascii()
                or not continuation.isdecimal()
            ):
                raise ValueError("Invalid Delta discovery continuation")
            offset = int(continuation)
            if str(offset) != continuation or not 0 < offset < len(self._identifiers):
                raise ValueError("Invalid Delta discovery continuation")
        end = offset + limit
        return DiscoveryPage(
            self._identifiers[offset:end], str(end) if end < len(self._identifiers) else None
        )

    def resolve_table(self, identifier: TableIdentifier, context: ExecutionContext) -> TableHandle:
        context.check_active()
        if identifier not in self._tables:
            raise ValueError("Unknown Delta table")
        member = self._tables[identifier]
        path = self._storage.location(member)
        table = open_snapshot(StorageRoot(path), context)
        version, table_id = table.version(), table.metadata().id
        return TableHandle(
            self.descriptor.plugin_id,
            self._config.instance_id,
            self._config.revision,
            identifier,
            "delta",
            1,
            f"{table_id}@{version}",
            {"root": str(self._root), "path": str(path), "version": version, "table_id": table_id},
        )

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def delta_catalog_factory(
    config: CatalogConfig, context: ExecutionContext
) -> DeltaDirectoryCatalog:
    return DeltaDirectoryCatalog(config, context)
