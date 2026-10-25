"""Explicit, bounded Delta table directory catalog; no filesystem discovery walk."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from types import TracebackType
from typing import Self, cast
from urllib.parse import urlsplit

from dal_obscura_plugin_api import (
    CatalogConfig,
    DiscoveryPage,
    ExecutionContext,
    TableHandle,
    TableIdentifier,
)
from dal_obscura_plugin_api.descriptors import load_packaged_descriptor
from dal_obscura_plugin_api.execution import active
from deltalake import DeltaTable

from dal_obscura_delta.snapshot import open_snapshot
from dal_obscura_delta.storage import DeltaStorage, storage_options

CATALOG_DESCRIPTOR = load_packaged_descriptor(
    "dal-obscura-delta", __package__, "catalog", "delta.directory"
)

LOCATION_DESCRIPTOR = load_packaged_descriptor(
    "dal-obscura-delta", __package__, "catalog", "delta.catalog"
)


class DeltaDirectoryCatalog:
    descriptor = CATALOG_DESCRIPTOR

    def __init__(self, config: CatalogConfig, context: ExecutionContext) -> None:
        context.check_active()
        if config.plugin_id != self.descriptor.plugin_id or set(config.options) != {
            "root",
            "tables_path",
        }:
            raise ValueError("Delta catalog requires root and tables_path")
        root, tables_path = config.options["root"], config.options["tables_path"]
        if not isinstance(root, str) or not isinstance(tables_path, str):
            raise ValueError("Invalid Delta catalog configuration")
        self._storage = DeltaStorage(root)
        registry_path = self._storage.member(tables_path)
        raw = self._storage.read(registry_path, limit=1_048_576)
        entries = json.loads(raw)
        if not isinstance(entries, list) or len(entries) > 128:
            raise ValueError("Delta table registry must contain at most 128 entries")
        tables = {}
        for entry in entries:
            if not isinstance(entry, Mapping) or set(entry) != {"namespace", "name", "path"}:
                raise ValueError("Invalid Delta table entry")
            entry = cast(Mapping[str, object], entry)
            namespace, name = entry["namespace"], entry["name"]
            if not isinstance(namespace, (list, tuple)) or not isinstance(name, str):
                raise ValueError("Invalid Delta table identifier")
            parts: list[str] = []
            for part in namespace:
                if not isinstance(part, str):
                    raise ValueError("Invalid Delta table namespace")
                parts.append(part)
            identifier = TableIdentifier(tuple(parts), name)
            path = entry["path"]
            if not isinstance(path, str) or Path(path).is_absolute() or "://" in path:
                raise ValueError("Delta table path must be relative to root")
            if identifier in tables:
                raise ValueError("Duplicate Delta table identifier")
            tables[identifier] = self._storage.member(path, exists=False)
        self._tables = tables
        self._identifiers = tuple(sorted(tables, key=lambda i: (*i.namespace, i.name)))
        self._config = config

    @active
    def list_namespaces(
        self, context: ExecutionContext, *, namespace: tuple[str, ...] = ()
    ) -> tuple[tuple[str, ...], ...]:
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

    @active
    def list_tables(
        self, context: ExecutionContext, *, continuation: str | None = None, limit: int = 100
    ) -> DiscoveryPage:
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

    @active
    def resolve_table(self, identifier: TableIdentifier, context: ExecutionContext) -> TableHandle:
        if identifier not in self._tables:
            raise ValueError("Unknown Delta table")
        path = self._location(identifier, context)
        table = open_snapshot(DeltaStorage(path), context)
        version, table_id = table.version(), table.metadata().id
        return TableHandle(
            self.descriptor.plugin_id,
            self._config.instance_id,
            self._config.revision,
            identifier,
            "delta",
            1,
            f"{table_id}@{version}",
            {"location": str(path), "version": version, "table_id": table_id},
        )

    @active
    def _location(self, identifier: TableIdentifier, context: ExecutionContext) -> str:
        return self._storage.location(self._tables[identifier])

    def close(self) -> None:
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


@active
def delta_catalog_factory(
    config: CatalogConfig, context: ExecutionContext
) -> DeltaDirectoryCatalog:
    return DeltaDirectoryCatalog(config, context)


class DeltaLocationCatalog(DeltaDirectoryCatalog):
    """Bounded catalog scope with native delta-rs location/Unity resolution.

    The official Python client exposes URI lookup, not catalog listing. Operators
    register the identifiers they want governed; no private catalog HTTP client.
    """

    descriptor = LOCATION_DESCRIPTOR

    def __init__(self, config: CatalogConfig, context: ExecutionContext) -> None:
        context.check_active()
        if config.plugin_id != self.descriptor.plugin_id or set(config.options) != {"tables"}:
            raise ValueError("Delta location catalog requires tables")
        entries = config.options["tables"]
        if not isinstance(entries, (tuple, list)) or len(entries) > 10_000:
            raise ValueError("Delta location catalog exceeds the table budget")
        tables = {}
        for entry in entries:
            if not isinstance(entry, Mapping) or set(entry) != {"namespace", "name", "location"}:
                raise ValueError("Invalid Delta catalog entry")
            entry = cast(Mapping[str, object], entry)
            namespace, name = entry["namespace"], entry["name"]
            if not isinstance(namespace, (list, tuple)) or not isinstance(name, str):
                raise ValueError("Invalid Delta table identifier")
            parts: list[str] = []
            for part in namespace:
                if not isinstance(part, str):
                    raise ValueError("Invalid Delta table namespace")
                parts.append(part)
            identifier = TableIdentifier(tuple(parts), name)
            location = entry["location"]
            if not isinstance(location, str) or not location or len(location) > 4096:
                raise ValueError("Invalid Delta table location")
            parsed = urlsplit(location)
            if (
                parsed.query
                or parsed.fragment
                or parsed.password
                or (parsed.username and parsed.scheme not in {"abfs", "abfss"})
            ):
                raise ValueError("Delta locations cannot contain credentials or queries")
            if identifier in tables:
                raise ValueError("Duplicate Delta table identifier")
            tables[identifier] = location
        self._tables = tables
        self._identifiers = tuple(sorted(tables, key=lambda i: (*i.namespace, i.name)))
        self._config = config

    @active
    def _location(self, identifier: TableIdentifier, context: ExecutionContext) -> str:
        location = self._tables[identifier]
        if location.startswith("uc://"):
            context.check_active()
            table = DeltaTable(location, without_files=True, storage_options=storage_options())
            context.check_active()
            return table.table_uri.rstrip("/")
        return DeltaStorage(location).uri


@active
def delta_location_catalog_factory(
    config: CatalogConfig, context: ExecutionContext
) -> DeltaLocationCatalog:
    return DeltaLocationCatalog(config, context)
