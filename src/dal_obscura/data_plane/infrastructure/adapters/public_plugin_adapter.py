"""Bridge the public plugin SDK to the legacy governed table-format ports.

The bridge is deliberately small: core still owns authorization, row-filter
reapplication, masking, ticket serialization, and output validation. Public
plugin tasks are carried inside the existing trusted ``ScanTask`` serializer;
the adapter stores only a resolved handle, bounded task data, and a factory
reference. Request contexts and live plugin instances are recreated at use time.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Any, cast
from uuid import uuid4

import pyarrow as pa
from dal_obscura_plugin_api import (
    CatalogConfig as PublicCatalogConfig,
)
from dal_obscura_plugin_api import (
    CatalogPlugin as PublicCatalogPlugin,
)
from dal_obscura_plugin_api import (
    ExecutionContext,
    SchemaDescriptor,
    TableFormatPlugin,
    TableHandle,
    TableIdentifier,
)

from dal_obscura.common.catalog.ports import (
    CatalogPlugin as LegacyCatalogPlugin,
)
from dal_obscura.common.catalog.ports import (
    CatalogTableListing,
    TableFormat,
)
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer

MAX_PLUGIN_TASK_BYTES = 16 * 1024 * 1024
MAX_PLUGIN_DISCOVERY_PAGES = 128
MAX_PLUGIN_DISCOVERY_ENTRIES = 10_000
PLUGIN_OPERATION_TIMEOUT_SECONDS = 10

PublicCatalogFactory = Callable[[PublicCatalogConfig, ExecutionContext], PublicCatalogPlugin]
PublicFormatFactory = Callable[[TableHandle, ExecutionContext], TableFormatPlugin]


@dataclass(frozen=True, kw_only=True)
class PublicPluginPartition(InputPartition):
    """Serialized-ticket-safe public plugin task and its resolved handle."""

    task: object
    handle: TableHandle
    format_factory: PublicFormatFactory
    schema: pa.Schema


@dataclass(frozen=True, kw_only=True)
class PublicPluginTableFormat(TableFormat):
    """Executes one public SDK format through the unchanged core ScanTask path."""

    format_factory: PublicFormatFactory
    handle: TableHandle
    format: str

    def get_schema(self) -> pa.Schema:
        plugin = self._open()
        descriptor = plugin.schema(self.handle, _context())
        _validate_schema_descriptor(descriptor)
        return descriptor.arrow_schema

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        if max_tickets <= 0:
            raise ValueError("max_tickets must be positive")
        plugin = self._open()
        descriptor = plugin.schema(self.handle, _context())
        _validate_schema_descriptor(descriptor)
        row_filter = None
        if request.row_filter is not None:
            row_filter = request.row_filter.expression.sql(dialect="duckdb")
        planned = plugin.plan(
            self.handle,
            descriptor,
            _context(),
            projection=request.columns,
            row_filter=row_filter,
            max_tasks=max_tickets,
        )
        output_schema = _projected_schema(descriptor.arrow_schema, request.columns)
        tasks: list[object] = []
        for task in planned:
            _validate_task_payload(task)
            tasks.append(task)
            if len(tasks) > max_tickets:
                raise ValueError("Plugin returned more tasks than requested")
        if not tasks:
            # Preserve schema-only/empty result behavior through one empty task.
            tasks = [None]
        partitions = [
            PublicPluginPartition(
                task=task,
                handle=self.handle,
                format_factory=self.format_factory,
                schema=output_schema,
            )
            for task in tasks
        ]
        scan_tasks = [
            ScanTask(table_format=self, schema=descriptor.arrow_schema, partition=partition)
            for partition in partitions
        ]
        return Plan(
            schema=descriptor.arrow_schema,
            tasks=scan_tasks,
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, PublicPluginPartition):
            raise TypeError("Public plugin format requires a PublicPluginPartition")
        if partition.handle != self.handle or partition.format_factory != self.format_factory:
            raise ValueError("Public plugin partition does not match its format")
        plugin = self._open()
        output_schema, batches = plugin.execute(partition.task, _context())
        if output_schema != partition.schema:
            raise ValueError("Public plugin changed the declared output schema")
        return output_schema, _checked_plugin_batches(batches, output_schema)

    def _open(self) -> TableFormatPlugin:
        plugin = self.format_factory(self.handle, _context())
        if not all(callable(getattr(plugin, name, None)) for name in ("schema", "plan", "execute")):
            raise ValueError("Public format factory returned an invalid plugin")
        return plugin


class PublicPluginCatalogAdapter(LegacyCatalogPlugin):
    """Adapts one public catalog factory to the existing governed catalog port."""

    def __init__(
        self,
        name: str,
        options: dict[str, Any],
        catalog_plugin_id: str,
        catalog_factory: PublicCatalogFactory,
        format_factory_loader: Callable[[str], object],
        path_enforcer: PathRuleEnforcer | None = None,
    ) -> None:
        self._name = name
        self._catalog_plugin_id = catalog_plugin_id
        self._catalog_factory = catalog_factory
        self._format_factory_loader = format_factory_loader
        self._path_enforcer = path_enforcer
        public_config = PublicCatalogConfig(
            plugin_id=catalog_plugin_id,
            instance_id=name,
            revision=0,
            options=dict(options),
        )
        self._catalog = catalog_factory(public_config, _context())
        if not all(
            callable(getattr(self._catalog, name, None))
            for name in ("list_tables", "resolve_table")
        ):
            raise ValueError("Public catalog factory returned an invalid plugin")

    @property
    def name(self) -> str:
        return self._name

    def resolve_table(self, target: str) -> TableFormat:
        identifier = _legacy_identifier(target)
        handle = self._catalog.resolve_table(identifier, _context())
        if not isinstance(handle, TableHandle):
            raise ValueError("Public catalog returned an invalid table handle")
        if handle.format_plugin_id == "iceberg":
            from dal_obscura.data_plane.infrastructure.table_formats.iceberg import (
                IcebergTableFormat,
            )

            metadata_location = handle.metadata.get("metadata_location")
            if not isinstance(metadata_location, str) or not metadata_location:
                raise ValueError("Iceberg plugin handle is missing metadata_location")
            if self._path_enforcer is not None:
                self._path_enforcer.check(metadata_location)
            io_options = handle.metadata.get("io_options", {})
            if not isinstance(io_options, dict):
                raise ValueError("Iceberg plugin handle has invalid io_options")
            return IcebergTableFormat(
                catalog_name=self._name,
                table_name=target,
                metadata_location=metadata_location,
                io_options=cast(dict[str, object], io_options),
                path_enforcer=self._path_enforcer,
            )
        raw_factory = self._format_factory_loader(handle.format_plugin_id)
        if not callable(raw_factory):
            raise ValueError("Public format factory is not callable")
        format_factory = cast(PublicFormatFactory, raw_factory)
        return PublicPluginTableFormat(
            catalog_name=self._name,
            table_name=target,
            format=handle.format_plugin_id,
            format_factory=format_factory,
            handle=handle,
        )

    def list_tables(self) -> list[CatalogTableListing]:
        continuation: str | None = None
        seen_tokens: set[str] = set()
        listings: list[CatalogTableListing] = []
        for _ in range(MAX_PLUGIN_DISCOVERY_PAGES):
            page = self._catalog.list_tables(_context(), continuation=continuation, limit=500)
            if not hasattr(page, "entries") or not hasattr(page, "continuation"):
                raise ValueError("Public catalog returned an invalid discovery page")
            for identifier in page.entries:
                if not isinstance(identifier, TableIdentifier):
                    raise ValueError("Public catalog returned an invalid table identifier")
                listings.append(
                    CatalogTableListing(
                        name=_identifier_name(identifier),
                        provider_id=self._catalog_plugin_id,
                        table_identifier=_identifier_name(identifier),
                    )
                )
                if len(listings) > MAX_PLUGIN_DISCOVERY_ENTRIES:
                    raise ValueError("Public catalog discovery exceeded the entry limit")
            token = page.continuation
            if token is None:
                break
            if token in seen_tokens:
                raise ValueError("Public catalog returned a repeated continuation token")
            seen_tokens.add(token)
            continuation = token
        else:
            raise ValueError("Public catalog discovery exceeded the page limit")
        if len({item.name for item in listings}) != len(listings):
            raise ValueError("Public catalog returned duplicate table identities")
        return listings


def _context() -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(seconds=PLUGIN_OPERATION_TIMEOUT_SECONDS),
        correlation_id=f"plugin-{uuid4().hex}",
    )


def _legacy_identifier(target: str) -> TableIdentifier:
    if not isinstance(target, str) or not target.strip():
        raise ValueError("table target must be non-empty")
    parts = tuple(target.split("."))
    return TableIdentifier(namespace=parts[:-1], name=parts[-1])


def _identifier_name(identifier: TableIdentifier) -> str:
    return ".".join((*identifier.namespace, identifier.name))


def _validate_schema_descriptor(descriptor: SchemaDescriptor) -> None:
    if not isinstance(descriptor, SchemaDescriptor):
        raise ValueError("Public plugin returned an invalid schema descriptor")
    serialized = descriptor.arrow_schema.serialize().size
    if serialized > MAX_PLUGIN_TASK_BYTES:
        raise ValueError("Public plugin schema exceeds the byte limit")


def _projected_schema(schema: pa.Schema, columns: list[str]) -> pa.Schema:
    if not columns or columns == ["*"]:
        return schema
    names: list[str] = []
    for column in columns:
        top_level = column.split(".", 1)[0]
        if top_level not in schema.names:
            raise ValueError("Plugin projection contains an unknown field")
        if top_level not in names:
            names.append(top_level)
    return pa.schema([schema.field(name) for name in names], metadata=schema.metadata)


def _validate_task_payload(value: object) -> None:  # noqa: C901
    """Allow only bounded inert values inside trusted ticket task payloads."""

    nodes = 0
    string_bytes = 0
    seen: set[int] = set()

    def visit(item: object, depth: int) -> None:  # noqa: C901
        nonlocal nodes, string_bytes
        nodes += 1
        if nodes > 2_048 or depth > 12:
            raise ValueError("Public plugin task payload is too large")
        if item is None or isinstance(item, (bool, int)):
            return
        if isinstance(item, float):
            if not isfinite(item):
                raise ValueError("Public plugin task payload contains a non-finite number")
            return
        if isinstance(item, str):
            string_bytes += len(item.encode("utf-8"))
            if len(item) > 1_048_576 or string_bytes > MAX_PLUGIN_TASK_BYTES:
                raise ValueError("Public plugin task payload contains oversized strings")
            if any(ord(char) < 0x20 or ord(char) == 0x7F for char in item):
                raise ValueError("Public plugin task payload contains control characters")
            return
        if isinstance(item, Mapping):
            if len(item) > 128:
                raise ValueError("Public plugin task payload has too many keys")
            for key, child in item.items():
                if not isinstance(key, str) or not key or len(key) > 256:
                    raise ValueError("Public plugin task payload has invalid keys")
                visit(key, depth + 1)
                visit(child, depth + 1)
            return
        if isinstance(item, (list, tuple)):
            if len(item) > 128:
                raise ValueError("Public plugin task payload has too many items")
            for child in item:
                visit(child, depth + 1)
            return
        if is_dataclass(item) and not isinstance(item, type):
            params = getattr(type(item), "__dataclass_params__", None)
            if params is None or not params.frozen:
                raise ValueError("Public plugin task dataclasses must be frozen")
            marker = id(item)
            if marker in seen:
                raise ValueError("Public plugin task payload contains a cycle")
            seen.add(marker)
            for field in fields(item):
                visit(getattr(item, field.name), depth + 1)
            seen.remove(marker)
            return
        raise ValueError("Public plugin task payload must be inert JSON-like data")

    visit(value, 0)


def _checked_plugin_batches(
    batches: Iterable[pa.RecordBatch], schema: pa.Schema
) -> Iterable[pa.RecordBatch]:
    """Validate each lazy batch before it reaches DuckDB or Flight output."""

    def checked() -> Iterable[pa.RecordBatch]:
        for batch in batches:
            if not isinstance(batch, pa.RecordBatch):
                raise ValueError("Public plugin returned a non-Arrow batch")
            if batch.schema != schema:
                raise ValueError("Public plugin batch schema differs from the declared schema")
            if batch.nbytes > MAX_PLUGIN_TASK_BYTES:
                raise ValueError("Public plugin batch exceeds the byte limit")
            yield batch

    return checked()
