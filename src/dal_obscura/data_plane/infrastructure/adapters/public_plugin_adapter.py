"""Bridge the public plugin SDK to the governed application ports.

The bridge is deliberately small: core still owns authorization, row-filter
reapplication, masking, ticket serialization, and output validation. Public
plugin tasks are carried inside the existing trusted ``ScanTask`` serializer;
the adapter stores only a resolved handle, bounded task data, and a factory
reference. Request contexts and live plugin instances are recreated at use time.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Any, cast
from uuid import uuid4

import pyarrow as pa
from dal_obscura_plugin_api import CatalogConfig as PublicCatalogConfig
from dal_obscura_plugin_api import CatalogPlugin as PublicCatalogPlugin
from dal_obscura_plugin_api import (
    DiscoveryPage,
    ExecutionContext,
    PluginDescriptor,
    SchemaDescriptor,
    TableFormatPlugin,
    TableHandle,
    TableIdentifier,
)

from dal_obscura.common.catalog.ports import CatalogPlugin as GovernedCatalogPlugin
from dal_obscura.common.catalog.ports import CatalogTableListing, TableFormat
from dal_obscura.common.query_planning.field_paths import (
    FieldPath,
    FieldSegment,
    parse_field_path,
    resolve_schema_path,
)
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.schema_bounds import validate_arrow_schema_bounds
from dal_obscura.common.schema_identity import schema_has_stable_ids
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer

MAX_PLUGIN_TASK_BYTES = 16 * 1024 * 1024
MAX_PLUGIN_DISCOVERY_PAGES = 128
MAX_PLUGIN_DISCOVERY_PAGE_ENTRIES = 500
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
        context = _context()
        plugin = self._open(context)
        try:
            descriptor = plugin.schema(self.handle, context)
            _ensure_context_active(context)
            _validate_schema_descriptor(descriptor, self.handle)
            return descriptor.arrow_schema
        finally:
            _close_plugin_preserving_error(plugin)

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        if max_tickets <= 0:
            raise ValueError("max_tickets must be positive")
        context = _context()
        plugin = self._open(context)
        try:
            descriptor = plugin.schema(self.handle, context)
            _ensure_context_active(context)
            _validate_schema_descriptor(descriptor, self.handle)
            row_filter = None
            if (
                request.row_filter is not None
                and "filter_pushdown" in plugin.descriptor.capabilities
            ):
                row_filter = request.row_filter.expression.sql(dialect="duckdb")
            output_schema = _projected_schema(descriptor.arrow_schema, request.columns)
            planned = plugin.plan(
                self.handle,
                descriptor,
                context,
                projection=output_schema.names,
                row_filter=row_filter,
                max_tasks=max_tickets,
            )
            _ensure_context_active(context)
            tasks: list[object] = []
            for task in planned:
                _ensure_context_active(context)
                _validate_task_payload(task)
                tasks.append(task)
                if len(tasks) > max_tickets:
                    raise ValueError("Plugin returned more tasks than requested")
            _ensure_context_active(context)
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
        finally:
            _close_plugin_preserving_error(plugin)

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, PublicPluginPartition):
            raise TypeError("Public plugin format requires a PublicPluginPartition")
        if partition.handle != self.handle or partition.format_factory != self.format_factory:
            raise ValueError("Public plugin partition does not match its format")

        def execute_batches() -> Iterable[pa.RecordBatch]:
            context = _context()
            plugin = self._open(context)
            try:
                output_schema, batches = plugin.execute(partition.task, context)
                try:
                    _ensure_context_active(context)
                    if output_schema != partition.schema:
                        raise ValueError("Public plugin changed the declared output schema")
                    yield from _checked_plugin_batches(batches, output_schema, context)
                finally:
                    _close_plugin_preserving_error(batches)
            finally:
                _close_plugin_preserving_error(plugin)

        return partition.schema, execute_batches()

    def _open(self, context: ExecutionContext | None = None) -> TableFormatPlugin:
        context = context or _context()
        _ensure_context_active(context)
        plugin = self.format_factory(self.handle, context)
        try:
            _ensure_context_active(context)
            if not all(
                callable(getattr(plugin, name, None))
                for name in ("schema", "plan", "execute", "close")
            ):
                raise ValueError("Public format factory returned an invalid plugin")
            descriptor = getattr(plugin, "descriptor", None)
            if not isinstance(descriptor, PluginDescriptor) or (
                getattr(descriptor, "kind", None) != "table_format"
                or getattr(descriptor, "plugin_id", None) != self.format
                or self.handle.handle_version not in descriptor.handle_versions
            ):
                raise ValueError("Public format factory returned a mismatched descriptor")
        except Exception:
            _close_plugin_preserving_error(plugin)
            raise
        return plugin


class PublicPluginCatalogAdapter(GovernedCatalogPlugin):
    """Adapts one public catalog factory to the existing governed catalog port."""

    def __init__(
        self,
        name: str,
        options: dict[str, Any],
        catalog_plugin_id: str,
        catalog_factory: PublicCatalogFactory,
        format_factory_loader: Callable[[str], object],
        path_enforcer: PathRuleEnforcer | None = None,
        revision: int = 0,
    ) -> None:
        self._name = name
        self._catalog_plugin_id = catalog_plugin_id
        self._catalog_revision = revision
        self._catalog_factory = catalog_factory
        self._format_factory_loader = format_factory_loader
        self._path_enforcer = path_enforcer
        public_config = PublicCatalogConfig(
            plugin_id=catalog_plugin_id, instance_id=name, revision=revision, options=dict(options)
        )
        context = _context()
        catalog = catalog_factory(public_config, context)
        try:
            if not all(
                callable(getattr(catalog, name, None))
                for name in (
                    "validate_config",
                    "list_namespaces",
                    "list_tables",
                    "resolve_table",
                    "close",
                )
            ):
                raise ValueError("Public catalog factory returned an invalid plugin")
            _ensure_context_active(context)
            catalog.validate_config(context)
            _ensure_context_active(context)
            descriptor = getattr(catalog, "descriptor", None)
            if not isinstance(descriptor, PluginDescriptor) or (
                getattr(descriptor, "kind", None) != "catalog"
                or getattr(descriptor, "plugin_id", None) != self._catalog_plugin_id
            ):
                raise ValueError("Public catalog factory returned a mismatched descriptor")
        except Exception:
            _close_plugin_preserving_error(catalog)
            raise
        self._catalog = catalog
        self._closed = False

    @property
    def name(self) -> str:
        return self._name

    def resolve_table(self, target: str) -> TableFormat:
        self._ensure_open()
        identifier = _table_identifier(target)
        context = _context()
        handle = self._catalog.resolve_table(identifier, context)
        _ensure_context_active(context)
        if not isinstance(handle, TableHandle):
            raise ValueError("Public catalog returned an invalid table handle")
        if (
            handle.catalog_plugin_id != self._catalog_plugin_id
            or handle.catalog_instance_id != self._name
            or handle.catalog_revision != self._catalog_revision
            or handle.identifier != identifier
        ):
            raise ValueError("Public catalog returned a mismatched table handle identity")
        catalog_descriptor = getattr(self._catalog, "descriptor", None)
        if not isinstance(catalog_descriptor, PluginDescriptor) or (
            handle.format_plugin_id not in getattr(catalog_descriptor, "output_formats", ())
            or handle.handle_version not in getattr(catalog_descriptor, "handle_versions", ())
        ):
            raise ValueError("Public catalog returned an undeclared table-format handle")
        if self._path_enforcer is not None:
            from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
                _nested_strings,
            )

            for location in _nested_strings(dict(handle.metadata)):
                if "://" in location or location.startswith("/"):
                    self._path_enforcer.check(location)
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
        self._ensure_open()
        continuation: str | None = None
        seen_tokens: set[str] = set()
        listings: list[CatalogTableListing] = []
        for _ in range(MAX_PLUGIN_DISCOVERY_PAGES):
            context = _context()
            page = self._catalog.list_tables(context, continuation=continuation, limit=500)
            _ensure_context_active(context)
            entries = _validated_page_entries(page)
            for identifier in entries:
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
            if (
                not isinstance(token, str)
                or not token
                or len(token) > 4_096
                or any(ord(char) < 0x20 or ord(char) == 0x7F for char in token)
            ):
                raise ValueError("Public catalog returned an invalid continuation token")
            if token in seen_tokens:
                raise ValueError("Public catalog returned a repeated continuation token")
            seen_tokens.add(token)
            continuation = token
        else:
            raise ValueError("Public catalog discovery exceeded the page limit")
        if len({item.name for item in listings}) != len(listings):
            raise ValueError("Public catalog returned duplicate table identities")
        return listings

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        _close_plugin(self._catalog)

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("Public catalog adapter is closed")


def _context() -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(seconds=PLUGIN_OPERATION_TIMEOUT_SECONDS),
        correlation_id=f"plugin-{uuid4().hex}",
    )


def _table_identifier(target: str) -> TableIdentifier:
    if not isinstance(target, str) or not target.strip():
        raise ValueError("table target must be non-empty")
    segments = parse_field_path(target).segments
    if any(not isinstance(segment, FieldSegment) for segment in segments):
        raise ValueError("Table identifiers cannot contain collection segments")
    parts = tuple(cast(FieldSegment, segment).name for segment in segments)
    return TableIdentifier(namespace=parts[:-1], name=parts[-1])


def _identifier_name(identifier: TableIdentifier) -> str:
    return FieldPath(
        tuple(FieldSegment(part) for part in (*identifier.namespace, identifier.name))
    ).to_human()


def _validated_page_entries(page: object) -> tuple[TableIdentifier, ...]:
    if not isinstance(page, DiscoveryPage):
        raise ValueError("Public catalog returned an invalid discovery page")
    entries = page.entries
    if not isinstance(entries, tuple):
        raise ValueError("Public catalog returned an invalid discovery page")
    if len(entries) > MAX_PLUGIN_DISCOVERY_PAGE_ENTRIES:
        raise ValueError("Public catalog returned too many page entries")
    return entries


def _ensure_context_active(context: ExecutionContext) -> None:
    if context.deadline <= datetime.now(timezone.utc):
        raise ValueError("Public plugin operation deadline expired")
    if context.cancel_check is not None and context.cancel_check():
        raise ValueError("Public plugin operation was cancelled")


def _close_plugin(plugin: object) -> None:
    close = getattr(plugin, "close", None)
    if callable(close):
        close()


def _close_plugin_preserving_error(plugin: object) -> None:
    """Close a provider without masking an active stream error."""

    active_error = sys.exc_info()[1]
    try:
        _close_plugin(plugin)
    except Exception:
        if active_error is None:
            raise


def _validate_schema_descriptor(descriptor: SchemaDescriptor, handle: TableHandle) -> None:
    if not isinstance(descriptor, SchemaDescriptor):
        raise ValueError("Public plugin returned an invalid schema descriptor")
    if handle.snapshot_id is not None and descriptor.snapshot_id != handle.snapshot_id:
        raise ValueError("Public plugin schema does not match the catalog snapshot")
    validate_arrow_schema_bounds(descriptor.arrow_schema)
    serialized = descriptor.arrow_schema.serialize().size
    if serialized > MAX_PLUGIN_TASK_BYTES:
        raise ValueError("Public plugin schema exceeds the byte limit")
    if descriptor.stable_ids and not schema_has_stable_ids(descriptor.arrow_schema):
        raise ValueError("Public plugin claimed stable IDs for a schema without provider IDs")


def _projected_schema(schema: pa.Schema, columns: list[str]) -> pa.Schema:
    if not columns or columns == ["*"]:
        return schema
    names: list[str] = []
    for column in columns:
        path = parse_field_path(column)
        resolve_schema_path(schema, path)
        top_level = cast(FieldSegment, path.segments[0]).name
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
    batches: Iterable[pa.RecordBatch], schema: pa.Schema, context: ExecutionContext
) -> Iterable[pa.RecordBatch]:
    """Validate each lazy batch before it reaches DuckDB or Flight output."""

    def checked() -> Iterable[pa.RecordBatch]:
        iterator = iter(batches)
        try:
            while True:
                _ensure_context_active(context)
                try:
                    batch = next(iterator)
                except StopIteration:
                    return
                _ensure_context_active(context)
                if not isinstance(batch, pa.RecordBatch):
                    raise ValueError("Public plugin returned a non-Arrow batch")
                if batch.schema != schema:
                    raise ValueError("Public plugin batch schema differs from the declared schema")
                if max(batch.nbytes, batch.get_total_buffer_size()) > MAX_PLUGIN_TASK_BYTES:
                    raise ValueError("Public plugin batch exceeds the byte limit")
                yield batch
        finally:
            if iterator is not batches:
                _close_plugin_preserving_error(iterator)

    return checked()
