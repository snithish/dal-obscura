"""SDK adapters with explicit provider lifetimes and output validation.

Core owns authorization, filters, masks and ticket issuance. Factories remain in
process memory; scan envelopes capture only handles, schemas and inert task data.
"""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import cast

import pyarrow as pa
from dal_obscura_plugin_api import CatalogConfig as PublicCatalogConfig
from dal_obscura_plugin_api import (
    CatalogFactory,
    DiscoveryPage,
    ExecutionContext,
    PluginDescriptor,
    ScanRequest,
    SchemaDescriptor,
    TableFormatFactory,
    TableFormatPlugin,
    TableHandle,
    TableIdentifier,
)
from dal_obscura_plugin_api import (
    ScanTask as PluginScanTask,
)
from dal_obscura_plugin_api.execution import (
    close_preserving_error,
)
from dal_obscura_plugin_api.identifiers import identifier_name, table_identifier
from dal_obscura_plugin_api.options import PluginOptions
from dal_obscura_plugin_api.paths import (
    FieldSegment,
    parse_field_path,
    resolve_schema_path,
)
from dal_obscura_plugin_api.schema.bounds import (
    validate_arrow_schema_bounds,
    validate_native_schema,
)
from dal_obscura_plugin_api.schema.identity import schema_has_stable_ids
from dal_obscura_plugin_api.storage import PathRuleEnforcer

from dal_obscura.application.reads.sources import (
    CatalogTableListing,
    Plan,
    ScanTask,
    Source,
    new_execution_context,
)
from dal_obscura.kernel.plans.request import PlanRequest

MAX_PLUGIN_BATCH_BYTES = 16 * 1024 * 1024
MAX_PLUGIN_DISCOVERY_PAGES = 128
MAX_PLUGIN_DISCOVERY_PAGE_ENTRIES = 500
MAX_PLUGIN_DISCOVERY_ENTRIES = 10_000


@dataclass(frozen=True, kw_only=True)
class PublicPluginPartition:
    """In-process SDK task and exact output schema; the source owns its handle."""

    task: PluginScanTask
    schema: pa.Schema


@dataclass(frozen=True, kw_only=True)
class PublicPluginTableFormat(Source):
    """Executes one public SDK format through the governed core ScanTask path."""

    catalog_name: str
    table_name: str
    format_factory: TableFormatFactory
    handle: TableHandle
    format: str
    path_roots: tuple[str, ...] = ()
    options: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "options", PluginOptions({self.format: self.options}).for_plugin(self.format)
        )

    @contextmanager
    def open(self, *, context: ExecutionContext | None = None) -> Generator[_BoundSource]:
        context = self._execution_context(context or new_execution_context())
        plugin = self._open(context)
        try:
            descriptor = plugin.schema(context)
            context.check_active()
            _validate_schema_descriptor(descriptor, self.handle)
            yield _BoundSource(
                catalog_name=self.catalog_name,
                table_name=self.table_name,
                format=self.format,
                source=self,
                plugin=plugin,
                descriptor=descriptor,
                context=context,
            )
        finally:
            close_preserving_error(plugin)

    def get_schema(self) -> pa.Schema:
        with self.open() as source:
            return source.get_schema()

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        with self.open() as source:
            return source.plan(request, max_tickets)

    def _plan(
        self,
        plugin: TableFormatPlugin,
        descriptor: SchemaDescriptor,
        context: ExecutionContext,
        request: PlanRequest,
        max_tickets: int,
    ) -> Plan:
        if max_tickets <= 0:
            raise ValueError("max_tickets must be positive")
        row_filter = None
        if request.row_filter is not None and "filter_pushdown" in plugin.descriptor.capabilities:
            row_filter = request.row_filter.expression.sql(dialect="duckdb")
        output_schema = _projected_schema(descriptor.arrow_schema, request.columns)
        validate_native_schema(output_schema)
        context.check_active()
        planned = plugin.plan(ScanRequest(output_schema, max_tickets, row_filter), context)
        tasks: list[PluginScanTask] = []
        iterator = None
        try:
            iterator = iter(planned)
            while True:
                context.check_active()
                try:
                    task = next(iterator)
                except StopIteration:
                    break
                context.check_active()
                if not isinstance(task, PluginScanTask):
                    raise ValueError("Plugin must return immutable ScanTask values")
                tasks.append(task)
                if len(tasks) > max_tickets:
                    raise ValueError("Plugin returned more tasks than requested")
        finally:
            try:
                if iterator is not None and iterator is not planned:
                    close_preserving_error(iterator)
            finally:
                close_preserving_error(planned)
        context.check_active()
        partitions = [
            PublicPluginPartition(
                task=task,
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

    def execute(
        self, partition: object, *, context: ExecutionContext
    ) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, PublicPluginPartition):
            raise TypeError("Public plugin format requires a PublicPluginPartition")

        def execute_batches() -> Iterable[pa.RecordBatch]:
            operation = self._execution_context(context)
            plugin = self._open(operation)
            try:
                output_schema, batches = plugin.execute(partition.task, operation)
                try:
                    operation.check_active()
                    if not output_schema.equals(partition.schema, check_metadata=True):
                        raise ValueError("Public plugin changed the declared output schema")
                    yield from _checked_plugin_batches(batches, output_schema, operation)
                finally:
                    close_preserving_error(batches)
            finally:
                close_preserving_error(plugin)

        return partition.schema, execute_batches()

    def _execution_context(self, context: ExecutionContext) -> ExecutionContext:
        return replace(
            context,
            options=self.options,
            check_path=PathRuleEnforcer([{"root": root} for root in self.path_roots]).check,
        )

    def _open(self, context: ExecutionContext) -> TableFormatPlugin:
        context.check_active()
        plugin = self.format_factory(self.handle, context)
        try:
            context.check_active()
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
        except BaseException:
            close_preserving_error(plugin)
            raise
        return plugin


@dataclass(frozen=True, kw_only=True)
class _BoundSource(Source):
    """An admitted provider and descriptor owned by one planning context."""

    catalog_name: str
    table_name: str
    format: str
    source: PublicPluginTableFormat
    plugin: TableFormatPlugin
    descriptor: SchemaDescriptor
    context: ExecutionContext

    def get_schema(self) -> pa.Schema:
        return self.descriptor.arrow_schema

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        return self.source._plan(self.plugin, self.descriptor, self.context, request, max_tickets)

    def execute(
        self, partition: object, *, context: ExecutionContext
    ) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        return self.source.execute(partition, context=context)


class PublicPluginCatalogAdapter:
    """Adapts one public catalog factory to the existing governed catalog port."""

    def __init__(
        self,
        name: str,
        options: Mapping[str, object],
        catalog_plugin_id: str,
        catalog_factory: CatalogFactory,
        format_factory_loader: Callable[[str], TableFormatFactory],
        path_enforcer: PathRuleEnforcer | None = None,
        revision: int = 0,
        format_options: Callable[[str], Mapping[str, object]] | None = None,
        *,
        context: ExecutionContext | None = None,
    ) -> None:
        self._name = name
        self._catalog_plugin_id = catalog_plugin_id
        self._catalog_revision = revision
        self._format_factory_loader = format_factory_loader
        self._format_options = format_options or (lambda _: {})
        self._path_enforcer = path_enforcer
        public_config = PublicCatalogConfig(
            plugin_id=catalog_plugin_id, instance_id=name, revision=revision, options=dict(options)
        )
        context = context or new_execution_context()
        context.check_active()
        catalog = catalog_factory(public_config, context)
        try:
            if not all(
                callable(getattr(catalog, name, None))
                for name in (
                    "list_namespaces",
                    "list_tables",
                    "resolve_table",
                    "close",
                )
            ):
                raise ValueError("Public catalog factory returned an invalid plugin")
            context.check_active()
            descriptor = getattr(catalog, "descriptor", None)
            if not isinstance(descriptor, PluginDescriptor) or (
                getattr(descriptor, "kind", None) != "catalog"
                or getattr(descriptor, "plugin_id", None) != self._catalog_plugin_id
            ):
                raise ValueError("Public catalog factory returned a mismatched descriptor")
        except BaseException:
            close_preserving_error(catalog)
            raise
        self._catalog = catalog
        self._closed = False

    @property
    def name(self) -> str:
        return self._name

    def resolve_table(
        self, target: str, *, context: ExecutionContext | None = None
    ) -> PublicPluginTableFormat:
        self._ensure_open()
        identifier = table_identifier(target)
        context = context or new_execution_context()
        context.check_active()
        handle = self._catalog.resolve_table(identifier, context)
        context.check_active()
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
            for location in _nested_strings(dict(handle.metadata)):
                if "://" in location or location.startswith("/"):
                    self._path_enforcer.check(location)
        format_factory = self._format_factory_loader(handle.format_plugin_id)
        return PublicPluginTableFormat(
            catalog_name=self._name,
            table_name=target,
            format=handle.format_plugin_id,
            format_factory=format_factory,
            handle=handle,
            path_roots=self._path_enforcer.roots if self._path_enforcer else (),
            options=self._format_options(handle.format_plugin_id),
        )

    def list_tables(self) -> list[CatalogTableListing]:
        self._ensure_open()
        continuation: str | None = None
        seen_tokens: set[str] = set()
        listings: list[CatalogTableListing] = []
        context = new_execution_context()
        for _ in range(MAX_PLUGIN_DISCOVERY_PAGES):
            page = self._catalog.list_tables(context, continuation=continuation, limit=500)
            context.check_active()
            entries = _validated_page_entries(page)
            for identifier in entries:
                if not isinstance(identifier, TableIdentifier):
                    raise ValueError("Public catalog returned an invalid table identifier")
                listings.append(
                    CatalogTableListing(
                        name=identifier_name(identifier),
                        provider_id=self._catalog_plugin_id,
                        table_identifier=identifier_name(identifier),
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
        close_preserving_error(self._catalog)

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("Public catalog adapter is closed")


def _validated_page_entries(page: object) -> tuple[TableIdentifier, ...]:
    if not isinstance(page, DiscoveryPage):
        raise ValueError("Public catalog returned an invalid discovery page")
    entries = page.entries
    if not isinstance(entries, tuple):
        raise ValueError("Public catalog returned an invalid discovery page")
    if len(entries) > MAX_PLUGIN_DISCOVERY_PAGE_ENTRIES:
        raise ValueError("Public catalog returned too many page entries")
    return entries


def _validate_schema_descriptor(descriptor: SchemaDescriptor, handle: TableHandle) -> None:
    if not isinstance(descriptor, SchemaDescriptor):
        raise ValueError("Public plugin returned an invalid schema descriptor")
    if handle.snapshot_id is not None and descriptor.snapshot_id != handle.snapshot_id:
        raise ValueError("Public plugin schema does not match the catalog snapshot")
    validate_arrow_schema_bounds(descriptor.arrow_schema)
    if descriptor.stable_ids and not schema_has_stable_ids(descriptor.arrow_schema):
        raise ValueError("Public plugin claimed stable IDs for a schema without provider IDs")


def _projected_schema(schema: pa.Schema, columns: Sequence[str]) -> pa.Schema:
    if not columns or tuple(columns) == ("*",):
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


def _checked_plugin_batches(
    batches: Iterable[pa.RecordBatch], schema: pa.Schema, context: ExecutionContext
) -> Iterable[pa.RecordBatch]:
    """Validate each lazy batch before it reaches DuckDB or Flight output."""

    def checked() -> Iterable[pa.RecordBatch]:
        iterator = iter(batches)
        try:
            while True:
                context.check_active()
                try:
                    batch = next(iterator)
                except StopIteration:
                    return
                context.check_active()
                if not isinstance(batch, pa.RecordBatch):
                    raise ValueError("Public plugin returned a non-Arrow batch")
                if not batch.schema.equals(schema, check_metadata=True):
                    raise ValueError("Public plugin batch schema differs from the declared schema")
                if max(batch.nbytes, batch.get_total_buffer_size()) > MAX_PLUGIN_BATCH_BYTES:
                    raise ValueError("Public plugin batch exceeds the byte limit")
                yield batch
        finally:
            if iterator is not batches:
                close_preserving_error(iterator)

    return checked()


def _nested_strings(value: object) -> Iterator[str]:
    if isinstance(value, Mapping):
        for item in value.values():
            yield from _nested_strings(item)
    elif isinstance(value, list | tuple):
        for item in value:
            yield from _nested_strings(item)
    elif isinstance(value, str):
        yield value
