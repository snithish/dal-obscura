"""The current public SDK adapter for the native Iceberg execution engine."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import cast

import pyarrow as pa
from dal_obscura_plugin_api import ExecutionContext, PluginDescriptor, SchemaDescriptor, TableHandle

from dal_obscura import __version__
from dal_obscura.policy.filters import deserialize_row_filter
from dal_obscura.policy.paths import FieldPath, FieldSegment
from dal_obscura.policy.schema_identity import schema_has_stable_ids
from dal_obscura.read.request import PlanRequest
from dal_obscura.sources.iceberg import (
    IcebergInputPartition,
    IcebergTableFormat,
)
from dal_obscura.sources.paths import PathRuleEnforcer


class IcebergFormatPlugin:
    descriptor = PluginDescriptor(
        kind="table_format",
        plugin_id="iceberg",
        api_version="2",
        config_version=1,
        distribution="dal-obscura",
        version=__version__,
        display_name="Apache Iceberg",
        capabilities=frozenset(
            {"nested_schema", "snapshot_reads", "splittable_scan", "filter_pushdown"}
        ),
        handle_versions=frozenset({1}),
    )

    def __init__(
        self,
        handle: TableHandle,
        context: ExecutionContext,
        *,
        path_enforcer: PathRuleEnforcer | None = None,
    ) -> None:
        del context
        if handle.format_plugin_id != "iceberg" or handle.handle_version != 1:
            raise ValueError("Unsupported Iceberg handle")
        location = handle.metadata.get("metadata_location")
        options = handle.metadata.get("io_options", {})
        if not isinstance(location, str) or not location or not isinstance(options, Mapping):
            raise ValueError("Invalid Iceberg handle metadata")
        self._handle = handle
        self._table = IcebergTableFormat(
            catalog_name=handle.catalog_instance_id,
            table_name=".".join((*handle.identifier.namespace, handle.identifier.name)),
            metadata_location=location,
            io_options=dict(cast(Mapping[str, object], options)),
            path_enforcer=path_enforcer,
        )

    def schema(self, handle: TableHandle, context: ExecutionContext) -> SchemaDescriptor:
        del context
        self._require_handle(handle)
        schema = self._table.get_schema()
        return SchemaDescriptor(
            schema_version=1,
            fingerprint=hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
            arrow_schema=schema,
            snapshot_id=handle.snapshot_id,
            stable_ids=schema_has_stable_ids(schema),
        )

    def plan(
        self,
        handle: TableHandle,
        schema: SchemaDescriptor,
        context: ExecutionContext,
        *,
        projection: Sequence[str],
        row_filter: str | None,
        max_tasks: int,
    ) -> Sequence[object]:
        del schema, context
        self._require_handle(handle)
        plan = self._table.plan(
            PlanRequest(
                catalog=handle.catalog_instance_id,
                target=self._table.table_name,
                columns=[FieldPath((FieldSegment(name),)).to_human() for name in projection],
                row_filter=deserialize_row_filter(row_filter) if row_filter else None,
            ),
            max_tasks,
        )
        return [
            self._encode_partition(cast(IcebergInputPartition, task.partition))
            for task in plan.tasks
        ]

    def execute(
        self, task: object, context: ExecutionContext
    ) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        del context
        if not isinstance(task, dict) or set(task) != {"columns", "tasks", "row_filter"}:
            raise ValueError("Invalid Iceberg task")
        payload = cast(dict[str, object], task)
        columns, tasks, row_filter = payload["columns"], payload["tasks"], payload["row_filter"]
        if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
            raise ValueError("Invalid Iceberg task columns")
        if not isinstance(tasks, list) or not all(isinstance(item, dict) for item in tasks):
            raise ValueError("Invalid Iceberg task files")
        if row_filter is not None and not isinstance(row_filter, str):
            raise ValueError("Invalid Iceberg task row filter")
        partition = IcebergInputPartition(
            columns=cast(list[str], columns),
            tasks=cast(list[dict[str, object]], tasks),
            backend_pushdown_row_filter=row_filter,
        )
        schema, batches = self._table.execute(partition)
        # PyIceberg selects by field ID and retains table order. The SDK contract
        # captures projection order; Arrow selection only rearranges references.
        schema = pa.schema([schema.field(name) for name in columns], metadata=schema.metadata)
        return schema, _batches_with_declared_schema(schema, batches)

    def close(self) -> None:
        pass

    def _require_handle(self, handle: TableHandle) -> None:
        if handle != self._handle:
            raise ValueError("Iceberg handle does not match this instance")

    @staticmethod
    def _encode_partition(partition: IcebergInputPartition) -> dict[str, object]:
        return {
            "columns": partition.columns,
            "tasks": partition.tasks,
            "row_filter": partition.backend_pushdown_row_filter,
        }


def _batches_with_declared_schema(
    schema: pa.Schema, batches: Iterable[pa.RecordBatch]
) -> Iterator[pa.RecordBatch]:
    """PyIceberg may emit small-offset arrays for its large-offset schema."""
    iterator = iter(batches)
    try:
        for batch in iterator:
            if batch.schema.names != schema.names:
                if set(batch.schema.names) != set(schema.names):
                    raise ValueError("Reader column names differ from declared schema")
                batch = batch.select(schema.names)
            # Preserve existing buffers when the reader already honors the schema.
            yield batch if batch.schema.equals(schema, check_metadata=True) else batch.cast(schema)
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()
