"""Row-group-splittable Parquet table format for manifest-governed files."""

from __future__ import annotations

import base64
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
from dal_obscura_plugin_api import (
    ExecutionContext,
    PluginDescriptor,
    ScanRequest,
    ScanTask,
    SchemaDescriptor,
    TableHandle,
)
from dal_obscura_plugin_api.storage import StorageRoot

_MAX_BATCH_ROWS = 65_536

FORMAT_DESCRIPTOR = PluginDescriptor(
    kind="table_format",
    plugin_id="parquet.dataset",
    api_version="2",
    config_version=1,
    distribution="dal-obscura-manifest-parquet",
    version="0.2.0",
    capabilities=frozenset({"nested_schema", "splittable_scan"}),
    handle_versions=frozenset({1}),
    display_name="Manifest Parquet dataset format",
)


@dataclass(frozen=True, slots=True)
class _ParquetRowGroup:
    """One immutable manifest member and Parquet row group."""

    relative_path: str
    row_group: int


@dataclass(frozen=True, slots=True)
class _ParquetScan:
    """Bounded group of row-group scans assigned to one independently readable ticket."""

    row_groups: tuple[_ParquetRowGroup, ...]
    columns: tuple[str, ...]


class ParquetDatasetFormat:
    descriptor = FORMAT_DESCRIPTOR

    def __init__(self, handle: TableHandle, context: ExecutionContext) -> None:
        context.check_active()
        if handle.format_plugin_id != FORMAT_DESCRIPTOR.plugin_id:
            raise ValueError("Parquet format received an incompatible handle")
        if handle.handle_version != 1 or set(handle.metadata) != {"root", "files", "schema_ipc"}:
            raise ValueError("Unsupported Parquet handle")
        metadata = dict(handle.metadata)
        root = metadata.get("root")
        files = metadata.get("files")
        schema_ipc = metadata.get("schema_ipc")
        if (
            not isinstance(root, str)
            or not isinstance(files, (tuple, list))
            or not files
            or any(not isinstance(item, str) for item in files)
            or not isinstance(schema_ipc, str)
        ):
            raise ValueError("Parquet handle metadata is incomplete")
        try:
            self._schema = pa.ipc.read_schema(
                pa.BufferReader(base64.b64decode(schema_ipc, validate=True))
            )
        except Exception as exc:
            raise ValueError("Parquet handle schema is invalid") from exc
        self._storage = StorageRoot(root)
        self._files = tuple(cast(str, item) for item in files)
        self._handle = handle

    def schema(self, context: ExecutionContext) -> SchemaDescriptor:
        context.check_active()
        return SchemaDescriptor(
            arrow_schema=self._schema,
            snapshot_id=self._handle.snapshot_id,
            stable_ids=False,
        )

    def plan(self, request: ScanRequest, context: ExecutionContext) -> list[ScanTask]:
        context.check_active()
        if request.row_filter is not None:
            raise ValueError("Parquet dataset plugin does not support row-filter pushdown")
        columns = request.columns
        if any(name not in self._schema.names for name in columns):
            raise ValueError("projection contains a field outside the pinned schema")
        if not _select_schema(self._schema, columns).equals(request.schema, check_metadata=True):
            raise ValueError("Parquet projection differs from the pinned schema")
        tasks: list[_ParquetRowGroup] = []
        for relative_path in self._files:
            context.check_active()
            path = self._storage.member(relative_path)
            try:
                parquet_file = pq.ParquetFile(path, filesystem=self._storage.filesystem)
            except Exception as exc:
                raise ValueError("manifest Parquet member is unreadable") from exc
            try:
                _validate_file_schema(parquet_file.schema_arrow, self._schema)
                for row_group in range(parquet_file.num_row_groups):
                    context.check_active()
                    tasks.append(_ParquetRowGroup(relative_path, row_group))
            finally:
                parquet_file.close()
        groups: list[list[_ParquetRowGroup]] = [
            [] for _ in range(min(request.max_tasks, len(tasks)))
        ]
        for index, task in enumerate(tasks):
            groups[index % len(groups)].append(task)
        return [
            ScanTask(
                {
                    "columns": list(columns),
                    "row_groups": [
                        {"relative_path": item.relative_path, "row_group": item.row_group}
                        for item in group
                    ],
                }
            )
            for group in groups
        ]

    def execute(
        self,
        task: ScanTask,
        context: ExecutionContext,
    ) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        context.check_active()
        scan = _decode_task(task.to_json())
        expected_schema = _select_schema(self._schema, scan.columns)
        for group in scan.row_groups:
            if group.relative_path not in self._files or group.row_group < 0:
                raise ValueError("Parquet task is not a member of the admitted manifest")

        def batches() -> Iterator[pa.RecordBatch]:
            for group in scan.row_groups:
                context.check_active()
                path = self._storage.member(group.relative_path)
                with pq.ParquetFile(path, filesystem=self._storage.filesystem) as parquet_file:
                    _validate_file_schema(parquet_file.schema_arrow, self._schema)
                    if group.row_group >= parquet_file.num_row_groups:
                        raise ValueError("Parquet task row group is outside the admitted file")
                    for batch in parquet_file.iter_batches(
                        batch_size=_MAX_BATCH_ROWS,
                        row_groups=[group.row_group],
                        columns=list(scan.columns),
                    ):
                        context.check_active()
                        # Parquet columns use dotted prefixes and may include a
                        # nested-name twin. Arrow selection uses exact field names.
                        batch = batch.select(list(scan.columns))
                        if not batch.schema.equals(expected_schema, check_metadata=True):
                            batch = batch.cast(expected_schema)
                        yield batch

        return expected_schema, batches()

    def close(self) -> None:
        return None


def parquet_factory(handle: TableHandle, context: ExecutionContext) -> ParquetDatasetFormat:
    """Open the format using only a catalog-resolved immutable handle."""

    return ParquetDatasetFormat(handle, context)


def _validate_file_schema(actual: pa.Schema, expected: pa.Schema) -> None:
    if actual != expected:
        raise ValueError("Parquet member schema differs from the pinned manifest schema")


def _select_schema(schema: pa.Schema, columns: Sequence[str]) -> pa.Schema:
    """Select top-level fields while preserving the pinned nested field types."""

    fields = [schema.field(name) for name in columns]
    return pa.schema(fields, metadata=schema.metadata)


def _decode_task(task: object) -> _ParquetScan:
    if not isinstance(task, dict) or set(task) != {"columns", "row_groups"}:
        raise ValueError("Parquet task must contain columns and row groups")
    payload = cast(dict[str, Any], task)
    columns, groups = payload["columns"], payload["row_groups"]
    if (
        not isinstance(columns, list)
        or not all(isinstance(name, str) for name in columns)
        or not isinstance(groups, list)
        or not groups
    ):
        raise ValueError("Invalid Parquet task")
    decoded = []
    for raw_group in groups:
        group = cast(dict[str, Any], raw_group)
        if (
            not isinstance(group, dict)
            or set(group) != {"relative_path", "row_group"}
            or not isinstance(group["relative_path"], str)
            or type(group["row_group"]) is not int
        ):
            raise ValueError("Invalid Parquet row group")
        decoded.append(_ParquetRowGroup(group["relative_path"], group["row_group"]))
    return _ParquetScan(tuple(decoded), tuple(columns))
