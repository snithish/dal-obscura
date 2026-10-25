"""Snapshot-bound row-group scans with kernel-decoded deletion vectors."""

from __future__ import annotations

import base64
import heapq
from collections.abc import Iterator, Sequence
from types import TracebackType
from typing import Self, TypedDict, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from dal_obscura_plugin_api import (
    ExecutionContext,
    ScanRequest,
    ScanTask,
    SchemaDescriptor,
    TableHandle,
)
from dal_obscura_plugin_api.descriptors import load_packaged_descriptor
from dal_obscura_plugin_api.execution import active
from dal_obscura_plugin_api.options import batch_rows
from pyarrow.dataset import ParquetFileFormat

from dal_obscura_delta.snapshot import MAX_DV_ROWS, MAX_FILES, open_snapshot
from dal_obscura_delta.storage import DeltaStorage

FORMAT_DESCRIPTOR = load_packaged_descriptor(
    "dal-obscura-delta", __package__, "table_format", "delta"
)
_KEEP_SCHEMA = pa.schema([pa.field("keep", pa.bool_(), nullable=False)])
_BATCH_ROWS = 8192


class _RowGroup(TypedDict):
    path: str
    row_group: int
    rows: int
    keep: object


class DeltaFormat:
    descriptor = FORMAT_DESCRIPTOR

    def __init__(self, handle: TableHandle, context: ExecutionContext) -> None:
        self._batch_rows = batch_rows(context.options, default=_BATCH_ROWS)
        if handle.format_plugin_id != "delta" or handle.handle_version != 1:
            raise ValueError("Incompatible Delta handle")
        metadata = handle.metadata
        location, version = metadata.get("location"), metadata.get("version")
        if (
            set(metadata) != {"location", "version", "table_id"}
            or not isinstance(location, str)
            or not isinstance(metadata["table_id"], str)
            or type(version) is not int
            or version < 0
        ):
            raise ValueError("Invalid Delta handle metadata")
        self._storage = DeltaStorage(location)
        self._table = open_snapshot(self._storage, context, version)
        if (
            self._table.metadata().id != metadata["table_id"]
            or handle.snapshot_id != f"{metadata['table_id']}@{metadata['version']}"
        ):
            raise ValueError("Delta snapshot identity changed")
        self._handle = handle
        self._schema = pa.schema(self._table.schema().to_arrow())
        actions = pa.table(self._table.get_add_actions())
        if actions.num_rows > MAX_FILES:
            raise ValueError("Delta snapshot exceeds the active file budget")
        columns = [
            name for name in ("path", "num_records", "partition") if name in actions.column_names
        ]
        self._actions = {row["path"]: row for row in actions.select(columns).to_pylist()}
        if len(self._actions) != actions.num_rows:
            raise ValueError("Delta snapshot contains duplicate active files")
        # The public Arrow dataset owns COW partition/statistics/schema handling.
        # It explicitly rejects deletion vectors; only that path needs the
        # kernel selection-vector bridge below.
        self._fragments = (
            None
            if "deletionVectors" in (self._table.protocol().reader_features or ())
            else {
                fragment.path: fragment
                for fragment in self._table.to_pyarrow_dataset(
                    filesystem=self._storage.filesystem
                ).get_fragments()
            }
        )

    @active
    def schema(self, context: ExecutionContext) -> SchemaDescriptor:
        return SchemaDescriptor(self._schema, self._handle.snapshot_id, stable_ids=False)

    @active
    def plan(self, request: ScanRequest, context: ExecutionContext) -> list[ScanTask]:
        if request.row_filter is not None:
            raise ValueError("Delta does not support SQL filter pushdown")
        if not self._projection(request.columns).equals(request.schema, check_metadata=True):
            raise ValueError("Delta projection differs from the pinned schema")
        masks = self._deletion_masks(context)
        units = []
        for path in sorted(self._actions):
            context.check_active()
            member = self._storage.member(path, encoded=True)
            with pq.ParquetFile(member, filesystem=self._storage.filesystem) as parquet:
                offset = 0
                mask = masks.get(str(member))
                if mask is not None and len(mask) != parquet.metadata.num_rows:
                    raise ValueError("Delta deletion vector row count differs from its file")
                for index in range(parquet.num_row_groups):
                    context.check_active()
                    group = parquet.metadata.row_group(index)
                    rows = group.num_rows
                    keep = mask.slice(offset, rows) if mask is not None else None
                    if rows and (keep is None or pc.call_function("any", [keep]).as_py()):
                        payload = {
                            "path": path,
                            "row_group": index,
                            "rows": rows,
                            "keep": _encode_keep(keep) if keep is not None else None,
                        }
                        units.append((group.total_byte_size, payload))
                        if len(units) > MAX_FILES:
                            raise ValueError("Delta scan exceeds the row-group budget")
                    offset += rows
        groups = [[] for _ in range(min(request.max_tasks, len(units)))]
        loads = [(0, i) for i in range(len(groups))]
        heapq.heapify(loads)
        for size, payload in sorted(units, key=lambda unit: -unit[0]):
            load, index = heapq.heappop(loads)
            groups[index].append(payload)
            heapq.heappush(loads, (load + size, index))
        return [
            ScanTask({"columns": list(request.columns), "row_groups": group}) for group in groups
        ]

    @active
    def _deletion_masks(self, context: ExecutionContext) -> dict[str, pa.BooleanArray]:
        if self._table is None:
            raise ValueError("Delta format is closed")
        if "deletionVectors" not in (self._table.protocol().reader_features or ()):
            return {}
        counts = [action["num_records"] for action in self._actions.values()]
        if any(type(n) is not int or n < 0 for n in counts) or sum(counts) > MAX_DV_ROWS:
            raise ValueError("Delta deletion-vector snapshot exceeds the physical-row budget")
        context.check_active()
        masks = {}
        with pa.RecordBatchReader.from_stream(self._table.deletion_vectors()) as reader:
            for batch in reader:
                context.check_active()
                for i, uri in enumerate(batch.column("filepath").to_pylist()):
                    path = self._storage.member(uri)
                    mask = batch.column("selection_vector")[i].values
                    if mask.null_count or len(mask) > MAX_DV_ROWS or path in masks:
                        raise ValueError("Invalid Delta deletion vector")
                    masks[path] = mask
        context.check_active()
        return masks

    def _projection(self, columns: Sequence[str]) -> pa.Schema:
        if any(name not in self._schema.names for name in columns) or len(set(columns)) != len(
            columns
        ):
            raise ValueError("Invalid Delta projection")
        return pa.schema(
            [self._schema.field(name) for name in columns], metadata=self._schema.metadata
        )

    @active
    def execute(
        self, task: ScanTask, context: ExecutionContext
    ) -> tuple[pa.Schema, Iterator[pa.RecordBatch]]:
        payload = task.to_json()
        if (
            set(payload) != {"columns", "row_groups"}
            or not isinstance(payload["columns"], list)
            or not isinstance(payload["row_groups"], list)
            or not payload["row_groups"]
        ):
            raise ValueError("Invalid Delta task")
        columns: list[str] = []
        for column in payload["columns"]:
            if not isinstance(column, str):
                raise ValueError("Invalid Delta task")
            columns.append(column)
        schema = self._projection(columns)
        seen = set()
        groups: list[_RowGroup] = []
        for raw_group in payload["row_groups"]:
            if not isinstance(raw_group, dict):
                raise ValueError("Delta task is not a member of its pinned snapshot")
            group = cast(dict[str, object], raw_group)
            path, index, rows = group.get("path"), group.get("row_group"), group.get("rows")
            if (
                set(group) != {"path", "row_group", "rows", "keep"}
                or not isinstance(path, str)
                or path not in self._actions
                or type(index) is not int
                or index < 0
                or type(rows) is not int
                or rows <= 0
            ):
                raise ValueError("Delta task is not a member of its pinned snapshot")
            key = (path, index)
            if key in seen:
                raise ValueError("Duplicate Delta row group")
            seen.add(key)
            groups.append({"path": path, "row_group": index, "rows": rows, "keep": group["keep"]})
        return schema, self._batches(groups, schema, context)

    @active
    def _batches(
        self, groups: Sequence[_RowGroup], schema: pa.Schema, context: ExecutionContext
    ) -> Iterator[pa.RecordBatch]:
        for group in groups:
            context.check_active()
            path = self._storage.member(group["path"], encoded=True)
            with pq.ParquetFile(path, filesystem=self._storage.filesystem) as parquet:
                index = group["row_group"]
                if (
                    index >= parquet.num_row_groups
                    or parquet.metadata.row_group(index).num_rows != group["rows"]
                ):
                    raise ValueError("Delta task row group differs from its pinned file")
            keep = _decode_keep(group["keep"], group["rows"])
            partition = self._actions[group["path"]].get("partition", {}) or {}
            expression = ds.scalar(True)
            for name, value in partition.items():
                field = ds.field(name)
                expression &= (
                    field.is_null()
                    if value is None
                    else field == pa.scalar(value, self._schema.field(name).type)
                )
            fragment = (
                self._fragments[group["path"]].subset(row_group_ids=[index])
                if self._fragments is not None
                else ParquetFileFormat().make_fragment(
                    str(path),
                    filesystem=self._storage.filesystem,
                    partition_expression=expression,
                    row_groups=[index],
                )
            )
            offset = 0
            batches = fragment.to_batches(
                schema=self._schema,
                columns={name: ds.field(name) for name in schema.names},
                batch_size=self._batch_rows,
                use_threads=False,
                batch_readahead=0,
                fragment_readahead=0,
            )
            try:
                while True:
                    context.check_active()
                    batch = next(batches, None)
                    if batch is None:
                        break
                    context.check_active()
                    rows = batch.num_rows
                    if keep is not None:
                        batch = batch.filter(keep.slice(offset, rows))
                    offset += rows
                    if batch.num_rows:
                        yield batch.cast(schema)
                if offset != group["rows"]:
                    raise ValueError("Delta scan row count differs from its pinned file")
            finally:
                batches.close()

    def close(self) -> None:
        self._table = None
        self._actions.clear()
        self._fragments = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


def _encode_keep(keep: pa.BooleanArray) -> str:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, _KEEP_SCHEMA) as writer:
        writer.write_batch(pa.RecordBatch.from_arrays([keep], schema=_KEEP_SCHEMA))
    return base64.b64encode(sink.getvalue()).decode("ascii")


def _decode_keep(value: object, rows: int) -> pa.BooleanArray | None:
    if value is None:
        return None
    if not isinstance(value, str) or rows > MAX_DV_ROWS or len(value) > (MAX_DV_ROWS // 6) + 1024:
        raise ValueError("Invalid Delta selection mask")
    try:
        with pa.ipc.open_stream(base64.b64decode(value, validate=True)) as reader:
            if not reader.schema.equals(_KEEP_SCHEMA, check_metadata=True):
                raise ValueError("Invalid selection mask schema")
            batch = reader.read_next_batch()
            if batch.num_rows != rows or batch.column(0).null_count:
                raise ValueError("Invalid selection mask rows")
            if next(reader, None) is not None:
                raise ValueError("Multiple selection mask batches")
            return batch.column(0)
    except (ValueError, pa.ArrowException, StopIteration) as exc:
        raise ValueError("Invalid Delta selection mask") from exc


@active
def delta_format_factory(handle: TableHandle, context: ExecutionContext) -> DeltaFormat:
    return DeltaFormat(handle, context)
