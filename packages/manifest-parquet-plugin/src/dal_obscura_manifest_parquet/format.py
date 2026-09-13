"""Row-group-splittable Parquet table format for manifest-governed files."""

from __future__ import annotations

import base64
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
from dal_obscura_plugin_api import (
    ExecutionContext,
    PluginDescriptor,
    SchemaDescriptor,
    TableHandle,
)

from dal_obscura_manifest_parquet.catalog import _check_context

_MAX_BATCH_ROWS = 65_536

FORMAT_DESCRIPTOR = PluginDescriptor(
    kind="table_format",
    plugin_id="parquet.dataset",
    api_version="1",
    config_version=1,
    distribution="dal-obscura-manifest-parquet",
    version="0.1.0",
    capabilities=frozenset({"nested_schema", "splittable_scan", "field_id_stability"}),
    display_name="Manifest Parquet dataset format",
)


@dataclass(frozen=True, slots=True)
class ParquetRowGroupTask:
    """One immutable manifest member and Parquet row group."""

    relative_path: str
    row_group: int
    columns: tuple[str, ...]


class ParquetDatasetFormat:
    descriptor = FORMAT_DESCRIPTOR

    def __init__(self, handle: TableHandle, context: ExecutionContext) -> None:
        _check_context(context)
        if handle.format_plugin_id != FORMAT_DESCRIPTOR.plugin_id:
            raise ValueError("Parquet format received an incompatible handle")
        metadata = dict(handle.metadata)
        root = metadata.get("root")
        files = metadata.get("files")
        schema_ipc = metadata.get("schema_ipc")
        manifest_hash = metadata.get("manifest_hash")
        if (
            not isinstance(root, str)
            or not isinstance(files, (tuple, list))
            or not files
            or any(not isinstance(item, str) for item in files)
            or not isinstance(schema_ipc, str)
            or not isinstance(manifest_hash, str)
        ):
            raise ValueError("Parquet handle metadata is incomplete")
        try:
            self._schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(schema_ipc)))
        except Exception as exc:
            raise ValueError("Parquet handle schema is invalid") from exc
        self._root = Path(root).resolve(strict=True)
        self._files = tuple(cast(str, item) for item in files)
        self._manifest_hash = manifest_hash
        self._handle = handle

    def schema(self, handle: TableHandle, context: ExecutionContext) -> SchemaDescriptor:
        _check_context(context)
        if handle != self._handle:
            raise ValueError("Parquet schema requested for a different handle")
        return SchemaDescriptor(
            schema_version=1,
            fingerprint=_schema_fingerprint(self._schema),
            arrow_schema=self._schema,
            snapshot_id=handle.snapshot_id,
            stable_ids=True,
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
    ) -> Sequence[ParquetRowGroupTask]:
        _check_context(context)
        if handle != self._handle or schema.arrow_schema != self._schema:
            raise ValueError("Parquet plan input does not match the admitted handle/schema")
        if row_filter is not None:
            raise ValueError("Parquet dataset plugin does not support row-filter pushdown")
        if max_tasks <= 0:
            raise ValueError("max_tasks must be positive")
        columns = _projected_columns(self._schema, projection)
        tasks: list[ParquetRowGroupTask] = []
        for relative_path in self._files:
            path = _safe_member(self._root, relative_path)
            try:
                parquet_file = pq.ParquetFile(path)
            except Exception as exc:
                raise ValueError("manifest Parquet member is unreadable") from exc
            _validate_file_schema(parquet_file.schema_arrow, self._schema)
            for row_group in range(parquet_file.num_row_groups):
                tasks.append(ParquetRowGroupTask(relative_path, row_group, columns))
                if len(tasks) > max_tasks:
                    raise ValueError("Parquet dataset requires more tasks than allowed")
        if not tasks:
            raise ValueError("manifest contains no readable Parquet row groups")
        return tasks

    def execute(
        self,
        task: object,
        context: ExecutionContext,
    ) -> tuple[pa.Schema, Sequence[pa.RecordBatch]]:
        _check_context(context)
        if not isinstance(task, ParquetRowGroupTask):
            raise ValueError("Parquet task has an invalid type")
        if task.relative_path not in self._files or task.row_group < 0:
            raise ValueError("Parquet task is not a member of the admitted manifest")
        path = _safe_member(self._root, task.relative_path)
        try:
            parquet_file = pq.ParquetFile(path)
            _validate_file_schema(parquet_file.schema_arrow, self._schema)
            table = parquet_file.read_row_group(task.row_group, columns=list(task.columns))
        except Exception as exc:
            raise ValueError("Parquet task execution failed") from exc
        expected_schema = _select_schema(self._schema, task.columns)
        if table.schema != expected_schema:
            try:
                table = table.cast(expected_schema)
            except Exception as exc:
                raise ValueError("Parquet output schema differs from the admitted schema") from exc
        return expected_schema, tuple(table.to_batches(max_chunksize=_MAX_BATCH_ROWS))

    def close(self) -> None:
        return None


def parquet_factory(handle: TableHandle, context: ExecutionContext) -> ParquetDatasetFormat:
    """Open the format using only a catalog-resolved immutable handle."""

    return ParquetDatasetFormat(handle, context)


def _safe_member(root: Path, relative_path: str) -> Path:
    candidate = Path(relative_path)
    if candidate.is_absolute() or len(relative_path) > 1_024:
        raise ValueError("manifest member path is invalid")
    try:
        (root / candidate).resolve(strict=False).relative_to(root)
    except ValueError as exc:
        raise ValueError("manifest member escapes the configured root") from exc
    try:
        resolved = (root / candidate).resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise ValueError("manifest member escapes the configured root") from exc
    if not resolved.is_file():
        raise ValueError("manifest member is not a regular file")
    return resolved


def _validate_file_schema(actual: pa.Schema, expected: pa.Schema) -> None:
    if actual != expected:
        raise ValueError("Parquet member schema differs from the pinned manifest schema")


def _projected_columns(schema: pa.Schema, projection: Sequence[str]) -> tuple[str, ...]:
    if not projection or (len(projection) == 1 and projection[0] == "*"):
        return tuple(schema.names)
    selected: list[str] = []
    for path in projection:
        top_level = path.split(".", 1)[0]
        if top_level not in schema.names:
            raise ValueError("projection contains a field outside the pinned schema")
        if top_level not in selected:
            selected.append(top_level)
    return tuple(selected)


def _select_schema(schema: pa.Schema, columns: Sequence[str]) -> pa.Schema:
    """Select top-level fields while preserving the pinned nested field types."""

    fields = [schema.field(name) for name in columns]
    return pa.schema(fields, metadata=schema.metadata)


def _schema_fingerprint(schema: pa.Schema) -> str:
    import hashlib

    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
