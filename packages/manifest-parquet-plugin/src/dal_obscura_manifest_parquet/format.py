"""Row-group-splittable Parquet table format for manifest-governed files."""

from __future__ import annotations

import base64
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
from dal_obscura_plugin_api import (
    ExecutionContext,
    PluginDescriptor,
    SchemaDescriptor,
    TableHandle,
)

from dal_obscura_manifest_parquet.catalog import (
    _check_context,
    _reject_symlink_components,
    _schema_identities,
)

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
    columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _ParquetScan:
    """Bounded group of row-group scans assigned to one independently readable ticket."""

    row_groups: tuple[_ParquetRowGroup, ...]
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
            self._schema = pa.ipc.read_schema(
                pa.BufferReader(base64.b64decode(schema_ipc, validate=True))
            )
        except Exception as exc:
            raise ValueError("Parquet handle schema is invalid") from exc
        _validate_handle_schema_identity(self._schema, metadata)
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
            stable_ids=False,
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
        _check_context(context)
        if handle != self._handle or schema.arrow_schema != self._schema:
            raise ValueError("Parquet plan input does not match the admitted handle/schema")
        if row_filter is not None:
            raise ValueError("Parquet dataset plugin does not support row-filter pushdown")
        if max_tasks <= 0:
            raise ValueError("max_tasks must be positive")
        columns = _projected_columns(self._schema, projection)
        tasks: list[_ParquetRowGroup] = []
        for relative_path in self._files:
            _check_context(context)
            path = _safe_member(self._root, relative_path)
            try:
                parquet_file = pq.ParquetFile(path)
            except Exception as exc:
                raise ValueError("manifest Parquet member is unreadable") from exc
            try:
                _validate_file_schema(parquet_file.schema_arrow, self._schema)
                for row_group in range(parquet_file.num_row_groups):
                    _check_context(context)
                    tasks.append(_ParquetRowGroup(relative_path, row_group, columns))
            finally:
                parquet_file.close()
        groups: list[list[_ParquetRowGroup]] = [[] for _ in range(min(max_tasks, len(tasks)))]
        for index, task in enumerate(tasks):
            groups[index % len(groups)].append(task)
        return [
            {
                "columns": list(columns),
                "row_groups": [
                    {"relative_path": item.relative_path, "row_group": item.row_group}
                    for item in group
                ],
            }
            for group in groups
        ]

    def execute(
        self,
        task: object,
        context: ExecutionContext,
    ) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        _check_context(context)
        task = _decode_task(task)
        expected_schema = _select_schema(self._schema, task.columns)
        for group in task.row_groups:
            if (
                group.relative_path not in self._files
                or group.row_group < 0
                or group.columns != task.columns
            ):
                raise ValueError("Parquet task is not a member of the admitted manifest")

        def batches() -> Iterator[pa.RecordBatch]:
            for group in task.row_groups:
                _check_context(context)
                path = _safe_member(self._root, group.relative_path)
                with pq.ParquetFile(path) as parquet_file:
                    _validate_file_schema(parquet_file.schema_arrow, self._schema)
                    if group.row_group >= parquet_file.num_row_groups:
                        raise ValueError("Parquet task row group is outside the admitted file")
                    for batch in parquet_file.iter_batches(
                        batch_size=_MAX_BATCH_ROWS,
                        row_groups=[group.row_group],
                        columns=list(task.columns),
                    ):
                        _check_context(context)
                        # Parquet columns use dotted prefixes and may include a
                        # nested-name twin. Arrow selection uses exact field names.
                        batch = batch.select(list(task.columns))
                        if batch.schema != expected_schema:
                            batch = batch.cast(expected_schema)
                        yield batch

        return expected_schema, batches()

    def close(self) -> None:
        return None


def parquet_factory(handle: TableHandle, context: ExecutionContext) -> ParquetDatasetFormat:
    """Open the format using only a catalog-resolved immutable handle."""

    return ParquetDatasetFormat(handle, context)


def _safe_member(root: Path, relative_path: str) -> Path:
    candidate = Path(relative_path)
    if candidate.is_absolute() or len(relative_path) > 1_024:
        raise ValueError("manifest member path is invalid")
    _reject_symlink_components(root, root / candidate)
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


def _validate_handle_schema_identity(schema: pa.Schema, metadata: dict[str, object]) -> None:
    """Verify the catalog's immutable identity claim before opening a format.

    The handle is ticket-bound, but its metadata still crosses the public plugin
    boundary. Recomputing the identity list from the pinned schema prevents a
    forged or stale handle from silently changing which nested fields a policy
    refers to. Collection markers use the same ``$element``/``$key``/``$value``
    vocabulary as the core schema and policy paths.
    """

    raw_field_ids = metadata.get("field_ids")
    raw_identities = metadata.get("schema_identities")
    if (
        not isinstance(raw_field_ids, (tuple, list))
        or len(raw_field_ids) != len(schema)
        or any(not isinstance(item, str) or not item for item in raw_field_ids)
        or not isinstance(raw_identities, (tuple, list))
    ):
        raise ValueError("Parquet handle schema identities are incomplete")
    expected = _schema_identities(schema, tuple(cast(str, item) for item in raw_field_ids))
    normalized: list[tuple[str, str]] = []
    for item in raw_identities:
        if (
            not isinstance(item, (tuple, list))
            or len(item) != 2
            or any(not isinstance(value, str) or not value for value in item)
        ):
            raise ValueError("Parquet handle schema identities are invalid")
        normalized.append((cast(str, item[0]), cast(str, item[1])))
    if tuple(normalized) != expected:
        raise ValueError("Parquet handle schema identities do not match the pinned schema")


def _projected_columns(schema: pa.Schema, projection: Sequence[str]) -> tuple[str, ...]:
    selected: list[str] = []
    for path in projection:
        top_level = path
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
        decoded.append(_ParquetRowGroup(group["relative_path"], group["row_group"], tuple(columns)))
    return _ParquetScan(tuple(decoded), tuple(columns))
