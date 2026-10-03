"""Passive distributed Iceberg scans backed by Apache Iceberg native file readers."""

from __future__ import annotations

import heapq
import os
from collections.abc import Iterator
from datetime import datetime, timezone
from itertools import pairwise

import pyarrow as pa
from dal_obscura_plugin_api import (
    ExecutionContext,
    PluginDescriptor,
    ScanRequest,
    ScanTask,
    SchemaDescriptor,
    TableHandle,
)
from pyiceberg.io.pyarrow import ArrowAccessor, ArrowProjectionVisitor, pyarrow_to_schema
from pyiceberg.manifest import DataFileContent
from pyiceberg.schema import visit_with_partner
from pyiceberg.table import StaticTable

from dal_obscura import __version__
from dal_obscura.policy.schema_bounds import validate_arrow_schema_bounds
from dal_obscura.policy.schema_identity import schema_has_stable_ids
from dal_obscura.sources.paths import PathRuleEnforcer
from dal_obscura.sources.plugin_runtime import _close_plugin_preserving_error

_MAX_FILES = 10_000


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
            {
                "nested_schema",
                "snapshot_reads",
                "splittable_scan",
                "delete_files",
                "cancellation",
            }
        ),
        handle_versions=frozenset({1}),
    )

    def __init__(
        self,
        handle: TableHandle,
        context: ExecutionContext,
        *,
        path_enforcer: PathRuleEnforcer | None = None,
    ):
        context.check_active()
        if handle.format_plugin_id != "iceberg" or handle.handle_version != 1:
            raise ValueError("Unsupported Iceberg handle")
        if set(handle.metadata) != {"metadata_location"}:
            raise ValueError("Iceberg handle contains unsupported metadata")
        location = handle.metadata.get("metadata_location")
        if not isinstance(location, str) or not location:
            raise ValueError("Invalid Iceberg metadata location")
        self._handle = handle
        self._location = location
        self._enforcer = path_enforcer
        self._table = None
        self._files = None
        self._reader = None

    def _check_path(self, location):
        if self._enforcer is not None:
            self._enforcer.check(location)

    def _load(self, context):
        if self._table is None:
            context.check_active()
            self._check_path(self._location)
            # Worker identity owns storage credentials; provider properties must
            # never be copied into the passive handle or ticket.
            options = {}
            endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
            region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
            if endpoint:
                options["s3.endpoint"] = endpoint
            if region:
                options["s3.region"] = region
            table = StaticTable.from_metadata(self._location, properties=options)
            context.check_active()
            if table.metadata.format_version != 2:
                raise ValueError(
                    f"Unsupported Iceberg format version: {table.metadata.format_version}"
                )
            snapshot = table.current_snapshot()
            if self._handle.snapshot_id is not None and (
                snapshot is None or str(snapshot.snapshot_id) != self._handle.snapshot_id
            ):
                raise ValueError("Iceberg snapshot identity changed")
            self._check_path(table.metadata.location)
            for item in table.metadata.metadata_log:
                self._check_path(item.metadata_file)
            for item in table.metadata.snapshots:
                self._check_path(item.manifest_list)
            self._table = table
        return self._table

    def schema(self, context: ExecutionContext) -> SchemaDescriptor:
        table = self._load(context)
        schema = table.schema().as_arrow()
        validate_arrow_schema_bounds(schema)
        return SchemaDescriptor(schema, self._handle.snapshot_id, schema_has_stable_ids(schema))

    def _members(self, context):
        if self._files is None:
            table = self._load(context)
            snapshot = table.current_snapshot()
            files = {}
            count = 0
            if snapshot is not None:
                for manifest in snapshot.manifests(table.io):
                    context.check_active()
                    self._check_path(manifest.manifest_path)
                    for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True):
                        context.check_active()
                        count += 1
                        if count > _MAX_FILES:
                            raise ValueError("Iceberg snapshot exceeds the file budget")
                        data = entry.data_file
                        self._check_path(data.file_path)
                        if data.content == DataFileContent.DATA:
                            if data.file_path in files:
                                raise ValueError("Duplicate Iceberg data file")
                            size = max(1, data.file_size_in_bytes)
                            offsets = tuple(data.split_offsets or ())
                            if (
                                len(offsets) > _MAX_FILES
                                or any(
                                    type(offset) is not int or not 0 <= offset < size
                                    for offset in offsets
                                )
                                or tuple(sorted(set(offsets))) != offsets
                            ):
                                raise ValueError("Invalid Iceberg split offsets")
                            files[data.file_path] = (size, offsets)
            self._files = files
        return self._files

    def plan(self, request: ScanRequest, context: ExecutionContext) -> list[ScanTask]:
        if request.row_filter is not None:
            raise ValueError("Iceberg does not support SQL filter pushdown")
        schema = self.schema(context).arrow_schema
        projected = _projection(schema, request.columns)
        if not projected.equals(request.schema, check_metadata=True):
            raise ValueError("Iceberg projection differs from pinned schema")
        files = self._members(context)
        if not files:
            return []
        self._native(context)
        units = _split_work(files, request.max_tasks)
        groups = [[] for _ in range(min(request.max_tasks, len(units)))]
        loads = [(0, index) for index in range(len(groups))]
        heapq.heapify(loads)
        for path, start, size in sorted(units, key=lambda item: (-item[2], item[0], item[1])):
            total, index = heapq.heappop(loads)
            groups[index].append((path, start, size))
            heapq.heappush(loads, (total + size, index))
        return [
            ScanTask(
                {
                    "columns": list(request.columns),
                    "files": [path for path, _, _ in group],
                    "ranges": [[start, size] for _, start, size in group],
                }
            )
            for group in groups
        ]

    def execute(self, task: ScanTask, context: ExecutionContext):
        context.check_active()
        payload = task.to_json()
        if (
            set(payload) != {"columns", "files", "ranges"}
            or not isinstance(payload["columns"], list)
            or not isinstance(payload["files"], list)
            or not payload["files"]
            or any(not isinstance(path, str) for path in payload["files"])
            or not isinstance(payload["ranges"], list)
            or len(payload["ranges"]) != len(payload["files"])
        ):
            raise ValueError("Invalid Iceberg task")
        schema = _projection(self.schema(context).arrow_schema, payload["columns"])
        members = self._members(context)
        if any(path not in members for path in payload["files"]):
            raise ValueError("Iceberg task is outside its pinned snapshot")
        intervals = {}
        for path, pair in zip(payload["files"], payload["ranges"], strict=True):
            if (
                not isinstance(pair, list)
                or len(pair) != 2
                or any(type(value) is not int for value in pair)
                or pair[0] < 0
                or pair[1] <= 0
                or sum(pair) > members[path][0]
            ):
                raise ValueError("Invalid Iceberg task range")
            intervals.setdefault(path, []).append((pair[0], sum(pair)))
        for ranges in intervals.values():
            ranges.sort()
            if any(left[1] > right[0] for left, right in pairwise(ranges)):
                raise ValueError("Overlapping Iceberg task ranges")
        return schema, self._batches(payload, schema, context)

    def _native(self, context):
        if self._reader is None:
            from dal_obscura_iceberg_reader import Reader

            members = self._members(context)
            table = self._load(context)
            snapshot = table.current_snapshot()
            if snapshot is None:
                raise ValueError("Cannot execute an empty Iceberg snapshot")
            context.check_active()
            reader = Reader(
                self._location, snapshot.snapshot_id, _storage_options(), _remaining(context)
            )
            context.check_active()
            planned = reader.files()
            if len(planned) != len(members) or {path for path, _ in planned} != set(members):
                raise ValueError("Native Iceberg plan differs from admitted snapshot")
            self._reader = reader
        return self._reader

    def _batches(self, payload, schema, context) -> Iterator[pa.RecordBatch]:
        context.check_active()
        reader = self._native(context).start(
            [(path, *pair) for path, pair in zip(payload["files"], payload["ranges"], strict=True)]
        )
        try:
            while True:
                context.check_active()
                batch = reader.next(_remaining(context))
                context.check_active()
                if batch is None:
                    break
                table = self._load(context)
                source_schema = pyarrow_to_schema(batch.schema)
                # Rust scans use the snapshot's historical schema. Upstream's
                # field-ID visitor evolves it to the captured metadata schema,
                # after deletes, so renamed/dropped delete keys remain available.
                batch = pa.RecordBatch.from_struct_array(
                    visit_with_partner(
                        table.schema(),
                        batch,
                        ArrowProjectionVisitor(source_schema, include_field_ids=True),
                        ArrowAccessor(source_schema),
                    )
                )
                # Upstream reads all fields, including hidden equality-delete keys.
                # Select buffers only after native deletes; preserve exact SDK metadata.
                batch = batch.select(schema.names)
                yield (
                    batch
                    if batch.schema.equals(schema, check_metadata=True)
                    else batch.cast(schema)
                )
        finally:
            _close_plugin_preserving_error(reader)

    def close(self):
        self._table = None
        self._files = None
        self._reader = None


def _projection(schema, columns):
    if any(not isinstance(name, str) or name not in schema.names for name in columns) or len(
        set(columns)
    ) != len(columns):
        raise ValueError("Invalid Iceberg projection")
    return pa.schema([schema.field(name) for name in columns], metadata=schema.metadata)


def _remaining(context):
    context.check_active()
    return (context.deadline - datetime.now(timezone.utc)).total_seconds()


def _split_work(files, budget):
    units = [(path, 0, size) for path, (size, _) in files.items()]
    while len(units) < budget:
        candidates = []
        for index, (path, start, size) in enumerate(units):
            points = [point for point in files[path][1][1:] if start < point < start + size]
            if points:
                boundary = min(points, key=lambda point: (abs(point - start - size / 2), point))
                candidates.append((-size, path, start, index, boundary))
        if not candidates:
            break
        _, path, start, index, boundary = min(candidates)
        size = units[index][2]
        units[index : index + 1] = [
            (path, start, boundary - start),
            (path, boundary, start + size - boundary),
        ]
    return units


def _storage_options():
    # OpenDAL obtains credentials from each worker's native AWS provider chain.
    options = {}
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
    if region:
        options["s3.region"] = region
    if endpoint:
        from urllib.parse import urlsplit

        parsed = urlsplit(endpoint)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError("Invalid operator S3 endpoint")
        options["s3.endpoint"] = endpoint
        options["s3.path-style-access"] = "true"
    return options
