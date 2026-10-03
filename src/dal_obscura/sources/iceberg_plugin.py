"""SDK-native Iceberg scans; DuckDB owns snapshot and delete interpretation."""

from __future__ import annotations

import heapq
import os
from collections.abc import Iterator
from importlib import resources

import duckdb
import pyarrow as pa
from dal_obscura_plugin_api import (
    ExecutionContext,
    PluginDescriptor,
    ScanRequest,
    ScanTask,
    SchemaDescriptor,
    TableHandle,
)
from pyiceberg.manifest import DataFileContent
from pyiceberg.table import StaticTable

from dal_obscura import __version__
from dal_obscura.policy.filters import deserialize_row_filter
from dal_obscura.policy.schema_bounds import validate_arrow_schema_bounds
from dal_obscura.policy.schema_identity import schema_has_stable_ids
from dal_obscura.sources.paths import PathRuleEnforcer
from dal_obscura.sources.plugin_runtime import _close_plugin_preserving_error

_MAX_FILES = 10_000
_BATCH_ROWS = 8192


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
                "filter_pushdown",
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
        self._equality_deletes = False

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
            equality_deletes = False
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
                        if data.content == DataFileContent.EQUALITY_DELETES:
                            equality_deletes = True
                        if data.content == DataFileContent.DATA:
                            if data.file_path in files:
                                raise ValueError("Duplicate Iceberg data file")
                            files[data.file_path] = max(1, data.file_size_in_bytes)
            self._files = files
            self._equality_deletes = equality_deletes
        return self._files

    def plan(self, request: ScanRequest, context: ExecutionContext) -> list[ScanTask]:
        row_filter = _filter(request.row_filter)
        schema = self.schema(context).arrow_schema
        projected = _projection(schema, request.columns)
        if not projected.equals(request.schema, check_metadata=True):
            raise ValueError("Iceberg projection differs from pinned schema")
        files = self._members(context)
        if not files:
            return []
        if self._equality_deletes:
            return [
                ScanTask(
                    {
                        "columns": list(request.columns),
                        "files": sorted(files),
                        "parallelism": min(request.max_tasks, len(files), 4),
                        "row_filter": row_filter,
                    }
                )
            ]
        groups = [[] for _ in range(min(request.max_tasks, len(files)))]
        loads = [(0, index) for index in range(len(groups))]
        heapq.heapify(loads)
        for path, size in sorted(files.items(), key=lambda item: (-item[1], item[0])):
            total, index = heapq.heappop(loads)
            groups[index].append(path)
            heapq.heappush(loads, (total + size, index))
        return [
            ScanTask(
                {
                    "columns": list(request.columns),
                    "files": group,
                    "parallelism": 1,
                    "row_filter": row_filter,
                }
            )
            for group in groups
        ]

    def execute(self, task: ScanTask, context: ExecutionContext):
        context.check_active()
        payload = task.to_json()
        if (
            set(payload) != {"columns", "files", "parallelism", "row_filter"}
            or not isinstance(payload["columns"], list)
            or not isinstance(payload["files"], list)
            or not payload["files"]
            or any(not isinstance(path, str) for path in payload["files"])
            or len(set(payload["files"])) != len(payload["files"])
            or type(payload["parallelism"]) is not int
            or not 1 <= payload["parallelism"] <= 4
        ):
            raise ValueError("Invalid Iceberg task")
        _filter(payload["row_filter"])
        schema = _projection(self.schema(context).arrow_schema, payload["columns"])
        members = self._members(context)
        if any(path not in members for path in payload["files"]):
            raise ValueError("Iceberg task is outside its pinned snapshot")
        if self._equality_deletes and set(payload["files"]) != set(members):
            raise ValueError("Equality-delete scans require complete native snapshot ownership")
        return schema, self._batches(payload, schema, context)

    def _batches(self, payload, schema, context) -> Iterator[pa.RecordBatch]:
        # Read all schema fields at the native boundary. This keeps equality
        # delete keys present even when callers project them away. Python selects
        # requested buffers afterwards; no custom equality-delete implementation.
        virtual = "__dal_source_file"
        while virtual in self._load(context).schema().as_arrow().names:
            virtual += "_"
        connection = _connection(self._location, payload["parallelism"])
        reader = None
        try:
            context.check_active()
            placeholders = ",".join("?" for _ in payload["files"])
            if self._equality_deletes:
                sql, parameters = "SELECT * FROM iceberg_scan(?)", [self._location]
            else:
                sql = (
                    f"SELECT * FROM iceberg_scan(?, filename={_literal(virtual)}) "
                    f"WHERE {_identifier(virtual)} IN ({placeholders})"
                )
                parameters = [self._location, *payload["files"]]
            if payload["row_filter"] is not None:
                sql += " WHERE " if self._equality_deletes else " AND "
                sql += "(" + _filter(payload["row_filter"]) + ")"
            reader = connection.execute(sql, parameters).to_arrow_reader(_BATCH_ROWS)
            while True:
                context.check_active()
                batch = next(reader, None)
                if batch is None:
                    break
                context.check_active()
                batch = batch.select(schema.names)
                yield (
                    batch
                    if batch.schema.equals(schema, check_metadata=True)
                    else batch.cast(schema)
                )
        finally:
            try:
                if reader is not None:
                    _close_plugin_preserving_error(reader)
            finally:
                _close_plugin_preserving_error(connection)

    def close(self):
        self._table = None
        self._files = None
        self._equality_deletes = False


def _filter(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("Invalid Iceberg filter hint")
    return deserialize_row_filter(value).expression.sql(dialect="duckdb")


def _projection(schema, columns):
    if any(not isinstance(name, str) or name not in schema.names for name in columns) or len(
        set(columns)
    ) != len(columns):
        raise ValueError("Invalid Iceberg projection")
    return pa.schema([schema.field(name) for name in columns], metadata=schema.metadata)


def _identifier(value):
    return '"' + value.replace('"', '""') + '"'


def _literal(value):
    return "'" + value.replace("'", "''") + "'"


def _connection(location, threads=1):
    connection = duckdb.connect(
        config={
            "threads": threads,
            "arrow_large_buffer_size": True,
            "memory_limit": "256MB",
            "temp_directory": "",
            "autoload_known_extensions": False,
            "autoinstall_known_extensions": False,
        }
    )
    try:
        for name in ("httpfs", "aws", "avro", "iceberg"):
            path = resources.files("duckdb_extension_" + name).joinpath(
                "extensions", "v1.5.4", name + ".duckdb_extension"
            )
            connection.execute("LOAD " + _literal(str(path)))
        if location.startswith("s3:"):
            properties = ["TYPE S3", "PROVIDER CREDENTIAL_CHAIN", "REFRESH auto"]
            region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
            if region:
                properties.append("REGION " + _literal(region))
            endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
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
                properties += [
                    "ENDPOINT " + _literal(parsed.netloc),
                    "URL_STYLE " + _literal("path"),
                    "USE_SSL " + ("true" if parsed.scheme == "https" else "false"),
                ]
            connection.execute("CREATE SECRET (" + ",".join(properties) + ")")
        return connection
    except BaseException:
        _close_plugin_preserving_error(connection)
        raise
