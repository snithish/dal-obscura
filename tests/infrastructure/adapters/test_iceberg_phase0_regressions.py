from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, ClassVar, cast

import pyarrow as pa
import pytest
from pyiceberg.expressions import EqualTo
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table import FileScanTask
from pyiceberg.typedef import Record

from dal_obscura.policy.filters import (
    deserialize_row_filter,
    row_filter_to_sql,
)
from dal_obscura.read.request import PlanRequest
from dal_obscura.sources.iceberg import (
    IcebergInputPartition,
    IcebergTableFormat,
    _check_file_tasks,
    _check_io_options,
    _check_table_locations,
    _chunk_by_max_tickets,
)
from dal_obscura.sources.iceberg_tasks import encode_scan_task
from dal_obscura.sources.paths import PathRuleEnforcer
from tests.support.iceberg_schema import _FakeProjectedSchema


def test_scan_groups_balance_data_and_delete_bytes_without_losing_tasks() -> None:
    def data_file(size: int, ordinal: int) -> DataFile:
        return DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=f"/tmp/{ordinal}.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=1,
            file_size_in_bytes=size,
        )

    tasks = [FileScanTask(data_file(size, index)) for index, size in enumerate((1000, 1, 500, 1))]
    tasks[2].delete_files.add(data_file(500, 4))
    groups = _chunk_by_max_tickets(tasks, 2)

    assert groups == [[tasks[0], tasks[1]], [tasks[2], tasks[3]]]
    assert _chunk_by_max_tickets(tasks, 2) == groups
    assert _chunk_by_max_tickets(tasks, 8) == [[task] for task in tasks]
    assert _chunk_by_max_tickets([], 2) == []


@dataclass(frozen=True, kw_only=True)
class _FakeTable:
    schema_value: pa.Schema

    @property
    def metadata(self) -> object:
        return object()

    @property
    def io(self) -> object:
        return object()

    def schema(self) -> _FakeProjectedSchema:
        return _FakeProjectedSchema(schema=self.schema_value)


def test_iceberg_path_policy_checks_delete_file_locations() -> None:
    class _DataFile:
        file_path = "s3://warehouse/data/part-0.parquet"

    class _DeleteFile:
        file_path = "s3://outside-bucket/deletes/part-0.parquet"

    class _Task:
        file = _DataFile()
        delete_files: ClassVar[set[object]] = {_DeleteFile()}

    enforcer = PathRuleEnforcer([{"root": "s3://warehouse/"}])
    with pytest.raises(PermissionError, match="Path is not allowed"):
        _check_file_tasks([_Task()], enforcer)


def test_iceberg_path_policy_checks_manifest_and_historical_metadata_locations() -> None:
    class _Snapshot:
        manifest_list = "s3://outside-bucket/manifests/snapshot.avro"

    class _MetadataLogEntry:
        metadata_file = "s3://outside-bucket/metadata-v1.json"

    class _Metadata:
        location = "s3://warehouse/table"
        snapshots: ClassVar[list[object]] = [_Snapshot()]
        metadata_log: ClassVar[list[object]] = [_MetadataLogEntry()]

    class _Table:
        metadata_location = "s3://warehouse/table/metadata.json"
        metadata = _Metadata()

    enforcer = PathRuleEnforcer([{"root": "s3://warehouse/"}])
    with pytest.raises(PermissionError, match="Path is not allowed"):
        _check_table_locations(_Table(), enforcer)


def test_iceberg_path_policy_checks_io_options_before_provider_load() -> None:
    enforcer = PathRuleEnforcer([{"root": "s3://warehouse/"}])
    with pytest.raises(PermissionError, match="Path is not allowed"):
        _check_io_options({"warehouse": "/outside/warehouse"}, enforcer)


def test_iceberg_execute_deserializes_sql_string_pushdown_filter(monkeypatch):
    captured: dict[str, object] = {}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    table = _FakeTable(schema_value=schema)
    table_format = IcebergTableFormat(
        catalog_name="analytics",
        table_name="users",
        metadata_location="/tmp/metadata.json",
        io_options={},
    )
    monkeypatch.setattr(IcebergTableFormat, "_load_table", lambda self: table)

    class _CapturingArrowScan:
        def __init__(self, *, table_metadata, io, projected_schema, row_filter) -> None:
            captured["row_filter"] = row_filter

        def to_record_batches(self, file_tasks):
            del file_tasks
            return iter(())

    monkeypatch.setattr(
        "dal_obscura.sources.iceberg.ArrowScan",
        _CapturingArrowScan,
    )

    file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path="/tmp/data.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=1,
        file_size_in_bytes=10,
    )
    file.spec_id = 0
    partition = IcebergInputPartition(
        columns=["id"],
        tasks=[encode_scan_task(FileScanTask(file))],
        backend_pushdown_row_filter="region = 'us'",
    )

    table_format.execute(partition)

    assert isinstance(captured["row_filter"], EqualTo)
    assert (
        row_filter_to_sql(deserialize_row_filter(partition.backend_pushdown_row_filter))
        == "region = 'us'"
    )


def test_iceberg_rejects_unproven_format_versions():
    from dal_obscura.sources.iceberg import (
        _require_supported_format_version,
    )

    _require_supported_format_version(2)

    import pytest

    with pytest.raises(ValueError, match="Unsupported Iceberg format version: 1"):
        _require_supported_format_version(1)
    with pytest.raises(ValueError, match="Unsupported Iceberg format version: 3"):
        _require_supported_format_version(3)


def test_iceberg_native_plan_is_split_and_pinned_to_its_metadata_snapshot(tmp_path):
    from pyiceberg.catalog import load_catalog

    from tests.support.iceberg import create_iceberg_table, iceberg_sql_catalog_options

    identifier = create_iceberg_table(
        tmp_path,
        "native_iceberg",
        "warehouse",
        append_batches=[[1, 2], [3, 4]],
    )
    catalog = load_catalog(
        "native_iceberg",
        **{
            key: str(value)
            for key, value in iceberg_sql_catalog_options(
                tmp_path, "native_iceberg", "warehouse"
            ).items()
        },
    )
    table = catalog.load_table(identifier)
    table_format = IcebergTableFormat(
        catalog_name="native_iceberg",
        table_name=identifier,
        metadata_location=table.metadata_location,
        io_options={},
    )

    planned = table_format.plan(
        PlanRequest(catalog="native_iceberg", target=identifier, columns=["id"]),
        max_tickets=8,
    )

    assert len(planned.tasks) == 2
    table.append(
        pa.table(
            {
                "id": [5],
                "email": ["user5@example.com"],
                "region": ["eu"],
            },
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("email", pa.string()),
                    pa.field("region", pa.string()),
                ]
            ),
        )
    )

    batches = []
    for task in planned.tasks:
        _schema, task_batches = table_format.execute(task.partition)
        batches.extend(task_batches)

    assert sorted(pa.Table.from_batches(batches).column("id").to_pylist()) == [1, 2, 3, 4]
    assert sorted(table.scan().to_arrow().column("id").to_pylist()) == [1, 2, 3, 4, 5]


def test_iceberg_native_schema_preserves_nested_field_ids(tmp_path):
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType, StructType

    catalog = load_catalog(
        "nested_iceberg",
        type="sql",
        uri=f"sqlite:///{tmp_path / 'nested_iceberg.db'}",
        warehouse=str(tmp_path / "warehouse"),
    )
    catalog.create_namespace("default")
    table = catalog.create_table(
        "default.users",
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="profile",
                field_type=StructType(
                    NestedField(
                        field_id=3,
                        name="email",
                        field_type=StringType(),
                        required=False,
                    )
                ),
                required=False,
            ),
        ),
        properties={"format-version": "2"},
    )
    arrow_schema = table.schema().as_arrow()
    table.append(
        pa.table({"id": [1], "profile": [{"email": "a@example.com"}]}, schema=arrow_schema)
    )
    table_format = IcebergTableFormat(
        catalog_name="nested_iceberg",
        table_name="default.users",
        metadata_location=table.metadata_location,
        io_options={},
    )

    schema = table_format.get_schema()
    assert schema.field("profile").metadata == {b"PARQUET:field_id": b"2"}
    assert schema.field("profile").type.field("email").metadata == {b"PARQUET:field_id": b"3"}

    plan = table_format.plan(
        PlanRequest(catalog="nested_iceberg", target="default.users", columns=["profile.email"]),
        max_tickets=1,
    )
    _output_schema, batches = table_format.execute(plan.tasks[0].partition)

    assert pa.Table.from_batches(list(batches)).column("profile").to_pylist() == [
        {"email": "a@example.com"}
    ]


def test_iceberg_stream_reads_one_batch_and_one_files_deletes_at_a_time(monkeypatch):
    from dal_obscura.sources import iceberg as iceberg

    visited = []
    closed = []
    deletes = []

    class Scan:
        def _record_batches_from_scan_tasks_and_deletes(self, tasks, delete_map):
            try:
                for index in range(3):
                    visited.append((tasks[0], index))
                    yield pa.record_batch([pa.array([index])], names=["id"])
            finally:
                closed.append(tasks[0])

    def read_deletes(io, tasks):
        deletes.extend(tasks)
        return {}

    monkeypatch.setattr(iceberg, "_read_all_delete_files", read_deletes)
    stream = iceberg._stream_iceberg_batches(
        cast(Any, Scan()), cast(Any, None), cast(Any, ["first", "second"])
    )
    assert deletes == []
    assert next(stream).num_rows == 1
    assert visited == [("first", 0)]
    assert deletes == ["first"]
    cast(Generator[pa.RecordBatch, None, None], stream).close()
    assert closed == ["first"]


@pytest.mark.parametrize("sql", ["id = NULL", "id <> NULL", "id > NULL", "id IN (1, NULL)"])
def test_iceberg_keeps_null_literal_predicates_in_core(sql):
    from dal_obscura.sources.iceberg import _split_row_filter

    row_filter = deserialize_row_filter(sql)
    pushdown, residual = _split_row_filter(row_filter)
    assert pushdown is None
    assert residual == row_filter
