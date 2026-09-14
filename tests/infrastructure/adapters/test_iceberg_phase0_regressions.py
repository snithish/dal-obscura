from __future__ import annotations

import pickle
from dataclasses import dataclass
from typing import ClassVar

import pyarrow as pa
import pytest
from pyiceberg.expressions import AlwaysTrue, EqualTo

from dal_obscura.common.access_control.filters import (
    deserialize_row_filter,
    parse_row_filter,
    row_filter_to_sql,
)
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.table_formats.iceberg import (
    IcebergInputPartition,
    IcebergTableFormat,
    _check_file_tasks,
    _check_table_locations,
)


@dataclass(frozen=True, kw_only=True)
class _FakeProjectedSchema:
    schema: pa.Schema

    def as_arrow(self) -> pa.Schema:
        return self.schema

    def select(self, *columns: str) -> _FakeProjectedSchema:
        return _FakeProjectedSchema(
            schema=pa.schema([self.schema.field(column) for column in columns])
        )


@dataclass(frozen=True, kw_only=True)
class _FakeScan:
    tasks: list[object]

    def plan_files(self) -> list[object]:
        return self.tasks


@dataclass(frozen=True, kw_only=True)
class _FakeTable:
    schema_value: pa.Schema
    planned_row_filter: object | None = None
    planned_selected_fields: tuple[str, ...] | None = None

    @property
    def metadata(self) -> object:
        return object()

    @property
    def io(self) -> object:
        return object()

    def schema(self) -> _FakeProjectedSchema:
        return _FakeProjectedSchema(schema=self.schema_value)

    def scan(self, *, row_filter, selected_fields: tuple[str, ...]) -> _FakeScan:
        object.__setattr__(self, "planned_row_filter", row_filter)
        object.__setattr__(self, "planned_selected_fields", selected_fields)
        return _FakeScan(tasks=[object()])


def test_iceberg_plan_tracks_requested_projection_for_baseline_behavior(monkeypatch):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    table = _FakeTable(schema_value=schema)
    table_format = IcebergTableFormat(
        catalog_name="analytics",
        table_name="users",
        metadata_location="/tmp/metadata.json",
        io_options={},
    )
    monkeypatch.setattr(IcebergTableFormat, "_load_table", lambda self: table)

    plan = table_format.plan(
        PlanRequest(catalog="analytics", target="users", columns=["id"]),
        max_tickets=1,
    )

    partition = plan.tasks[0].partition
    assert isinstance(partition, IcebergInputPartition)
    assert partition.columns == ["id"]
    assert table.planned_selected_fields == ("id",)
    assert isinstance(table.planned_row_filter, AlwaysTrue)


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


def test_iceberg_plan_pushes_down_simple_row_filter_as_sql_string(monkeypatch):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    table = _FakeTable(schema_value=schema)
    table_format = IcebergTableFormat(
        catalog_name="analytics",
        table_name="users",
        metadata_location="/tmp/metadata.json",
        io_options={},
    )
    monkeypatch.setattr(IcebergTableFormat, "_load_table", lambda self: table)

    plan = table_format.plan(
        PlanRequest(
            catalog="analytics",
            target="users",
            columns=["id"],
            row_filter=parse_row_filter("region = 'us'", schema),
        ),
        max_tickets=1,
    )

    partition = plan.tasks[0].partition
    assert isinstance(partition, IcebergInputPartition)
    assert isinstance(table.planned_row_filter, EqualTo)
    assert partition.backend_pushdown_row_filter == "region = 'us'"
    assert plan.full_row_filter is not None
    assert row_filter_to_sql(plan.full_row_filter) == "region = 'us'"
    assert plan.backend_pushdown_row_filter is not None
    assert row_filter_to_sql(plan.backend_pushdown_row_filter) == "region = 'us'"
    assert plan.residual_row_filter is None


def test_iceberg_plan_splits_function_filter_into_pushdown_and_residual(monkeypatch):
    schema = pa.schema(
        [
            pa.field("region", pa.string()),
            pa.field("active", pa.bool_()),
        ]
    )
    table = _FakeTable(schema_value=schema)
    table_format = IcebergTableFormat(
        catalog_name="analytics",
        table_name="users",
        metadata_location="/tmp/metadata.json",
        io_options={},
    )
    monkeypatch.setattr(IcebergTableFormat, "_load_table", lambda self: table)

    plan = table_format.plan(
        PlanRequest(
            catalog="analytics",
            target="users",
            columns=["region", "active"],
            row_filter=parse_row_filter("lower(region) = 'us' AND active = true", schema),
        ),
        max_tickets=1,
    )

    partition = plan.tasks[0].partition
    assert isinstance(partition, IcebergInputPartition)
    assert partition.backend_pushdown_row_filter == "active = TRUE"
    assert plan.full_row_filter is not None
    assert row_filter_to_sql(plan.full_row_filter) == "LOWER(region) = 'us' AND active = TRUE"
    assert plan.backend_pushdown_row_filter is not None
    assert row_filter_to_sql(plan.backend_pushdown_row_filter) == "active = TRUE"
    assert plan.residual_row_filter is not None
    assert row_filter_to_sql(plan.residual_row_filter) == "LOWER(region) = 'us'"


def test_iceberg_plan_keeps_computed_expression_fully_residual(monkeypatch):
    schema = pa.schema([pa.field("id", pa.int64())])
    table = _FakeTable(schema_value=schema)
    table_format = IcebergTableFormat(
        catalog_name="analytics",
        table_name="users",
        metadata_location="/tmp/metadata.json",
        io_options={},
    )
    monkeypatch.setattr(IcebergTableFormat, "_load_table", lambda self: table)

    plan = table_format.plan(
        PlanRequest(
            catalog="analytics",
            target="users",
            columns=["id"],
            row_filter=parse_row_filter("id + 1 > 5", schema),
        ),
        max_tickets=1,
    )

    partition = plan.tasks[0].partition
    assert isinstance(partition, IcebergInputPartition)
    assert partition.backend_pushdown_row_filter is None
    assert plan.full_row_filter is not None
    assert row_filter_to_sql(plan.full_row_filter) == "id + 1 > 5"
    assert plan.backend_pushdown_row_filter is None
    assert plan.residual_row_filter is not None
    assert row_filter_to_sql(plan.residual_row_filter) == "id + 1 > 5"


def test_iceberg_plan_splits_combined_filters_with_safe_and_residual_clauses(monkeypatch):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    table = _FakeTable(schema_value=schema)
    table_format = IcebergTableFormat(
        catalog_name="analytics",
        table_name="users",
        metadata_location="/tmp/metadata.json",
        io_options={},
    )
    monkeypatch.setattr(IcebergTableFormat, "_load_table", lambda self: table)

    plan = table_format.plan(
        PlanRequest(
            catalog="analytics",
            target="users",
            columns=["id", "region"],
            row_filter=parse_row_filter(
                "id > 1 AND region = 'us' AND LOWER(region) = 'us'",
                schema,
            ),
        ),
        max_tickets=1,
    )

    partition = plan.tasks[0].partition
    assert isinstance(partition, IcebergInputPartition)
    assert partition.backend_pushdown_row_filter == "id > 1 AND region = 'us'"
    assert plan.full_row_filter is not None
    assert (
        row_filter_to_sql(plan.full_row_filter)
        == "id > 1 AND region = 'us' AND LOWER(region) = 'us'"
    )
    assert plan.backend_pushdown_row_filter is not None
    assert row_filter_to_sql(plan.backend_pushdown_row_filter) == "id > 1 AND region = 'us'"
    assert plan.residual_row_filter is not None
    assert row_filter_to_sql(plan.residual_row_filter) == "LOWER(region) = 'us'"


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
        "dal_obscura.data_plane.infrastructure.table_formats.iceberg.ArrowScan",
        _CapturingArrowScan,
    )

    partition = IcebergInputPartition(
        columns=["id"],
        tasks=[pickle.dumps(object())],
        backend_pushdown_row_filter="region = 'us'",
    )

    table_format.execute(partition)

    assert isinstance(captured["row_filter"], EqualTo)
    assert (
        row_filter_to_sql(deserialize_row_filter(partition.backend_pushdown_row_filter))
        == "region = 'us'"
    )


def test_iceberg_rejects_unproven_format_versions():
    from dal_obscura.data_plane.infrastructure.table_formats.iceberg import (
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
