from __future__ import annotations

import pickle
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.expressions import AlwaysTrue, EqualTo

from dal_obscura.domain.access_control.filters import (
    deserialize_row_filter,
    parse_row_filter,
    row_filter_to_sql,
)
from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.infrastructure.table_formats.iceberg import (
    IcebergInputPartition,
    IcebergTableFormat,
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
    assert partition.pushdown_row_filter == "region = 'us'"
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
    assert partition.pushdown_row_filter == "active = TRUE"
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
    assert partition.pushdown_row_filter is None
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
    assert partition.pushdown_row_filter == "id > 1 AND region = 'us'"
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
        "dal_obscura.infrastructure.table_formats.iceberg.ArrowScan",
        _CapturingArrowScan,
    )

    partition = IcebergInputPartition(
        columns=["id"],
        tasks=[pickle.dumps(object())],
        pushdown_row_filter="region = 'us'",
    )

    table_format.execute(partition)

    assert isinstance(captured["row_filter"], EqualTo)
    assert (
        row_filter_to_sql(deserialize_row_filter(partition.pushdown_row_filter)) == "region = 'us'"
    )
