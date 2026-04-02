from __future__ import annotations

import pickle
from dataclasses import dataclass

import pyarrow as pa
import pytest

from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.infrastructure.table_formats.iceberg import (
    IcebergInputPartition,
    IcebergTableFormat,
)


@dataclass(frozen=True, kw_only=True)
class _FakeArrowSchema:
    schema: pa.Schema

    def as_arrow(self) -> pa.Schema:
        return self.schema


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

    @property
    def metadata(self) -> object:
        return object()

    @property
    def io(self) -> object:
        return object()

    def schema(self) -> _FakeProjectedSchema:
        return _FakeProjectedSchema(schema=self.schema_value)

    def scan(self, *, selected_fields: tuple[str, ...]) -> _FakeScan:
        del selected_fields
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


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Phase 3: Iceberg execution still uses ALWAYS_TRUE instead of a "
        "policy-derived pushdown predicate"
    ),
)
def test_iceberg_execute_pushes_down_row_filter_instead_of_using_always_true(monkeypatch):
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
            captured["table_metadata"] = table_metadata
            captured["io"] = io
            captured["projected_schema"] = projected_schema
            captured["row_filter"] = row_filter

        def to_record_batches(self, file_tasks):
            del file_tasks
            return iter(())

    monkeypatch.setattr(
        "dal_obscura.infrastructure.table_formats.iceberg.ArrowScan",
        _CapturingArrowScan,
    )

    partition = IcebergInputPartition(columns=["id"], tasks=[pickle.dumps(object())])
    table_format.execute(partition)

    assert captured["row_filter"] == "region = 'us'"
