from __future__ import annotations

import pyarrow as pa
from deltalake import write_deltalake

from dal_obscura.common.access_control.filters import parse_row_filter
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.table_formats import delta as delta_module
from dal_obscura.data_plane.infrastructure.table_formats.delta import (
    DeltaInputPartition,
    DeltaTableFormat,
)


def test_delta_table_format_reads_schema_and_projected_batches(tmp_path):
    table_uri = tmp_path / "users"
    write_deltalake(
        table_uri,
        pa.table(
            {
                "id": [1, 2],
                "email": ["one@example.com", "two@example.com"],
                "region": ["us", "eu"],
            }
        ),
    )
    table_format = DeltaTableFormat(
        catalog_name="analytics",
        table_name="users",
        table_uri=str(table_uri),
    )

    plan = table_format.plan(
        PlanRequest(catalog="analytics", target="users", columns=["id", "region"]),
        max_tickets=4,
    )
    partition = plan.tasks[0].partition
    schema, batches = table_format.execute(partition)
    table = pa.Table.from_batches(list(batches), schema=schema)

    assert isinstance(partition, DeltaInputPartition)
    assert partition.columns == ["id", "region"]
    assert partition.version == 0
    assert plan.schema.names == ["id", "email", "region"]
    assert table.to_pydict() == {"id": [1, 2], "region": ["us", "eu"]}


def test_delta_table_format_pins_planned_version(tmp_path):
    table_uri = tmp_path / "users"
    write_deltalake(table_uri, pa.table({"id": [1], "region": ["us"]}))
    table_format = DeltaTableFormat(
        catalog_name="analytics",
        table_name="users",
        table_uri=str(table_uri),
    )
    plan = table_format.plan(
        PlanRequest(catalog="analytics", target="users", columns=["id"]),
        max_tickets=4,
    )
    write_deltalake(
        table_uri,
        pa.table({"id": [2], "region": ["eu"]}),
        mode="append",
    )

    schema, batches = table_format.execute(plan.tasks[0].partition)
    table = pa.Table.from_batches(list(batches), schema=schema)

    assert table.to_pydict() == {"id": [1]}


def test_delta_table_format_parallelizes_unfiltered_files(tmp_path):
    table_uri = tmp_path / "users"
    write_deltalake(table_uri, pa.table({"id": [1], "region": ["us"]}))
    write_deltalake(
        table_uri,
        pa.table({"id": [2], "region": ["eu"]}),
        mode="append",
    )
    table_format = DeltaTableFormat(
        catalog_name="analytics",
        table_name="users",
        table_uri=str(table_uri),
    )

    plan = table_format.plan(
        PlanRequest(
            catalog="analytics",
            target="users",
            columns=["id", "region"],
        ),
        max_tickets=8,
    )

    assert len(plan.tasks) == 2


def test_delta_table_format_streams_planned_fragments_without_concat(tmp_path, monkeypatch):
    table_uri = tmp_path / "users"
    write_deltalake(table_uri, pa.table({"id": [1], "region": ["us"]}))
    write_deltalake(
        table_uri,
        pa.table({"id": [2], "region": ["eu"]}),
        mode="append",
    )
    table_format = DeltaTableFormat(
        catalog_name="analytics",
        table_name="users",
        table_uri=str(table_uri),
    )
    plan = table_format.plan(
        PlanRequest(
            catalog="analytics",
            target="users",
            columns=["id", "region"],
        ),
        max_tickets=1,
    )

    def fail_concat_tables(*args, **kwargs):
        raise AssertionError("Delta execute must stream fragments without concat_tables")

    monkeypatch.setattr(delta_module.pa, "concat_tables", fail_concat_tables)
    schema, batches = table_format.execute(plan.tasks[0].partition)
    table = pa.Table.from_batches(list(batches), schema=schema)

    rows = sorted(table.to_pylist(), key=lambda row: row["id"])
    assert rows == [{"id": 1, "region": "us"}, {"id": 2, "region": "eu"}]


def test_delta_table_format_plans_pruned_file_tasks_and_applies_filter(tmp_path):
    table_uri = tmp_path / "users"
    write_deltalake(
        table_uri,
        pa.table({"id": [1], "region": ["us"]}),
        partition_by=["region"],
    )
    write_deltalake(
        table_uri,
        pa.table({"id": [2], "region": ["eu"]}),
        mode="append",
        partition_by=["region"],
    )
    table_format = DeltaTableFormat(
        catalog_name="analytics",
        table_name="users",
        table_uri=str(table_uri),
    )
    row_filter = parse_row_filter("region = 'us'", table_format.get_schema())

    plan = table_format.plan(
        PlanRequest(
            catalog="analytics",
            target="users",
            columns=["id", "region"],
            row_filter=row_filter,
        ),
        max_tickets=8,
    )
    tables = []
    for task in plan.tasks:
        schema, batches = table_format.execute(task.partition)
        tables.append(pa.Table.from_batches(list(batches), schema=schema))
    table = pa.concat_tables(tables)

    assert len(plan.tasks) == 1
    assert plan.backend_pushdown_row_filter == row_filter
    assert plan.residual_row_filter is None
    assert table.to_pydict() == {"id": [1], "region": ["us"]}


def test_delta_table_format_executes_integer_partition_columns(tmp_path):
    table_uri = tmp_path / "users"
    write_deltalake(
        table_uri,
        pa.table({"id": [1], "bucket": [1]}),
        partition_by=["bucket"],
    )
    table_format = DeltaTableFormat(
        catalog_name="analytics",
        table_name="users",
        table_uri=str(table_uri),
    )
    row_filter = parse_row_filter("bucket = 1", table_format.get_schema())

    plan = table_format.plan(
        PlanRequest(
            catalog="analytics",
            target="users",
            columns=["id", "bucket"],
            row_filter=row_filter,
        ),
        max_tickets=4,
    )
    schema, batches = table_format.execute(plan.tasks[0].partition)
    table = pa.Table.from_batches(list(batches), schema=schema)

    assert table.to_pydict() == {"id": [1], "bucket": [1]}


def test_delta_table_format_enforces_planned_data_file_paths(tmp_path):
    table_uri = tmp_path / "users"
    write_deltalake(table_uri, pa.table({"id": [1]}))
    table_format = DeltaTableFormat(
        catalog_name="analytics",
        table_name="users",
        table_uri=str(table_uri),
        path_enforcer=PathRuleEnforcer([{"root": str(table_uri / "_delta_log")}]),
    )

    try:
        table_format.plan(
            PlanRequest(catalog="analytics", target="users", columns=["id"]),
            max_tickets=4,
        )
    except PermissionError as exc:
        assert str(exc) == "Path is not allowed"
    else:
        raise AssertionError("expected planned data file path enforcement")


def test_delta_table_format_returns_empty_task_when_filter_prunes_all_files(tmp_path):
    table_uri = tmp_path / "users"
    write_deltalake(
        table_uri,
        pa.table({"id": [1], "region": ["us"]}),
        partition_by=["region"],
    )
    table_format = DeltaTableFormat(
        catalog_name="analytics",
        table_name="users",
        table_uri=str(table_uri),
    )
    row_filter = parse_row_filter("region = 'eu'", table_format.get_schema())

    plan = table_format.plan(
        PlanRequest(
            catalog="analytics",
            target="users",
            columns=["id", "region"],
            row_filter=row_filter,
        ),
        max_tickets=4,
    )
    schema, batches = table_format.execute(plan.tasks[0].partition)
    table = pa.Table.from_batches(list(batches), schema=schema)

    assert len(plan.tasks) == 1
    assert table.num_rows == 0
