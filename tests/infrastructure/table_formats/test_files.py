from __future__ import annotations

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

from dal_obscura.common.access_control.filters import parse_row_filter
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.data_plane.infrastructure.table_formats import files as files_module
from dal_obscura.data_plane.infrastructure.table_formats.files import (
    ArrowDatasetTableFormat,
    AvroTableFormat,
    FileInputPartition,
    TextTableFormat,
)


def test_arrow_dataset_table_format_reads_parquet_projection(tmp_path):
    path = tmp_path / "events.parquet"
    pq.write_table(
        pa.table({"id": [1, 2], "region": ["us", "eu"], "hidden": ["a", "b"]}),
        path,
    )
    table_format = ArrowDatasetTableFormat(
        catalog_name="files",
        table_name="events",
        uri=str(path),
        format="parquet",
    )

    plan = table_format.plan(
        PlanRequest(catalog="files", target="events", columns=["id", "region"]),
        max_tickets=4,
    )
    schema, batches = table_format.execute(plan.tasks[0].partition)
    table = pa.Table.from_batches(list(batches), schema=schema)

    assert isinstance(plan.tasks[0].partition, FileInputPartition)
    assert plan.schema.names == ["id", "region", "hidden"]
    assert table.to_pydict() == {"id": [1, 2], "region": ["us", "eu"]}


def test_arrow_dataset_table_format_plans_one_task_per_pruned_file(tmp_path):
    root = tmp_path / "events"
    (root / "region=us").mkdir(parents=True)
    (root / "region=eu").mkdir(parents=True)
    pq.write_table(pa.table({"id": [1]}), root / "region=us" / "part.parquet")
    pq.write_table(pa.table({"id": [2]}), root / "region=eu" / "part.parquet")
    table_format = ArrowDatasetTableFormat(
        catalog_name="files",
        table_name="events",
        uri=str(root),
        format="parquet",
        options={"partitioning": "hive"},
    )
    row_filter = parse_row_filter("region = 'us'", table_format.get_schema())

    plan = table_format.plan(
        PlanRequest(
            catalog="files",
            target="events",
            columns=["id", "region"],
            row_filter=row_filter,
        ),
        max_tickets=4,
    )
    tables = [_execute_to_table(table_format, task.partition) for task in plan.tasks]
    table = pa.concat_tables(tables)

    assert len(plan.tasks) == 1
    assert plan.backend_pushdown_row_filter == row_filter
    assert plan.residual_row_filter is None
    assert table.to_pydict() == {"id": [1], "region": ["us"]}


def test_arrow_dataset_table_format_parallelizes_unfiltered_files(tmp_path):
    root = tmp_path / "events"
    root.mkdir()
    pq.write_table(pa.table({"id": [1]}), root / "part-a.parquet")
    pq.write_table(pa.table({"id": [2]}), root / "part-b.parquet")
    table_format = ArrowDatasetTableFormat(
        catalog_name="files",
        table_name="events",
        uri=str(root),
        format="parquet",
    )

    plan = table_format.plan(
        PlanRequest(catalog="files", target="events", columns=["id"]),
        max_tickets=8,
    )

    assert len(plan.tasks) == 2


def test_arrow_dataset_table_format_accepts_scientific_numeric_filter(tmp_path):
    path = tmp_path / "events.parquet"
    pq.write_table(pa.table({"value": [100.0, 200.0]}), path)
    table_format = ArrowDatasetTableFormat(
        catalog_name="files",
        table_name="events",
        uri=str(path),
        format="parquet",
    )
    row_filter = parse_row_filter("value = 1e2", table_format.get_schema())

    plan = table_format.plan(
        PlanRequest(
            catalog="files",
            target="events",
            columns=["value"],
            row_filter=row_filter,
        ),
        max_tickets=4,
    )

    assert plan.backend_pushdown_row_filter == row_filter
    assert plan.residual_row_filter is None


def test_arrow_dataset_table_format_returns_empty_task_when_filter_prunes_all_files(tmp_path):
    root = tmp_path / "events"
    (root / "region=us").mkdir(parents=True)
    pq.write_table(pa.table({"id": [1]}), root / "region=us" / "part.parquet")
    table_format = ArrowDatasetTableFormat(
        catalog_name="files",
        table_name="events",
        uri=str(root),
        format="parquet",
        options={"partitioning": "hive"},
    )
    row_filter = parse_row_filter("region = 'eu'", table_format.get_schema())

    plan = table_format.plan(
        PlanRequest(
            catalog="files",
            target="events",
            columns=["id", "region"],
            row_filter=row_filter,
        ),
        max_tickets=4,
    )
    schema, batches = table_format.execute(plan.tasks[0].partition)
    table = pa.Table.from_batches(list(batches), schema=schema)

    assert len(plan.tasks) == 1
    assert table.num_rows == 0


def test_avro_table_format_reads_projection(tmp_path):
    path = tmp_path / "events.avro"
    schema = {
        "type": "record",
        "name": "Event",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "region", "type": "string"},
        ],
    }
    with path.open("wb") as handle:
        fastavro.writer(handle, schema, [{"id": 1, "region": "us"}, {"id": 2, "region": "eu"}])
    table_format = AvroTableFormat(
        catalog_name="files",
        table_name="events",
        uri=str(path),
    )

    plan = table_format.plan(
        PlanRequest(catalog="files", target="events", columns=["id"]),
        max_tickets=4,
    )
    schema, batches = table_format.execute(plan.tasks[0].partition)
    table = pa.Table.from_batches(list(batches), schema=schema)

    assert plan.schema.names == ["id", "region"]
    assert table.to_pydict() == {"id": [1, 2]}


def test_avro_table_format_plans_directory_files_and_applies_filter(tmp_path):
    root = tmp_path / "events"
    root.mkdir()
    _write_avro(root / "part-us.avro", [{"id": 1, "region": "us"}])
    _write_avro(root / "part-eu.avro", [{"id": 2, "region": "eu"}])
    table_format = AvroTableFormat(
        catalog_name="files",
        table_name="events",
        uri=str(root),
    )
    row_filter = parse_row_filter("region = 'us'", table_format.get_schema())

    plan = table_format.plan(
        PlanRequest(
            catalog="files",
            target="events",
            columns=["id", "region"],
            row_filter=row_filter,
        ),
        max_tickets=4,
    )
    table = pa.concat_tables(
        [_execute_to_table(table_format, task.partition) for task in plan.tasks]
    )

    assert len(plan.tasks) == 2
    assert plan.backend_pushdown_row_filter is None
    assert plan.residual_row_filter == row_filter
    assert table.to_pydict() == {"id": [1], "region": ["us"]}


def test_avro_table_format_filters_per_file_before_concat(tmp_path, monkeypatch):
    root = tmp_path / "events"
    root.mkdir()
    _write_avro(root / "part-us.avro", [{"id": 1, "region": "us"}])
    _write_avro(root / "part-eu.avro", [{"id": 2, "region": "eu"}])
    table_format = AvroTableFormat(
        catalog_name="files",
        table_name="events",
        uri=str(root),
    )
    row_filter = parse_row_filter("region = 'us'", table_format.get_schema())
    plan = table_format.plan(
        PlanRequest(
            catalog="files",
            target="events",
            columns=["id", "region"],
            row_filter=row_filter,
        ),
        max_tickets=1,
    )

    def fail_concat_tables(*args, **kwargs):
        del args, kwargs
        raise AssertionError("Avro execute should stream/filter files without pa.concat_tables")

    monkeypatch.setattr(files_module.pa, "concat_tables", fail_concat_tables)

    schema, batches = table_format.execute(plan.tasks[0].partition)
    table = pa.Table.from_batches(list(batches), schema=schema)

    assert table.to_pydict() == {"id": [1], "region": ["us"]}


def test_text_table_format_reads_lines_into_configured_column(tmp_path):
    path = tmp_path / "events.txt"
    path.write_text("first\nsecond\n")
    table_format = TextTableFormat(
        catalog_name="files",
        table_name="events",
        uri=str(path),
        column_name="line",
    )

    plan = table_format.plan(
        PlanRequest(catalog="files", target="events", columns=["line"]),
        max_tickets=4,
    )
    schema, batches = table_format.execute(plan.tasks[0].partition)
    table = pa.Table.from_batches(list(batches), schema=schema)

    assert plan.schema.names == ["line"]
    assert table.to_pydict() == {"line": ["first", "second"]}


def test_text_table_format_plans_directory_files_and_applies_filter(tmp_path):
    root = tmp_path / "logs"
    root.mkdir()
    (root / "part-a.txt").write_text("keep\n")
    (root / "part-b.txt").write_text("drop\n")
    table_format = TextTableFormat(
        catalog_name="files",
        table_name="logs",
        uri=str(root),
        column_name="line",
    )
    row_filter = parse_row_filter("line = 'keep'", table_format.get_schema())

    plan = table_format.plan(
        PlanRequest(
            catalog="files",
            target="logs",
            columns=["line"],
            row_filter=row_filter,
        ),
        max_tickets=4,
    )
    table = pa.concat_tables(
        [_execute_to_table(table_format, task.partition) for task in plan.tasks]
    )

    assert len(plan.tasks) == 2
    assert plan.backend_pushdown_row_filter is None
    assert plan.residual_row_filter == row_filter
    assert table.to_pydict() == {"line": ["keep"]}


def test_text_table_format_streams_batches(tmp_path):
    path = tmp_path / "events.txt"
    path.write_text("".join(f"line-{index}\n" for index in range(9000)))
    table_format = TextTableFormat(
        catalog_name="files",
        table_name="events",
        uri=str(path),
        column_name="line",
    )
    plan = table_format.plan(
        PlanRequest(catalog="files", target="events", columns=["line"]),
        max_tickets=4,
    )

    schema, batches = table_format.execute(plan.tasks[0].partition)
    batch_list = list(batches)
    table = pa.Table.from_batches(batch_list, schema=schema)

    assert len(batch_list) > 1
    assert table.num_rows == 9000
    assert table.column("line")[0].as_py() == "line-0"
    assert table.column("line")[-1].as_py() == "line-8999"


def _execute_to_table(table_format, partition):
    schema, batches = table_format.execute(partition)
    return pa.Table.from_batches(list(batches), schema=schema)


def _write_avro(path, records):
    schema = {
        "type": "record",
        "name": "Event",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "region", "type": "string"},
        ],
    }
    with path.open("wb") as handle:
        fastavro.writer(handle, schema, records)
