from __future__ import annotations

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

from dal_obscura.common.query_planning.models import PlanRequest
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


def test_arrow_dataset_table_format_reads_json_projection(tmp_path):
    path = tmp_path / "events.json"
    path.write_text('{"id": 1, "region": "us"}\n{"id": 2, "region": "eu"}\n')
    table_format = ArrowDatasetTableFormat(
        catalog_name="files",
        table_name="events",
        uri=str(path),
        format="json",
    )

    plan = table_format.plan(
        PlanRequest(catalog="files", target="events", columns=["region"]),
        max_tickets=4,
    )
    schema, batches = table_format.execute(plan.tasks[0].partition)
    table = pa.Table.from_batches(list(batches), schema=schema)

    assert table.to_pydict() == {"region": ["us", "eu"]}


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
