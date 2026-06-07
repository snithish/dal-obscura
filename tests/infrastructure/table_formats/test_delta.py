from __future__ import annotations

import pyarrow as pa
from deltalake import write_deltalake

from dal_obscura.common.query_planning.models import PlanRequest
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
