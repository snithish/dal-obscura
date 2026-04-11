from __future__ import annotations

from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

from tests.support.iceberg import create_iceberg_table


def _load_sql_catalog(tmp_path: Path, catalog_name: str, warehouse_name: str):
    return load_catalog(
        catalog_name,
        type="sql",
        uri=f"sqlite:///{tmp_path / f'{catalog_name}.db'}",
        warehouse=str(tmp_path / warehouse_name),
    )


def _sorted_table_dict(table: pa.Table, column: str) -> dict[str, list[object]]:
    return table.sort_by([(column, "ascending")]).to_pydict()


def test_create_iceberg_table_keeps_default_compatibility(tmp_path: Path):
    table_id = create_iceberg_table(tmp_path, "iceberg_catalog", "warehouse", [1, 2, 3])

    assert table_id == "default.users"

    catalog = _load_sql_catalog(tmp_path, "iceberg_catalog", "warehouse")
    table = catalog.load_table(table_id)

    assert table.scan().to_arrow().to_pydict() == {
        "id": [1, 2, 3],
        "email": [
            "user1@example.com",
            "user2@example.com",
            "user3@example.com",
        ],
        "region": ["eu", "us", "eu"],
    }


def test_create_iceberg_table_supports_custom_arrow_schema_and_append_tables(
    tmp_path: Path,
):
    catalog_name = "custom_catalog"
    warehouse_name = "custom-warehouse"
    arrow_schema = pa.schema(
        [
            pa.field("user_id", pa.int64(), nullable=False),
            pa.field("display_name", pa.string(), nullable=False),
            pa.field("region", pa.string(), nullable=True),
        ]
    )
    append_tables = [
        pa.table(
            {
                "user_id": [10, 11],
                "display_name": ["Ada", "Bea"],
                "region": ["eu", "us"],
            },
            schema=arrow_schema,
        ),
        pa.table(
            {
                "user_id": [12],
                "display_name": ["Cy"],
                "region": ["eu"],
            },
            schema=arrow_schema,
        ),
    ]

    table_id = create_iceberg_table(
        tmp_path,
        catalog_name,
        warehouse_name,
        arrow_schema=arrow_schema,
        append_tables=append_tables,
        identifier="analytics.events",
        table_properties={"custom-property": "enabled"},
    )

    assert table_id == "analytics.events"

    catalog = _load_sql_catalog(tmp_path, catalog_name, warehouse_name)
    table = catalog.load_table(table_id)

    assert table.metadata.properties["custom-property"] == "enabled"
    assert _sorted_table_dict(table.scan().to_arrow(), "user_id") == {
        "user_id": [10, 11, 12],
        "display_name": ["Ada", "Bea", "Cy"],
        "region": ["eu", "us", "eu"],
    }


def test_create_iceberg_table_supports_custom_arrow_schema_with_generated_rows(
    tmp_path: Path,
):
    arrow_schema = pa.schema(
        [
            pa.field("user_id", pa.int64(), nullable=False),
            pa.field("display_name", pa.string(), nullable=False),
        ]
    )

    table_id = create_iceberg_table(
        tmp_path,
        "generated_catalog",
        "generated-warehouse",
        [1, 2],
        arrow_schema=arrow_schema,
        identifier="analytics.people",
    )

    assert table_id == "analytics.people"

    catalog = _load_sql_catalog(tmp_path, "generated_catalog", "generated-warehouse")
    table = catalog.load_table(table_id)

    assert _sorted_table_dict(table.scan().to_arrow(), "user_id") == {
        "user_id": [1, 2],
        "display_name": ["display_name-1", "display_name-2"],
    }


def test_create_iceberg_table_accepts_partition_spec(tmp_path: Path):
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=3,
            field_id=1000,
            transform=IdentityTransform(),
            name="region",
        )
    )

    table_id = create_iceberg_table(
        tmp_path,
        "partition_catalog",
        "partition-warehouse",
        [1, 2, 3],
        partition_spec=partition_spec,
    )

    assert table_id == "default.users"

    catalog = _load_sql_catalog(tmp_path, "partition_catalog", "partition-warehouse")
    table = catalog.load_table(table_id)

    assert table.spec().fields[0].name == "region"
    assert table.spec().fields[0].transform == IdentityTransform()
    assert _sorted_table_dict(table.scan().to_arrow(), "id")["id"] == [1, 2, 3]
