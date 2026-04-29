from __future__ import annotations

from contextlib import suppress
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


def _generated_column_values(field: pa.Field, batch_values: list[int]) -> list[Any]:
    if pa.types.is_integer(field.type) or field.name.endswith("_id") or field.name == "id":
        return batch_values
    if field.name == "email":
        return [f"user{i}@example.com" for i in batch_values]
    if field.name == "region":
        return ["us" if i % 2 == 0 else "eu" for i in batch_values]
    if pa.types.is_string(field.type):
        return [f"{field.name}-{i}" for i in batch_values]
    return batch_values


def _generated_batch_table(
    batch_values: list[int],
    *,
    schema: pa.Schema,
) -> pa.Table:
    return pa.table(
        {field.name: _generated_column_values(field, batch_values) for field in schema},
        schema=schema,
    )


def create_iceberg_table(
    tmp_path: Path,
    catalog_name: str,
    warehouse_name: str,
    values: list[int] | None = None,
    *,
    identifier: str = "default.users",
    append_batches: list[list[int]] | None = None,
    arrow_schema: pa.Schema | None = None,
    append_tables: list[pa.Table] | None = None,
    table_properties: dict[str, object] | None = None,
    partition_spec: PartitionSpec | None = None,
) -> str:
    warehouse = tmp_path / warehouse_name
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=f"sqlite:///{tmp_path / f'{catalog_name}.db'}",
        warehouse=str(warehouse),
    )
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="email", field_type=StringType(), required=False),
        NestedField(field_id=3, name="region", field_type=StringType(), required=False),
    )
    table_schema: Schema | pa.Schema = arrow_schema if arrow_schema is not None else schema
    namespace = ".".join(identifier.split(".")[:-1])
    with suppress(Exception):
        catalog.create_namespace(namespace)
    properties = {"format-version": "2", **(table_properties or {})}
    if partition_spec is None:
        table = catalog.create_table(
            identifier=identifier,
            schema=table_schema,
            properties=properties,
        )
    else:
        table = catalog.create_table(
            identifier=identifier,
            schema=table_schema,
            partition_spec=partition_spec,
            properties=properties,
        )
    default_arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("email", pa.string(), nullable=True),
            pa.field("region", pa.string(), nullable=True),
        ]
    )
    if append_tables is not None:
        for append_table in append_tables:
            table.append(append_table)
    else:
        append_schema = arrow_schema if arrow_schema is not None else default_arrow_schema
        batches = append_batches or [values or []]
        for batch_values in batches:
            table.append(_generated_batch_table(batch_values, schema=append_schema))
    return identifier


def iceberg_sql_catalog_options(
    tmp_path: Path,
    catalog_name: str,
    warehouse_name: str,
) -> dict[str, object]:
    return {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path / f'{catalog_name}.db'}",
        "warehouse": str(tmp_path / warehouse_name),
    }
