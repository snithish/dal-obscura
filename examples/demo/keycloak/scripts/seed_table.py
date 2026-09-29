from __future__ import annotations

import json
import os
from contextlib import suppress
from itertools import count
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    IcebergType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

DEMO_DIR = Path(os.environ.get("DEMO_DIR", "/workspace/demo"))
RUNTIME_DIR = DEMO_DIR / ".runtime"
FIXTURE_FILE = DEMO_DIR / "fixtures" / "demo_fixture.json"


def main() -> None:
    fixture = _read_fixture()
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    for table_fixture in fixture["tables"]:
        _create_iceberg_table(table_fixture)
    print(json.dumps({"tables": [table["target"] for table in fixture["tables"]]}))


def _read_fixture() -> dict[str, Any]:
    fixture = json.loads(FIXTURE_FILE.read_text(encoding="utf-8"))
    if not isinstance(fixture, dict):
        raise ValueError("fixture must be a JSON object")
    return fixture


def _create_iceberg_table(table_fixture: dict[str, Any]) -> None:
    catalog_name = str(table_fixture["catalog"])
    target = str(table_fixture["target"])
    warehouse = RUNTIME_DIR / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=f"sqlite:///{RUNTIME_DIR / f'{catalog_name}.db'}",
        warehouse=str(warehouse),
    )
    namespace = ".".join(target.split(".")[:-1])
    with suppress(Exception):
        if namespace:
            catalog.create_namespace(namespace)
    # Demo startup is intentionally restart-safe.  The warehouse is a durable
    # volume during a normal compose restart, so replacing the table here
    # would silently destroy data that an operator or test just wrote.  The
    # reset workflow removes the volume and is the explicit destructive path.
    if catalog.table_exists(target):
        return
    iceberg_schema, arrow_schema = _schemas(table_fixture["schema"])
    created = catalog.create_table(
        target,
        schema=iceberg_schema,
        properties={"format-version": "2"},
    )
    created.append(pa.Table.from_pylist(table_fixture["rows"], schema=arrow_schema))


def _schemas(fields: list[dict[str, Any]]) -> tuple[Schema, pa.Schema]:
    ids = count(1)

    def field_pair(field: dict[str, Any]) -> tuple[NestedField, pa.Field]:
        field_id = next(ids)
        iceberg_type, arrow_type = type_pair(field)
        required = bool(field.get("required", False))
        name = str(field["name"])
        return NestedField(
            field_id=field_id, name=name, field_type=iceberg_type, required=required
        ), pa.field(name, arrow_type, nullable=not required)

    def type_pair(spec: dict[str, Any]) -> tuple[IcebergType, pa.DataType]:
        kind = spec["type"]
        if kind == "long":
            return LongType(), pa.int64()
        if kind == "double":
            return DoubleType(), pa.float64()
        if kind == "string":
            return StringType(), pa.string()
        if kind == "struct":
            pairs = [field_pair(child) for child in spec["fields"]]
            return StructType(*[pair[0] for pair in pairs]), pa.struct([pair[1] for pair in pairs])
        if kind == "list":
            element_id = next(ids)
            iceberg_element, arrow_element = type_pair(spec["element"])
            return ListType(
                element_id=element_id, element=iceberg_element, element_required=False
            ), pa.list_(arrow_element)
        if kind == "map":
            key_id, value_id = next(ids), next(ids)
            iceberg_value, arrow_value = type_pair(spec["value"])
            return MapType(
                key_id=key_id,
                key_type=StringType(),
                value_id=value_id,
                value_type=iceberg_value,
                value_required=False,
            ), pa.map_(pa.string(), arrow_value)
        raise ValueError(f"unsupported fixture type {kind!r}")

    pairs = [field_pair(field) for field in fields]
    return Schema(*[pair[0] for pair in pairs]), pa.schema([pair[1] for pair in pairs])


if __name__ == "__main__":
    main()
