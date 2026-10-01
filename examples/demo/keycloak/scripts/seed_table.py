from __future__ import annotations

import json
import os
from contextlib import suppress
from itertools import count
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
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

DEMO_DIR = Path(__file__).resolve().parents[1]
FIXTURE_FILE = DEMO_DIR / "fixtures" / "demo_fixture.json"


def main() -> None:
    fixture = _read_fixture()
    for table_fixture in fixture["tables"]:
        _create_iceberg_table(table_fixture)
    print(json.dumps({"tables": [table["target"] for table in fixture["tables"]]}))


def _read_fixture() -> dict[str, Any]:
    fixture = json.loads(FIXTURE_FILE.read_text(encoding="utf-8"))
    if not isinstance(fixture, dict):
        raise ValueError("fixture must be a JSON object")
    return fixture


def _catalog_options() -> dict[str, str]:
    return {
        "type": "sql",
        "uri": os.environ["ICEBERG_CATALOG_URI"],
        "warehouse": os.environ.get("ICEBERG_WAREHOUSE", "/warehouse"),
    }


def _create_iceberg_table(table_fixture: dict[str, Any]) -> None:
    catalog = load_catalog(str(table_fixture["catalog"]), **_catalog_options())
    target = str(table_fixture["target"])
    with suppress(NamespaceAlreadyExistsError):
        catalog.create_namespace(tuple(target.split(".")[:-1]))
    if catalog.table_exists(target):
        return
    iceberg_schema, arrow_schema = _schemas(table_fixture["schema"])
    rows = pa.Table.from_pylist(table_fixture["rows"], schema=arrow_schema)
    # Both appends are staged; publish the table only after all data succeeds.
    # An interrupted first seed can be retried without exposing partial rows.
    with catalog.create_table_transaction(
        target, schema=iceberg_schema, properties={"format-version": "2"}
    ) as transaction:
        midpoint = max(1, rows.num_rows // 2)
        transaction.append(rows.slice(0, midpoint))
        transaction.append(rows.slice(midpoint))


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
