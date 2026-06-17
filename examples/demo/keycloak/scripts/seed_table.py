from __future__ import annotations

import json
import os
import shutil
from contextlib import suppress
from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import write_deltalake
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

DEMO_DIR = Path(os.environ.get("DEMO_DIR", "/workspace/demo"))
RUNTIME_DIR = DEMO_DIR / ".runtime"
FIXTURE_FILE = DEMO_DIR / "fixtures" / "demo_fixture.json"


def main() -> None:
    fixture = _read_fixture()
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    for table_fixture in fixture["tables"]:
        backend = str(table_fixture["backend"])
        if backend == "iceberg":
            _create_iceberg_table(table_fixture)
        elif backend == "delta":
            _create_delta_table(table_fixture)
        else:
            raise ValueError(f"unsupported demo backend {backend!r}")
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
    with suppress(Exception):
        catalog.drop_table(target)
    iceberg_schema, arrow_schema = _schemas(table_fixture["schema"])
    created = catalog.create_table(
        target,
        schema=iceberg_schema,
        properties={"format-version": "2"},
    )
    created.append(pa.Table.from_pylist(table_fixture["rows"], schema=arrow_schema))


def _create_delta_table(table_fixture: dict[str, Any]) -> None:
    table_path = Path(str(table_fixture["table_path"]))
    if table_path.exists():
        shutil.rmtree(table_path)
    _, arrow_schema = _schemas(table_fixture["schema"])
    write_deltalake(
        str(table_path),
        pa.Table.from_pylist(table_fixture["rows"], schema=arrow_schema),
    )


def _schemas(fields: list[dict[str, Any]]) -> tuple[Schema, pa.Schema]:
    iceberg_fields: list[NestedField] = []
    arrow_fields: list[pa.Field] = []
    for index, field in enumerate(fields, start=1):
        name = str(field["name"])
        type_name = str(field["type"])
        required = bool(field.get("required", False))
        if type_name == "long":
            iceberg_type = LongType()
            arrow_type = pa.int64()
        elif type_name == "double":
            iceberg_type = DoubleType()
            arrow_type = pa.float64()
        elif type_name == "string":
            iceberg_type = StringType()
            arrow_type = pa.string()
        else:
            raise ValueError(f"unsupported fixture type {type_name!r}")
        iceberg_fields.append(
            NestedField(field_id=index, name=name, field_type=iceberg_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))
    return Schema(*iceberg_fields), pa.schema(arrow_fields)


if __name__ == "__main__":
    main()
