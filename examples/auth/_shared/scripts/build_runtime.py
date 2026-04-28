from __future__ import annotations

import csv
import json
import os
from contextlib import suppress
from pathlib import Path
from typing import Any

import pyarrow as pa
import yaml
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DoubleType, IntegerType, LongType, NestedField, StringType

RUNTIME_DIR = Path(os.environ.get("RUNTIME_DIR", "/workspace/runtime"))
EXAMPLE_ROOT = Path(os.environ.get("EXAMPLE_ROOT", "/workspace/example"))
FIXTURE_FILE = Path(os.environ.get("FIXTURE_FILE", EXAMPLE_ROOT / "fixture" / "fixture.yaml"))
AUTH_CONFIG_FILE = Path(os.environ.get("AUTH_CONFIG_FILE", EXAMPLE_ROOT / "config" / "auth.yaml"))
TRANSPORT_CONFIG_FILE = Path(
    os.environ.get("TRANSPORT_CONFIG_FILE", EXAMPLE_ROOT / "config" / "transport.yaml")
)

TYPE_BUILDERS: dict[str, tuple[type, pa.DataType]] = {
    "string": (StringType, pa.string()),
    "long": (LongType, pa.int64()),
    "int": (IntegerType, pa.int32()),
    "double": (DoubleType, pa.float64()),
    "boolean": (BooleanType, pa.bool_()),
}


def main() -> None:
    fixture = _load_yaml(FIXTURE_FILE, "Fixture")
    catalog_name = str(_require_key(fixture, "catalog")).strip()
    if not catalog_name:
        raise ValueError("Fixture catalog must be non-empty")

    tables = _read_tables(fixture)
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    _create_tables(catalog_name, tables)
    _write_catalog_config(catalog_name)
    _write_policy_config(catalog_name, fixture, tables)
    _write_app_config()
    print(json.dumps({"catalog": catalog_name, "tables": [table["target"] for table in tables]}))


def _read_tables(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    raw_tables = fixture.get("tables")
    if not isinstance(raw_tables, list) or not raw_tables:
        raise ValueError("Fixture must define a non-empty tables list")
    tables: list[dict[str, Any]] = []
    for raw_table in raw_tables:
        if not isinstance(raw_table, dict):
            raise ValueError("Each fixture table must be a YAML object")
        target = str(_require_key(raw_table, "target")).strip()
        if not target:
            raise ValueError("Fixture table target must be non-empty")
        schema = raw_table.get("schema")
        if not isinstance(schema, list) or not schema:
            raise ValueError(f"Fixture table {target!r} must define a non-empty schema")
        tables.append({"target": target, "schema": schema, "rows": _table_rows(raw_table, target)})
    return tables


def _table_rows(table: dict[str, Any], target: str) -> list[dict[str, Any]]:
    rows = table.get("rows", [])
    csv_path = table.get("csv")
    if rows and csv_path:
        raise ValueError(f"Fixture table {target!r} must not define both rows and csv")
    if rows:
        if not isinstance(rows, list):
            raise ValueError(f"Fixture table {target!r} rows must be a list")
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError(f"Fixture table {target!r} rows must be YAML objects")
            normalized_rows.append(dict(row))
        return normalized_rows
    if not csv_path:
        return []
    csv_file = EXAMPLE_ROOT / str(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(csv_file)
    with csv_file.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _create_tables(catalog_name: str, tables: list[dict[str, Any]]) -> None:
    warehouse = RUNTIME_DIR / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=f"sqlite:///{RUNTIME_DIR / f'{catalog_name}.db'}",
        warehouse=str(warehouse),
    )
    for table in tables:
        target = str(table["target"])
        namespace = ".".join(target.split(".")[:-1])
        with suppress(Exception):
            if namespace:
                catalog.create_namespace(namespace)
        with suppress(Exception):
            catalog.drop_table(target)
        iceberg_schema, arrow_schema = _schemas(table["schema"], target)
        created = catalog.create_table(
            target,
            schema=iceberg_schema,
            properties={"format-version": "2"},
        )
        created.append(
            pa.Table.from_pylist(
                _cast_rows(table["rows"], table["schema"], target),
                schema=arrow_schema,
            )
        )


def _schemas(fields: list[dict[str, Any]], target: str) -> tuple[Schema, pa.Schema]:
    iceberg_fields: list[NestedField] = []
    arrow_fields: list[pa.Field] = []
    for index, raw_field in enumerate(fields, start=1):
        if not isinstance(raw_field, dict):
            raise ValueError(f"Fixture table {target!r} schema entries must be YAML objects")
        name = str(_require_key(raw_field, "name")).strip()
        type_name = str(_require_key(raw_field, "type")).strip().lower()
        required = bool(raw_field.get("required", False))
        if type_name not in TYPE_BUILDERS:
            supported = ", ".join(sorted(TYPE_BUILDERS))
            raise ValueError(
                f"Fixture table {target!r} field {name!r} uses unsupported "
                f"type {type_name!r}; supported: {supported}"
            )
        iceberg_type, arrow_type = TYPE_BUILDERS[type_name]
        iceberg_fields.append(
            NestedField(field_id=index, name=name, field_type=iceberg_type(), required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))
    return Schema(*iceberg_fields), pa.schema(arrow_fields)


def _cast_rows(
    rows: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    target: str,
) -> list[dict[str, Any]]:
    field_types = {str(field["name"]): str(field["type"]).lower() for field in fields}
    required_fields = {str(field["name"]) for field in fields if bool(field.get("required", False))}
    casted_rows: list[dict[str, Any]] = []
    for row in rows:
        casted_row: dict[str, Any] = {}
        for name, type_name in field_types.items():
            value = row.get(name)
            if value in {None, ""}:
                if name in required_fields:
                    raise ValueError(
                        f"Fixture table {target!r} is missing required value for {name!r}"
                    )
                casted_row[name] = None
                continue
            casted_row[name] = _cast_value(type_name, value)
        casted_rows.append(casted_row)
    return casted_rows


def _cast_value(type_name: str, value: Any) -> Any:
    if type_name == "string":
        return str(value)
    if type_name in {"long", "int"}:
        return int(value)
    if type_name == "double":
        return float(value)
    if type_name == "boolean":
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
        raise ValueError(f"Unsupported boolean value {value!r}")
    raise ValueError(f"Unsupported fixture type {type_name!r}")


def _write_catalog_config(catalog_name: str) -> None:
    _write_yaml(
        RUNTIME_DIR / "catalogs.yaml",
        {
            "catalogs": {
                catalog_name: {
                    "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{RUNTIME_DIR / f'{catalog_name}.db'}",
                        "warehouse": str(RUNTIME_DIR / "warehouse"),
                    },
                }
            }
        },
    )


def _write_policy_config(
    catalog_name: str,
    fixture: dict[str, Any],
    tables: list[dict[str, Any]],
) -> None:
    policies: dict[str, dict[str, Any]] = {}
    raw_policies = fixture.get("policies", [])
    if raw_policies:
        if not isinstance(raw_policies, list):
            raise ValueError("Fixture policies must be a list")
        for raw_policy in raw_policies:
            if not isinstance(raw_policy, dict):
                raise ValueError("Fixture policies must contain YAML objects")
            target = str(_require_key(raw_policy, "target")).strip()
            rules = raw_policy.get("rules")
            if not isinstance(rules, list) or not rules:
                raise ValueError(f"Fixture policy {target!r} must define a non-empty rules list")
            policies[target] = {"rules": rules}
    else:
        for table in tables:
            rules = []
            for rule in table.get("rules", []):
                if not isinstance(rule, dict):
                    raise ValueError(
                        f"Fixture table {table['target']!r} rules must be YAML objects"
                    )
                rules.append(rule)
            if rules:
                policies[str(table["target"])] = {"rules": rules}
    if not policies:
        raise ValueError("Fixture must define policies or per-table rules")
    _write_yaml(
        RUNTIME_DIR / "policies.yaml",
        {"version": 1, "catalogs": {catalog_name: {"targets": policies}}},
    )


def _write_app_config() -> None:
    app: dict[str, Any] = {
        "location": "grpc://0.0.0.0:8815",
        "catalog_file": str(RUNTIME_DIR / "catalogs.yaml"),
        "policy_file": str(RUNTIME_DIR / "policies.yaml"),
        "secret_provider": {
            "module": "dal_obscura.infrastructure.adapters.secret_providers.EnvSecretProvider",
            "args": {},
        },
        "ticket": {
            "ttl_seconds": 900,
            "max_tickets": 64,
            "secret": {"key": "DAL_OBSCURA_TICKET_SECRET"},
        },
        "logging": {"level": "INFO", "json": True},
    }
    auth_config = _load_yaml(AUTH_CONFIG_FILE, "Auth config")
    _merge_dict(app, auth_config)
    if TRANSPORT_CONFIG_FILE.exists():
        _merge_dict(app, _load_yaml(TRANSPORT_CONFIG_FILE, "Transport config"))
    _write_yaml(RUNTIME_DIR / "app.yaml", app)


def _merge_dict(base: dict[str, Any], override: dict[str, Any]) -> None:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _merge_dict(base[key], value)
        else:
            base[key] = value


def _load_yaml(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must contain a YAML object")
    return raw


def _write_yaml(path: Path, value: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(value, sort_keys=False))


def _require_key(raw: dict[str, Any], key: str) -> Any:
    if key not in raw:
        raise ValueError(f"Missing required key {key!r}")
    return raw[key]


if __name__ == "__main__":
    main()
