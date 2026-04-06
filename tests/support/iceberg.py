from __future__ import annotations

from contextlib import suppress
from pathlib import Path

import pyarrow as pa
import yaml
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


def create_iceberg_table(
    tmp_path: Path,
    catalog_name: str,
    warehouse_name: str,
    values: list[int] | None = None,
    *,
    identifier: str = "default.users",
    append_batches: list[list[int]] | None = None,
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
    namespace = ".".join(identifier.split(".")[:-1])
    with suppress(Exception):
        catalog.create_namespace(namespace)
    table = catalog.create_table(
        identifier=identifier,
        schema=schema,
        properties={"format-version": "2"},
    )
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("email", pa.string(), nullable=True),
            pa.field("region", pa.string(), nullable=True),
        ]
    )
    batches = append_batches or [values or []]
    for batch_values in batches:
        table.append(
            pa.table(
                {
                    "id": batch_values,
                    "email": [f"user{i}@example.com" for i in batch_values],
                    "region": ["us" if i % 2 == 0 else "eu" for i in batch_values],
                },
                schema=arrow_schema,
            )
        )
    return identifier


def write_yaml_files(
    tmp_path: Path,
    *,
    service_config: dict[str, object],
    policy: dict[str, object],
) -> tuple[Path, Path]:
    service_config_path = tmp_path / "service.yaml"
    policy_path = tmp_path / "policy.yaml"
    service_config_path.write_text(yaml.safe_dump(service_config, sort_keys=False))
    policy_path.write_text(yaml.safe_dump(policy, sort_keys=False))
    return service_config_path, policy_path
