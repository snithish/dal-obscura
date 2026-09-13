from __future__ import annotations

import base64
import json
import pickle
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
from dal_obscura_manifest_parquet.catalog import CATALOG_DESCRIPTOR, manifest_factory
from dal_obscura_manifest_parquet.format import FORMAT_DESCRIPTOR, parquet_factory

from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from dal_obscura.data_plane.infrastructure.adapters.public_plugin_adapter import (
    PublicPluginTableFormat,
)


def _fixture(tmp_path: Path) -> tuple[Path, pa.Table]:
    root = tmp_path / "dataset"
    root.mkdir()
    table = pa.table({"id": pa.array([1, 2], type=pa.int64())})
    pq.write_table(table, root / "part.parquet", row_group_size=1)
    schema_ipc = base64.b64encode(table.schema.serialize().to_pybytes()).decode("ascii")
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "revision": "r1",
                "tables": {
                    "default.users": {
                        "files": ["part.parquet"],
                        "schema_ipc": schema_ipc,
                        "field_ids": ["id"],
                    }
                },
            }
        )
    )
    return root, table


def test_catalog_registry_routes_public_manifest_plugin_through_legacy_port(tmp_path: Path) -> None:
    root, table = _fixture(tmp_path)
    registry = PluginRegistry(
        builtins=cast(Any, {
            ("catalog", "manifest"): (CATALOG_DESCRIPTOR, manifest_factory),
            ("table_format", "parquet.dataset"): (FORMAT_DESCRIPTOR, parquet_factory),
        }),
    )
    registry.reload()
    catalogs = CatalogRegistry(
        ServiceConfig(
            catalogs={
                "datasets": CatalogConfig(
                    name="datasets",
                    type=cast(Any, "iceberg"),
                    plugin_id="manifest",
                    options={"root": str(root), "manifest_path": "manifest.json"},
                )
            }
        ),
        plugin_registry=registry,
    )

    table_format = catalogs.resolve("datasets", "default.users")

    assert isinstance(table_format, PublicPluginTableFormat)
    assert table_format.get_schema() == table.schema
    plan = table_format.plan(PlanRequest(target="default.users", columns=["*"]), max_tickets=4)
    assert len(plan.tasks) == 2
    serialized = pickle.dumps(plan.tasks[0])
    restored = pickle.loads(serialized)
    output_schema, batches = restored.table_format.execute(restored.partition)
    assert output_schema == table.schema
    assert pa.Table.from_batches(list(batches)).to_pylist() == [{"id": 1}]

    projected = table_format.plan(
        PlanRequest(target="default.users", columns=["id"]), max_tickets=4
    )
    projected_schema, projected_batches = projected.tasks[0].table_format.execute(
        projected.tasks[0].partition
    )
    assert projected_schema.names == ["id"]
    assert pa.Table.from_batches(list(projected_batches)).to_pylist() == [{"id": 1}]
