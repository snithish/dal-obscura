from __future__ import annotations

import base64
import json
import pickle
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dal_obscura_manifest_parquet.catalog import CATALOG_DESCRIPTOR, manifest_factory
from dal_obscura_manifest_parquet.format import FORMAT_DESCRIPTOR, parquet_factory
from dal_obscura_plugin_api import DiscoveryPage, ExecutionContext, TableHandle, TableIdentifier

from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.adapters.public_plugin_adapter import (
    PublicPluginCatalogAdapter,
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


def test_public_iceberg_compatibility_path_enforces_metadata_destination(tmp_path: Path) -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    handle = TableHandle(
        catalog_plugin_id="rest.catalog",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="iceberg",
        handle_version=1,
        metadata={"metadata_location": "s3://untrusted/metadata.json"},
    )

    class Catalog:
        def list_tables(self, context: ExecutionContext, *, continuation: str | None, limit: int):
            return DiscoveryPage((identifier,))

        def resolve_table(self, value: TableIdentifier, context: ExecutionContext):
            assert value == identifier
            return handle

    adapter = PublicPluginCatalogAdapter(
        "analytics",
        {},
        "rest.catalog",
        lambda config, context: Catalog(),
        lambda plugin_id: object(),
        PathRuleEnforcer([{"root": str(tmp_path)}]),
    )
    with pytest.raises(PermissionError, match="not allowed"):
        adapter.resolve_table("default.users")


def test_public_format_rejects_opaque_task_payloads_before_ticket_serialization() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    handle = TableHandle(
        catalog_plugin_id="manifest",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="parquet.dataset",
        handle_version=1,
    )
    schema = pa.schema([pa.field("id", pa.int64())])

    class OpaqueFormat:
        def schema(self, value, context):
            del value, context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=schema)

        def plan(self, value, descriptor, context, *, projection, row_filter, max_tasks):
            del value, descriptor, context, projection, row_filter, max_tasks
            return [object()]

        def execute(self, task, context):
            del task, context
            return schema, []

    table_format = PublicPluginTableFormat(
        catalog_name="fixture",
        table_name="default.users",
        format="parquet.dataset",
        format_factory=lambda value, context: OpaqueFormat(),
        handle=handle,
    )
    with pytest.raises(ValueError, match="inert JSON-like"):
        table_format.plan(PlanRequest(target="default.users", columns=["*"]), max_tickets=2)


def test_public_format_validates_lazy_batch_schema_before_streaming() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    handle = TableHandle(
        catalog_plugin_id="manifest",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="parquet.dataset",
        handle_version=1,
    )
    schema = pa.schema([pa.field("id", pa.int64())])

    class BadBatchFormat:
        def schema(self, value, context):
            del value, context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=schema)

        def plan(self, value, descriptor, context, *, projection, row_filter, max_tasks):
            del value, descriptor, context, projection, row_filter, max_tasks
            return ["task"]

        def execute(self, task, context):
            del task, context
            wrong_schema = pa.schema([pa.field("secret", pa.string())])
            return schema, [pa.RecordBatch.from_pylist([{"secret": "hidden"}], schema=wrong_schema)]

    table_format = PublicPluginTableFormat(
        catalog_name="fixture",
        table_name="default.users",
        format="parquet.dataset",
        format_factory=lambda value, context: BadBatchFormat(),
        handle=handle,
    )
    plan = table_format.plan(PlanRequest(target="default.users", columns=["*"]), max_tickets=2)
    _schema, batches = plan.tasks[0].table_format.execute(plan.tasks[0].partition)
    with pytest.raises(ValueError, match="batch schema"):
        list(batches)


def test_public_format_rejects_factory_descriptor_mismatch() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    handle = TableHandle(
        catalog_plugin_id="manifest",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="parquet.dataset",
        handle_version=1,
    )

    class WrongDescriptorFormat:
        descriptor = type("Descriptor", (), {"kind": "catalog", "plugin_id": "other"})()

        def schema(self, value, context):
            del value, context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(
                schema_version=1,
                fingerprint="0" * 64,
                arrow_schema=pa.schema([]),
            )

        def plan(self, value, descriptor, context, *, projection, row_filter, max_tasks):
            del value, descriptor, context, projection, row_filter, max_tasks
            return []

        def execute(self, task, context):
            del task, context
            return pa.schema([]), []

    table_format = PublicPluginTableFormat(
        catalog_name="fixture",
        table_name="default.users",
        format="parquet.dataset",
        format_factory=lambda value, context: WrongDescriptorFormat(),
        handle=handle,
    )
    with pytest.raises(ValueError, match="mismatched descriptor"):
        table_format.get_schema()
