from __future__ import annotations

import base64
import json
import pickle
from datetime import datetime, timedelta, timezone
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
    PublicPluginPartition,
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
        builtins=cast(
            Any,
            {
                ("catalog", "manifest"): (CATALOG_DESCRIPTOR, manifest_factory),
                ("table_format", "parquet.dataset"): (FORMAT_DESCRIPTOR, parquet_factory),
            },
        ),
    )
    registry.reload()
    catalogs = CatalogRegistry(
        ServiceConfig(
            catalogs={
                "datasets": CatalogConfig(
                    name="datasets",
                    type=cast(Any, "iceberg"),
                    plugin_id="manifest",
                    revision=17,
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
    partition = cast(PublicPluginPartition, plan.tasks[0].partition)
    assert partition.handle.catalog_revision == 17
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
        catalog_instance_id="analytics",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="iceberg",
        handle_version=1,
        metadata={"metadata_location": "s3://untrusted/metadata.json"},
    )

    class Catalog:
        def close(self):
            return None

        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return (("default",),)

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
        revision=1,
    )
    with pytest.raises(PermissionError, match="not allowed"):
        adapter.resolve_table("default.users")


def test_public_catalog_adapter_rejects_forged_handle_identity() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    forged = TableHandle(
        catalog_plugin_id="other.catalog",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="parquet.dataset",
        handle_version=1,
    )

    class Catalog:
        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return ()

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            return DiscoveryPage(())

        def resolve_table(self, value, context):
            del value, context
            return forged

        def close(self):
            return None

    adapter = PublicPluginCatalogAdapter(
        "fixture",
        {},
        "manifest",
        lambda config, context: Catalog(),
        lambda plugin_id: object(),
        revision=1,
    )
    with pytest.raises(ValueError, match="mismatched table handle identity"):
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
        def close(self):
            return None

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


def test_public_format_requires_explicit_close_lifecycle() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    handle = TableHandle(
        catalog_plugin_id="manifest",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="parquet.dataset",
        handle_version=1,
    )

    class MissingCloseFormat:
        def schema(self, value, context):
            del value, context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(
                schema_version=1,
                fingerprint="0" * 64,
                arrow_schema=pa.schema([pa.field("id", pa.int64())]),
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
        format_factory=lambda value, context: MissingCloseFormat(),
        handle=handle,
    )
    with pytest.raises(ValueError, match="invalid plugin"):
        table_format.get_schema()


def test_public_format_preserves_schema_error_when_close_fails() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    handle = TableHandle(
        catalog_plugin_id="manifest",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="parquet.dataset",
        handle_version=1,
    )

    class FailingCloseFormat:
        def close(self):
            raise RuntimeError("close failed")

        def schema(self, value, context):
            del value, context
            return object()

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
        format_factory=lambda value, context: FailingCloseFormat(),
        handle=handle,
    )
    with pytest.raises(ValueError, match="invalid schema descriptor"):
        table_format.get_schema()


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
        def __init__(self):
            self.executed = False

        def close(self):
            if self.executed:
                raise RuntimeError("close failed")

        def schema(self, value, context):
            del value, context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=schema)

        def plan(self, value, descriptor, context, *, projection, row_filter, max_tasks):
            del value, descriptor, context, projection, row_filter, max_tasks
            return ["task"]

        def execute(self, task, context):
            del task, context
            self.executed = True
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


def test_public_format_stops_lazy_batches_when_context_is_cancelled(monkeypatch) -> None:
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
    cancelled = [False]

    class SlowFormat:
        def close(self):
            return None

        def schema(self, value, context):
            del value, context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=schema)

        def plan(self, value, descriptor, context, *, projection, row_filter, max_tasks):
            del value, descriptor, context, projection, row_filter, max_tasks
            return ["task"]

        def execute(self, task, context):
            del task, context

            def batches():
                cancelled[0] = True
                yield pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)

            return schema, batches()

    from dal_obscura.data_plane.infrastructure.adapters import public_plugin_adapter

    monkeypatch.setattr(
        public_plugin_adapter,
        "_context",
        lambda: ExecutionContext(
            deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
            correlation_id="cancelled-plugin",
            cancel_check=lambda: cancelled[0],
        ),
    )
    table_format = PublicPluginTableFormat(
        catalog_name="fixture",
        table_name="default.users",
        format="parquet.dataset",
        format_factory=lambda value, context: SlowFormat(),
        handle=handle,
    )
    plan = table_format.plan(PlanRequest(target="default.users", columns=["*"]), max_tickets=2)
    _schema, batches = plan.tasks[0].table_format.execute(plan.tasks[0].partition)
    with pytest.raises(ValueError, match="cancelled"):
        list(batches)


def test_public_format_closes_plugin_after_lazy_output_is_consumed() -> None:
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
    closed = []

    class ClosableFormat:
        def schema(self, value, context):
            del value, context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=schema)

        def plan(self, value, descriptor, context, *, projection, row_filter, max_tasks):
            del value, descriptor, context, projection, row_filter, max_tasks
            return ["task"]

        def execute(self, task, context):
            del task, context
            return schema, [pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)]

        def close(self):
            closed.append(True)

    table_format = PublicPluginTableFormat(
        catalog_name="fixture",
        table_name="default.users",
        format="parquet.dataset",
        format_factory=lambda value, context: ClosableFormat(),
        handle=handle,
    )
    plan = table_format.plan(PlanRequest(target="default.users", columns=["*"]), max_tickets=2)
    _schema, batches = plan.tasks[0].table_format.execute(plan.tasks[0].partition)

    observed = [batch.to_pylist() for batch in batches]
    assert observed == [[{"id": 1}]]
    assert closed == [True, True]


def test_public_catalog_adapter_closes_catalog_and_rejects_reuse() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    closed = []

    class ClosableCatalog:
        descriptor = type("Descriptor", (), {"kind": "catalog", "plugin_id": "manifest"})()

        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return (("default",),)

        def list_tables(self, context, *, continuation, limit):
            del context, continuation, limit
            return DiscoveryPage((identifier,))

        def resolve_table(self, value, context):
            del value, context
            raise AssertionError("resolve should not run after close")

        def close(self):
            closed.append(True)

    adapter = PublicPluginCatalogAdapter(
        "fixture",
        {},
        "manifest",
        lambda config, context: ClosableCatalog(),
        lambda plugin_id: object(),
    )
    adapter.close()
    adapter.close()

    assert closed == [True]
    with pytest.raises(ValueError, match="closed"):
        adapter.list_tables()


def test_public_catalog_adapter_rejects_missing_lifecycle_methods() -> None:
    class IncompleteCatalog:
        def list_tables(self, context, *, continuation, limit):
            del context, continuation, limit
            return DiscoveryPage(())

        def resolve_table(self, value, context):
            del value, context
            raise AssertionError("resolve should not run")

        def close(self):
            return None

    with pytest.raises(ValueError, match="invalid plugin"):
        PublicPluginCatalogAdapter(
            "fixture",
            {},
            "manifest",
            lambda config, context: IncompleteCatalog(),
            lambda plugin_id: object(),
        )


@pytest.mark.parametrize("malformed_token", [["unhashable"], "bad\n token", "x" * 4_097])
def test_public_catalog_adapter_rejects_malformed_continuation(malformed_token) -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")

    class Catalog:
        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return ()

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            return type("Page", (), {"entries": (identifier,), "continuation": malformed_token})()

        def resolve_table(self, value, context):
            del value, context
            raise AssertionError("resolve should not run")

        def close(self):
            return None

    adapter = PublicPluginCatalogAdapter(
        "fixture",
        {},
        "manifest",
        lambda config, context: Catalog(),
        lambda plugin_id: object(),
    )
    with pytest.raises(ValueError, match="continuation token"):
        adapter.list_tables()


def test_public_catalog_adapter_rejects_oversized_page() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")

    class Catalog:
        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return ()

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            entries = tuple(identifier for _ in range(501))
            return type("Page", (), {"entries": entries, "continuation": None})()

        def resolve_table(self, value, context):
            del value, context
            raise AssertionError("resolve should not run")

        def close(self):
            return None

    adapter = PublicPluginCatalogAdapter(
        "fixture",
        {},
        "manifest",
        lambda config, context: Catalog(),
        lambda plugin_id: object(),
    )
    with pytest.raises(ValueError, match="too many page entries"):
        adapter.list_tables()


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

        def close(self):
            return None

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


def test_public_format_rejects_false_stable_id_claim() -> None:
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

    class LyingFormat:
        def close(self):
            return None

        def schema(self, value, context):
            del value, context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(
                schema_version=1,
                fingerprint="0" * 64,
                arrow_schema=schema,
                stable_ids=True,
            )

        def plan(self, value, descriptor, context, *, projection, row_filter, max_tasks):
            del value, descriptor, context, projection, row_filter, max_tasks
            return []

        def execute(self, task, context):
            del task, context
            return schema, []

    table_format = PublicPluginTableFormat(
        catalog_name="fixture",
        table_name="default.users",
        format="parquet.dataset",
        format_factory=lambda value, context: LyingFormat(),
        handle=handle,
    )
    with pytest.raises(ValueError, match="stable IDs"):
        table_format.get_schema()


def test_public_format_rejects_schema_depth_before_plugin_execution() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    handle = TableHandle(
        catalog_plugin_id="manifest",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="parquet.dataset",
        handle_version=1,
    )
    nested: pa.DataType = pa.string()
    for index in range(66):
        nested = pa.struct([pa.field(f"level_{index}", nested)])
    schema = pa.schema([pa.field("root", nested)])

    class DeepFormat:
        def close(self):
            return None

        def schema(self, value, context):
            del value, context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=schema)

        def plan(self, value, descriptor, context, *, projection, row_filter, max_tasks):
            del value, descriptor, context, projection, row_filter, max_tasks
            return []

        def execute(self, task, context):
            del task, context
            return schema, []

    table_format = PublicPluginTableFormat(
        catalog_name="fixture",
        table_name="default.users",
        format="parquet.dataset",
        format_factory=lambda value, context: DeepFormat(),
        handle=handle,
    )
    with pytest.raises(ValueError, match="nesting-depth"):
        table_format.get_schema()
