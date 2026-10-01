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


def _unchecked_discovery_page(entries: object, continuation: object) -> DiscoveryPage:
    page = object.__new__(DiscoveryPage)
    object.__setattr__(page, "entries", entries)
    object.__setattr__(page, "continuation", continuation)
    return page


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
                        "namespace": ["default"],
                        "name": "users",
                        "files": ["part.parquet"],
                        "schema_ipc": schema_ipc,
                        "field_ids": ["id"],
                    }
                },
            }
        )
    )
    return root, table


def test_catalog_registry_routes_public_manifest_plugin_through_governed_port(
    tmp_path: Path,
) -> None:
    root, table = _fixture(tmp_path)
    registry = PluginRegistry(
        builtins=cast(
            Any,
            {
                ("catalog", "manifest"): (CATALOG_DESCRIPTOR, manifest_factory),
                ("table_format", "parquet.dataset"): (FORMAT_DESCRIPTOR, parquet_factory),
            },
        )
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


def test_public_catalog_enforces_handle_metadata_destination(tmp_path: Path) -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")
    handle = TableHandle(
        catalog_plugin_id="manifest",
        catalog_instance_id="analytics",
        catalog_revision=1,
        identifier=identifier,
        format_plugin_id="parquet.dataset",
        handle_version=1,
        metadata={"metadata_location": "s3://untrusted/metadata.json"},
    )

    class Catalog:
        descriptor = CATALOG_DESCRIPTOR

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
        "manifest",
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
        descriptor = CATALOG_DESCRIPTOR

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
        descriptor = FORMAT_DESCRIPTOR

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
        descriptor = FORMAT_DESCRIPTOR

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
        descriptor = FORMAT_DESCRIPTOR

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
        descriptor = FORMAT_DESCRIPTOR

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
        descriptor = FORMAT_DESCRIPTOR

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
        descriptor = FORMAT_DESCRIPTOR

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
        descriptor = CATALOG_DESCRIPTOR

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
        descriptor = CATALOG_DESCRIPTOR

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
        descriptor = CATALOG_DESCRIPTOR

        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return ()

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            return _unchecked_discovery_page((identifier,), malformed_token)

        def resolve_table(self, value, context):
            del value, context
            raise AssertionError("resolve should not run")

        def close(self):
            return None

    adapter = PublicPluginCatalogAdapter(
        "fixture", {}, "manifest", lambda config, context: Catalog(), lambda plugin_id: object()
    )
    with pytest.raises(ValueError, match="continuation token"):
        adapter.list_tables()


def test_public_catalog_adapter_rejects_oversized_page() -> None:
    identifier = TableIdentifier(namespace=("default",), name="users")

    class Catalog:
        descriptor = CATALOG_DESCRIPTOR

        def validate_config(self, context):
            del context

        def list_namespaces(self, context, *, namespace=()):
            del context, namespace
            return ()

        def list_tables(self, context, *, continuation=None, limit):
            del context, continuation, limit
            entries = tuple(identifier for _ in range(501))
            return _unchecked_discovery_page(entries, None)

        def resolve_table(self, value, context):
            del value, context
            raise AssertionError("resolve should not run")

        def close(self):
            return None

    adapter = PublicPluginCatalogAdapter(
        "fixture", {}, "manifest", lambda config, context: Catalog(), lambda plugin_id: object()
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
                schema_version=1, fingerprint="0" * 64, arrow_schema=pa.schema([])
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
        descriptor = FORMAT_DESCRIPTOR

        def close(self):
            return None

        def schema(self, value, context):
            del value, context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(
                schema_version=1, fingerprint="0" * 64, arrow_schema=schema, stable_ids=True
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
        descriptor = FORMAT_DESCRIPTOR

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


def test_format_without_current_descriptor_is_rejected_and_closed():
    closed = []

    class MissingDescriptor:
        def schema(self, *args):
            pytest.fail("schema must not execute")

        def plan(self, *args):
            pytest.fail("plan must not execute")

        def execute(self, *args):
            pytest.fail("execute must not execute")

        def close(self):
            closed.append(True)

    handle = TableHandle(
        catalog_plugin_id="manifest",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="parquet.dataset",
        handle_version=1,
    )
    adapter = PublicPluginTableFormat(
        catalog_name="fixture",
        table_name="default.users",
        format="parquet.dataset",
        handle=handle,
        format_factory=cast(Any, lambda handle, context: MissingDescriptor()),
    )
    with pytest.raises(ValueError, match="mismatched descriptor"):
        adapter.get_schema()
    assert closed == [True]


def _contract_format(schema, *, tasks=("scan",)):
    from dal_obscura_plugin_api import SchemaDescriptor

    calls = []
    handle = TableHandle(
        catalog_plugin_id="manifest",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="parquet.dataset",
        handle_version=1,
    )

    class Format:
        descriptor = FORMAT_DESCRIPTOR

        def schema(self, handle, context):
            return SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=schema)

        def plan(self, handle, schema, context, **kwargs):
            calls.append(kwargs)
            return list(tasks)

        def execute(self, task, context):
            calls.append("execute")
            return schema, iter(())

        def close(self):
            calls.append("close")

    def factory(handle, context):
        calls.append("open")
        return Format()

    return PublicPluginTableFormat(
        catalog_name="fixture",
        table_name="default.users",
        format="parquet.dataset",
        format_factory=factory,
        handle=handle,
    ), calls


def test_optional_filter_pushdown_keeps_full_filter_for_core():
    from dal_obscura.common.access_control.filters import parse_row_filter

    schema = pa.schema([pa.field("id", pa.int64())])
    table, calls = _contract_format(schema)
    row_filter = parse_row_filter("id > 0", schema)
    plan = table.plan(PlanRequest(target="default.users", columns=["id"], row_filter=row_filter), 2)
    assert calls[1]["row_filter"] is None
    assert plan.full_row_filter == row_filter
    assert plan.residual_row_filter == row_filter


def test_backend_projection_uses_literal_top_level_names():
    schema = pa.schema(
        [
            pa.field("profile.email", pa.string()),
            pa.field("profile", pa.struct([pa.field("email", pa.string())])),
        ]
    )
    table, calls = _contract_format(schema)
    plan = table.plan(
        PlanRequest(target="default.users", columns=['["profile.email"]', "profile.email"]), 2
    )
    assert calls[1]["projection"] == ["profile.email", "profile"]
    assert plan.tasks[0].partition.schema == schema


def test_empty_plan_does_not_invent_a_backend_task():
    schema = pa.schema([pa.field("id", pa.int64())])
    table, calls = _contract_format(schema, tasks=())
    plan = table.plan(PlanRequest(target="default.users", columns=["id"]), 2)
    assert plan.tasks == []
    assert "execute" not in calls


def test_unstarted_execution_does_not_open_plugin_resources():
    schema = pa.schema([pa.field("id", pa.int64())])
    table, calls = _contract_format(schema)
    plan = table.plan(PlanRequest(target="default.users", columns=["id"]), 2)
    calls.clear()
    output_schema, batches = table.execute(plan.tasks[0].partition)
    assert output_schema == schema
    assert calls == []
    assert list(batches) == []
    assert calls == ["open", "execute", "close"]


def test_catalog_identifiers_round_trip_without_dotted_name_collisions():
    from dal_obscura.data_plane.infrastructure.adapters.public_plugin_adapter import (
        _identifier_name,
        _table_identifier,
    )

    identifiers = [
        TableIdentifier(namespace=("a.b",), name="c"),
        TableIdentifier(namespace=("a",), name="b.c"),
    ]
    names = [_identifier_name(identifier) for identifier in identifiers]
    assert len(set(names)) == 2
    assert [_table_identifier(name) for name in names] == identifiers


def test_expired_batch_context_does_not_read_source():
    from dal_obscura.data_plane.infrastructure.adapters.public_plugin_adapter import (
        _checked_plugin_batches,
    )

    read = []

    def source():
        read.append(True)
        yield pa.record_batch([pa.array([1])], names=["id"])

    context = ExecutionContext(
        deadline=datetime.now(timezone.utc) - timedelta(seconds=1), correlation_id="expired"
    )
    with pytest.raises(ValueError, match="deadline"):
        list(_checked_plugin_batches(source(), pa.schema([pa.field("id", pa.int64())]), context))
    assert read == []


def test_manifest_read_enforces_policy_filter_before_masking(tmp_path):
    import hashlib

    from dal_obscura.common.access_control.models import AccessDecision, MaskRule
    from tests.application.access_flow.helpers import (
        AUTHORIZATION_HEADER,
        _build_end_to_end_access_flow,
    )

    root, _ = _fixture(tmp_path)
    catalog = PublicPluginCatalogAdapter(
        "fixture",
        {"root": str(root), "manifest_path": str(root / "manifest.json")},
        "manifest",
        manifest_factory,
        lambda _: parquet_factory,
    )
    try:
        table = catalog.resolve_table("default.users")
        planner, fetch = _build_end_to_end_access_flow(
            table,
            AccessDecision(
                allowed_columns=["id"],
                masks={"id": MaskRule(type="hash")},
                row_filter="id > 1",
                policy_version=1,
            ),
        )
        plan = planner.execute(
            PlanRequest(catalog="fixture", target="default.users", columns=["id"]),
            AUTHORIZATION_HEADER,
        )
        assert len(plan.ticket_tokens) == 1
        result = fetch.execute(plan.ticket_tokens[0], AUTHORIZATION_HEADER)
        assert pa.Table.from_batches(result.result_batches).to_pylist() == [
            {"id": hashlib.sha256(b"2").hexdigest()}
        ]
    finally:
        catalog.close()


def test_schema_must_match_catalog_pinned_snapshot():
    from dataclasses import replace

    schema = pa.schema([pa.field("id", pa.int64())])
    table, _ = _contract_format(schema)
    table = replace(table, handle=replace(table.handle, snapshot_id="pinned"))
    with pytest.raises(ValueError, match="snapshot"):
        table.get_schema()
    with pytest.raises(ValueError, match="snapshot"):
        table.plan(PlanRequest(target="default.users", columns=["id"]), 1)
