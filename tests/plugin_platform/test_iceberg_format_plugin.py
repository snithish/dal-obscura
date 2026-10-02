from typing import Any, cast

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import TableHandle, TableIdentifier

from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import Plan, ScanTask
from dal_obscura.data_plane.infrastructure.adapters import iceberg_format_plugin as module
from dal_obscura.data_plane.infrastructure.adapters.builtin_plugins import (
    create_builtin_plugin_registry,
)
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.adapters.public_plugin_adapter import (
    PublicPluginTableFormat,
)
from dal_obscura.data_plane.infrastructure.table_formats.iceberg import IcebergInputPartition


def handle(location="https://catalog.example/metadata.json"):
    return TableHandle(
        catalog_plugin_id="iceberg.rest",
        catalog_instance_id="analytics",
        catalog_revision=2,
        identifier=TableIdentifier(namespace=("default",), name="events"),
        format_plugin_id="iceberg",
        handle_version=1,
        metadata={"metadata_location": location},
    )


def test_registered_iceberg_uses_public_sdk_schema_plan_and_stream(monkeypatch):
    schema = pa.schema([pa.field("id", pa.int64())])
    received = []

    class Engine:
        def __init__(self, **kwargs):
            received.append(kwargs)
            self.table_name = kwargs["table_name"]

        def get_schema(self):
            return schema

        def plan(self, request, max_tasks):
            assert max_tasks == 2
            return Plan(
                schema=schema,
                tasks=[
                    ScanTask(
                        table_format=cast(Any, self),
                        schema=schema,
                        partition=IcebergInputPartition(columns=["id"], tasks=[value]),
                    )
                    for value in (b"one", b"two")
                ],
            )

        def execute(self, partition):
            value = 1 if partition.tasks == [b"one"] else 2
            return schema, iter([pa.record_batch([pa.array([value])], schema=schema)])

    monkeypatch.setattr(module, "IcebergTableFormat", Engine)
    registry = create_builtin_plugin_registry()
    factory = registry.load("table_format", "iceberg")
    adapter = PublicPluginTableFormat(
        catalog_name="analytics",
        table_name="default.events",
        format="iceberg",
        handle=handle(),
        format_factory=cast(Any, factory),
    )
    assert adapter.get_schema() == schema
    plan = adapter.plan(PlanRequest(target="default.events", columns=["id"]), max_tickets=2)
    assert len(plan.tasks) == 2
    batches = [batch for task in plan.tasks for batch in adapter.execute(task.partition)[1]]
    assert pa.Table.from_batches(batches).column("id").to_pylist() == [1, 2]
    assert all(
        item["metadata_location"] == "https://catalog.example/metadata.json" for item in received
    )


def test_iceberg_sdk_checks_storage_allowlist_before_io():
    plugin = module.IcebergFormatPlugin(
        handle("https://blocked.example/metadata.json"),
        cast(Any, None),
        path_enforcer=PathRuleEnforcer([{"root": "https://allowed.example/"}]),
    )
    with pytest.raises(PermissionError):
        plugin.schema(handle("https://blocked.example/metadata.json"), cast(Any, None))


@pytest.mark.parametrize(
    "columns", [["id"], ["id", "email", "region"]], ids=["integers", "strings"]
)
def test_sdk_iceberg_reads_real_parallel_scan_tasks_after_ticket_round_trip(tmp_path, columns):
    import pickle

    from pyiceberg.catalog import load_catalog

    from tests.support.iceberg import create_iceberg_table, iceberg_sql_catalog_options

    identifier = create_iceberg_table(tmp_path, "sdk", "warehouse", append_batches=[[1, 2], [3, 4]])
    catalog = load_catalog(
        "sdk", **cast(dict[str, str], iceberg_sql_catalog_options(tmp_path, "sdk", "warehouse"))
    )
    location = catalog.load_table(identifier).metadata_location
    resolved = handle(location)
    adapter = PublicPluginTableFormat(
        catalog_name="analytics",
        table_name="default.events",
        format="iceberg",
        handle=resolved,
        format_factory=module.IcebergFormatPlugin,
    )
    plan = adapter.plan(PlanRequest(target="default.events", columns=columns), max_tickets=2)
    assert len(plan.tasks) == 2
    batches = []
    for task in plan.tasks:
        restored = pickle.loads(pickle.dumps(task))
        schema, stream = restored.table_format.execute(restored.partition)
        assert schema.names == columns
        batches.extend(stream)
    assert sorted(pa.Table.from_batches(batches).column("id").to_pylist()) == [1, 2, 3, 4]
    assert all(batch.schema.equals(schema, check_metadata=True) for batch in batches)


@pytest.fixture
def native_plugin(monkeypatch):
    def create(schema, batches):
        class Engine:
            def __init__(self, **kwargs):
                pass

            def execute(self, partition):
                return schema, batches

        monkeypatch.setattr(module, "IcebergTableFormat", Engine)
        return module.IcebergFormatPlugin(handle(), cast(Any, None))

    return create


def test_native_iceberg_keeps_matching_arrow_buffers_and_closes_abandoned_reader(native_plugin):
    batch = pa.record_batch([pa.array([1])], names=["id"])
    closed = []

    def source():
        try:
            yield batch
            raise AssertionError("Early stop must not read the next batch")
        finally:
            closed.append(True)

    plugin = native_plugin(batch.schema, source())
    _, stream = plugin.execute(
        {"columns": ["id"], "tasks": [], "row_filter": None}, cast(Any, None)
    )
    iterator = iter(stream)

    assert next(iterator) is batch
    iterator.close()
    assert closed == [True]


def test_native_iceberg_closes_reader_when_schema_normalization_fails(native_plugin):
    closed = []

    def source():
        try:
            yield pa.record_batch([pa.array([1])], names=["wrong-column"])
        finally:
            closed.append(True)

    plugin = native_plugin(pa.schema([pa.field("id", pa.int64())]), source())
    _, stream = plugin.execute(
        {"columns": ["id"], "tasks": [], "row_filter": None}, cast(Any, None)
    )

    with pytest.raises(ValueError, match="names"):
        next(iter(stream))
    assert closed == [True]
