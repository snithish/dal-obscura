from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, cast

import pyarrow as pa
import pytest
from dal_obscura_manifest_parquet.format import FORMAT_DESCRIPTOR
from dal_obscura_plugin_api import ExecutionContext, ScanTask, TableHandle, TableIdentifier

from dal_obscura.read.request import PlanRequest
from dal_obscura.sources.plugin_runtime import (
    PublicPluginTableFormat,
)
from tests.support.public_plugins import _contract_format


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

        def schema(self, context):
            del context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(arrow_schema=schema)

        def plan(self, request, context):
            del request, context
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
    with pytest.raises(ValueError, match="immutable ScanTask"):
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

        def schema(self, context):
            del context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(arrow_schema=pa.schema([pa.field("id", pa.int64())]))

        def plan(self, request, context):
            del request, context
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

        def schema(self, context):
            del context
            return object()

        def plan(self, request, context):
            del request, context
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

        def schema(self, context):
            del context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(arrow_schema=schema)

        def plan(self, request, context):
            del request, context
            return [ScanTask({"id": "task"})]

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

        def schema(self, context):
            del context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(arrow_schema=schema)

        def plan(self, request, context):
            del request, context
            return [ScanTask({"id": "task"})]

        def execute(self, task, context):
            del task, context

            def batches():
                cancelled[0] = True
                yield pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)

            return schema, batches()

    from dal_obscura.sources import plugin_runtime as public_plugin_adapter

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
    with pytest.raises(InterruptedError, match="cancelled"):
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

        def schema(self, context):
            del context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(arrow_schema=schema)

        def plan(self, request, context):
            del request, context
            return [ScanTask({"id": "task"})]

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

        def schema(self, context):
            del context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(arrow_schema=pa.schema([]))

        def plan(self, request, context):
            del request, context
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

        def schema(self, context):
            del context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(arrow_schema=schema, stable_ids=True)

        def plan(self, request, context):
            del request, context
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

        def schema(self, context):
            del context
            from dal_obscura_plugin_api import SchemaDescriptor

            return SchemaDescriptor(arrow_schema=schema)

        def plan(self, request, context):
            del request, context
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


def test_optional_filter_pushdown_keeps_full_filter_for_core():
    from dal_obscura.policy.filters import parse_row_filter

    schema = pa.schema([pa.field("id", pa.int64())])
    table, calls = _contract_format(schema)
    row_filter = parse_row_filter("id > 0", schema)
    plan = table.plan(PlanRequest(target="default.users", columns=["id"], row_filter=row_filter), 2)
    assert calls[1].row_filter is None
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
    assert calls[1].columns == ("profile.email", "profile")
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


def test_expired_batch_context_does_not_read_source():
    from dal_obscura.sources.plugin_runtime import (
        _checked_plugin_batches,
    )

    read = []

    def source():
        read.append(True)
        yield pa.record_batch([pa.array([1])], names=["id"])

    context = ExecutionContext(
        deadline=datetime.now(timezone.utc) - timedelta(seconds=1), correlation_id="expired"
    )
    with pytest.raises(TimeoutError, match="deadline"):
        list(_checked_plugin_batches(source(), pa.schema([pa.field("id", pa.int64())]), context))
    assert read == []


def test_bound_source_acquires_plugin_and_schema_once():
    from dataclasses import replace

    schema = pa.schema([("id", pa.int64())])
    base, calls = _contract_format(schema)

    def factory(handle, context):
        plugin = base.format_factory(handle, context)
        original = plugin.schema

        def read_schema(context):
            calls.append("schema")
            return original(context)

        plugin.schema = read_schema
        return plugin

    table = replace(base, format_factory=factory)
    with table.open() as bound:
        assert bound.get_schema() == schema
        bound.plan(PlanRequest(target="default.users", columns=["id"]), 1)
    assert calls.count("open") == calls.count("schema") == calls.count("close") == 1


@pytest.mark.parametrize("invalid_task", [False, True], ids=["budget", "untyped-task"])
def test_runtime_closes_lazy_plans_before_closing_the_plugin(invalid_task):
    schema = pa.schema([("id", pa.int64())])
    base, _ = _contract_format(schema)
    closed = []

    def factory(handle, context):
        plugin = base.format_factory(handle, context)

        def plan(request, context):
            try:
                yield object() if invalid_task else ScanTask({"file": "first"})
                yield ScanTask({"file": "second"})
            finally:
                closed.append("plan")

        plugin.plan = plan
        plugin.close = lambda: closed.append("plugin")
        return plugin

    from dataclasses import replace

    table = replace(base, format_factory=factory)
    with pytest.raises(ValueError, match=r"immutable ScanTask|more tasks"):
        table.plan(PlanRequest(target="default.users", columns=["id"]), 1)
    assert closed == ["plan", "plugin"]


def test_runtime_closes_plan_when_iterator_construction_and_cleanup_fail():
    from dataclasses import replace

    base, _ = _contract_format(pa.schema([("id", pa.int64())]))
    closed = []

    class BrokenPlan:
        def __iter__(self):
            raise ValueError("cannot start planning")

        def close(self):
            closed.append("plan")
            raise RuntimeError("plan close failed")

    def factory(handle, context):
        plugin = base.format_factory(handle, context)
        plugin.plan = lambda request, context: BrokenPlan()
        plugin.close = lambda: closed.append("plugin")
        return plugin

    table = replace(base, format_factory=factory)
    with pytest.raises(ValueError, match="cannot start planning"):
        table.plan(PlanRequest(target="default.users", columns=["id"]), 1)
    assert closed == ["plan", "plugin"]
