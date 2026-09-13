from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import cast

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import (
    ExecutionContext,
    PluginDescriptor,
    SchemaDescriptor,
    TableFormatPlugin,
    TableHandle,
    TableIdentifier,
)
from dal_obscura_plugin_conformance import (
    check_capabilities,
    check_record_batches,
    nested_golden_table,
    run_format_checks,
)


def _descriptor() -> PluginDescriptor:
    return PluginDescriptor(
        kind="table_format",
        plugin_id="fixture",
        api_version="1",
        config_version=1,
        distribution="fixture-package",
        version="1.0.0",
        capabilities=frozenset({"nested_schema", "splittable_scan"}),
    )


def _context() -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        correlation_id="fixture",
    )


def test_nested_golden_fixture_is_deterministic_and_typed():
    first = nested_golden_table()
    second = nested_golden_table()

    assert first.schema == second.schema
    assert first.to_pylist() == second.to_pylist()
    assert pa.types.is_struct(first.schema.field("profile").type)
    assert pa.types.is_list(first.schema.field("tags").type)
    assert pa.types.is_map(first.schema.field("labels").type)


def test_capability_negative_case_fails_closed():
    with pytest.raises(ValueError, match="missing required capabilities"):
        check_capabilities({"nested_schema"}, {"snapshot_reads"})


def test_record_batches_reject_schema_mutation():
    schema = pa.schema([pa.field("id", pa.int64())])
    bad = pa.RecordBatch.from_arrays([pa.array([1])], names=["unexpected"])

    with pytest.raises(ValueError, match="schema differs"):
        check_record_batches(schema, [bad])


def test_record_batch_validation_stops_unbounded_output_generators():
    schema = pa.schema([pa.field("id", pa.int64())])
    batch = pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)

    def endless_batches():
        while True:
            yield batch

    with pytest.raises(ValueError, match="more than 2 output batches"):
        check_record_batches(schema, endless_batches(), max_batches=2)


class _ConformingFormat:
    descriptor = _descriptor()

    def plan(self, handle, schema, context, *, projection, row_filter, max_tasks):
        del handle, context, projection, row_filter, max_tasks
        return [schema]

    def execute(self, task, context):
        del context
        batch = pa.RecordBatch.from_pylist([{"id": 1}], schema=task.arrow_schema)
        return task.arrow_schema, [batch]


def test_runner_returns_machine_readable_passing_result():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(
        schema_version=1,
        fingerprint="0" * 64,
        arrow_schema=table.schema,
    )
    handle = TableHandle(
        catalog_plugin_id="fixture",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="fixture",
        handle_version=1,
    )

    result = run_format_checks(
        cast(TableFormatPlugin, _ConformingFormat()),
        handle,
        schema,
        _context(),
        required_capabilities={"nested_schema"},
        artifact_identity="sha256:fixture",
    )

    payload = result.to_dict()
    assert payload["status"] == "passed"
    assert payload["core_version"] == "0.1.0"
    assert payload["artifact_identity"] == "sha256:fixture"
    assert payload["capability_matrix"] == {
        "nested_schema": True,
        "splittable_scan": True,
    }
    assert result.checks["bounded_plan"] == "passed"


class _EndlessFormat(_ConformingFormat):
    def plan(self, handle, schema, context, *, projection, row_filter, max_tasks):
        del handle, context, projection, row_filter, max_tasks
        while True:
            yield schema


class _MutatingFormat(_ConformingFormat):
    def execute(self, task, context):
        del context
        schema = pa.schema([pa.field("secret", pa.string())])
        return schema, [pa.RecordBatch.from_pylist([{"secret": "x"}], schema=schema)]


class _CleanupFormat(_MutatingFormat):
    closed = False

    def close(self):
        self.closed = True


class _CoverageFormat(_ConformingFormat):
    def plan(self, handle, schema, context, *, projection, row_filter, max_tasks):
        del handle, context, projection, row_filter, max_tasks
        return ["part-a", "part-b"]

    def execute(self, task, context):
        del task, context
        schema = pa.schema([pa.field("id", pa.int64())])
        return schema, [pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)]


class _DuplicateCoverageFormat(_CoverageFormat):
    def plan(self, handle, schema, context, *, projection, row_filter, max_tasks):
        del handle, schema, context, projection, row_filter, max_tasks
        return ["part-a", "part-a"]


def test_runner_rejects_unbounded_plan_and_output_schema_mutation():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(
        schema_version=1,
        fingerprint="0" * 64,
        arrow_schema=table.schema,
    )
    handle = TableHandle(
        catalog_plugin_id="fixture",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="fixture",
        handle_version=1,
    )

    endless = run_format_checks(
        cast(TableFormatPlugin, _EndlessFormat()), handle, schema, _context(), max_tasks=2
    )
    mutated = run_format_checks(
        cast(TableFormatPlugin, _MutatingFormat()), handle, schema, _context()
    )

    assert endless.to_dict()["status"] == "failed"
    assert any("more tasks" in failure for failure in endless.failures)
    assert mutated.to_dict()["status"] == "failed"
    assert any("output schema" in failure for failure in mutated.failures)


def test_runner_closes_plugin_after_failure_and_serializes_skips():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(
        schema_version=1,
        fingerprint="0" * 64,
        arrow_schema=table.schema,
    )
    handle = TableHandle(
        catalog_plugin_id="fixture",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="fixture",
        handle_version=1,
    )
    plugin = _CleanupFormat()

    result = run_format_checks(cast(TableFormatPlugin, plugin), handle, schema, _context())
    result.record_skip("provider", "provider fixture is not configured")

    assert plugin.closed is True
    payload = result.to_dict()
    checks = cast(dict[str, str], payload["checks"])
    skips = cast(list[str], payload["skips"])
    assert checks["cleanup"] == "passed"
    assert checks["provider"] == "skipped"
    assert "provider: provider fixture is not configured" in skips


def test_runner_rejects_duplicate_or_missing_task_coverage():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(
        schema_version=1,
        fingerprint="0" * 64,
        arrow_schema=table.schema,
    )
    handle = TableHandle(
        catalog_plugin_id="fixture",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="fixture",
        handle_version=1,
    )
    passing = run_format_checks(
        cast(TableFormatPlugin, _CoverageFormat()),
        handle,
        schema,
        _context(),
        expected_task_ids=("part-a", "part-b"),
        task_identity=lambda task: str(task),
    )
    duplicate = run_format_checks(
        cast(TableFormatPlugin, _DuplicateCoverageFormat()),
        handle,
        schema,
        _context(),
        expected_task_ids=("part-a", "part-b"),
        task_identity=lambda task: str(task),
    )

    assert passing.checks["task_coverage"] == "passed"
    assert duplicate.to_dict()["status"] == "failed"
    assert any("duplicate task identities" in failure for failure in duplicate.failures)


def test_runner_checks_cancellation_before_requesting_more_plan_work():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(
        schema_version=1,
        fingerprint="0" * 64,
        arrow_schema=table.schema,
    )
    handle = TableHandle(
        catalog_plugin_id="fixture",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="fixture",
        handle_version=1,
    )
    calls = 0

    def cancelled() -> bool:
        nonlocal calls
        calls += 1
        return calls >= 3

    requested = 0

    class _CancelledPlan(_ConformingFormat):
        def plan(self, handle, schema, context, *, projection, row_filter, max_tasks):
            del handle, context, projection, row_filter, max_tasks
            nonlocal requested
            requested += 1
            yield schema
            requested += 1
            yield schema

    result = run_format_checks(
        cast(TableFormatPlugin, _CancelledPlan()),
        handle,
        schema,
        replace(_context(), cancel_check=cancelled),
        max_tasks=2,
    )

    assert result.to_dict()["status"] == "failed"
    assert any("cancelled while planning" in failure for failure in result.failures)
    assert requested == 1


def test_runner_checks_cancellation_before_requesting_more_output():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(
        schema_version=1,
        fingerprint="0" * 64,
        arrow_schema=table.schema,
    )
    handle = TableHandle(
        catalog_plugin_id="fixture",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="fixture",
        handle_version=1,
    )
    calls = 0

    def cancelled() -> bool:
        nonlocal calls
        calls += 1
        return calls >= 6

    requested = 0

    class _CancelledOutput(_ConformingFormat):
        def execute(self, task, context):
            del context

            def batches():
                nonlocal requested
                requested += 1
                yield pa.RecordBatch.from_pylist([{"id": 1}], schema=task.arrow_schema)
                requested += 1
                yield pa.RecordBatch.from_pylist([{"id": 2}], schema=task.arrow_schema)

            return task.arrow_schema, batches()

    result = run_format_checks(
        cast(TableFormatPlugin, _CancelledOutput()),
        handle,
        schema,
        replace(_context(), cancel_check=cancelled),
    )

    assert result.to_dict()["status"] == "failed"
    assert any("cancelled while reading output" in failure for failure in result.failures)
    assert requested == 1


def test_record_batch_validation_rejects_expired_deadline():
    schema = pa.schema([pa.field("id", pa.int64())])
    batch = pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)

    with pytest.raises(TimeoutError, match="deadline expired"):
        check_record_batches(
            schema,
            [batch],
            deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
        )


def test_runner_honors_cancellation_before_plugin_execution():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(
        schema_version=1,
        fingerprint="0" * 64,
        arrow_schema=table.schema,
    )
    handle = TableHandle(
        catalog_plugin_id="fixture",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="fixture",
        handle_version=1,
    )

    result = run_format_checks(
        cast(TableFormatPlugin, _ConformingFormat()),
        handle,
        schema,
        replace(_context(), cancel_check=lambda: True),
    )

    assert result.to_dict()["status"] == "failed"
    assert any("cancelled" in failure for failure in result.failures)


def test_runner_honors_cancellation_between_tasks():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(
        schema_version=1,
        fingerprint="0" * 64,
        arrow_schema=table.schema,
    )
    handle = TableHandle(
        catalog_plugin_id="fixture",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="fixture",
        handle_version=1,
    )
    checks = 0

    def cancel_after_first_task_check() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 3

    result = run_format_checks(
        cast(TableFormatPlugin, _CoverageFormat()),
        handle,
        schema,
        replace(_context(), cancel_check=cancel_after_first_task_check),
        expected_task_ids=("part-a", "part-b"),
        task_identity=str,
    )

    assert result.to_dict()["status"] == "failed"
    assert any("cancelled" in failure for failure in result.failures)
