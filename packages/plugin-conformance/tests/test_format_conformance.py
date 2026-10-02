from __future__ import annotations

from dataclasses import replace
from typing import cast

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import (
    SchemaDescriptor,
    TableFormatPlugin,
    TableHandle,
    TableIdentifier,
)
from dal_obscura_plugin_conformance import (
    run_format_checks,
)
from plugin_conformance_fakes import (
    _CleanupFormat,
    _ConformingFormat,
    _context,
    _CoverageFormat,
    _DuplicateCoverageFormat,
    _EndlessFormat,
    _MutatingFormat,
)


def test_runner_returns_machine_readable_passing_result():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=table.schema)
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
    assert payload["capability_matrix"] == {"nested_schema": True, "splittable_scan": True}
    assert result.checks["bounded_plan"] == "passed"


def test_runner_requires_explicit_format_cleanup() -> None:
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=table.schema)
    handle = TableHandle(
        catalog_plugin_id="fixture",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="fixture",
        handle_version=1,
    )

    class NoClose(_ConformingFormat):
        close = None

    result = run_format_checks(cast(TableFormatPlugin, NoClose()), handle, schema, _context())

    assert result.to_dict()["status"] == "failed"
    assert any("must expose close" in failure for failure in result.failures)


@pytest.mark.parametrize(
    "plugin_type,message",
    [
        pytest.param(_EndlessFormat, "more tasks", id="unbounded-plan"),
        pytest.param(_MutatingFormat, "output schema", id="schema-mutation"),
    ],
)
def test_runner_rejects_invalid_format_behavior(plugin_type, message):
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=table.schema)
    handle = TableHandle(
        catalog_plugin_id="fixture",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="fixture",
        handle_version=1,
    )

    result = run_format_checks(
        cast(TableFormatPlugin, plugin_type()), handle, schema, _context(), max_tasks=2
    )

    assert result.to_dict()["status"] == "failed"
    assert any(message in failure for failure in result.failures)


def test_runner_closes_plugin_after_failure_and_serializes_skips():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=table.schema)
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
    schema = SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=table.schema)
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
    schema = SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=table.schema)
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
    schema = SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=table.schema)
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


def test_runner_honors_cancellation_before_plugin_execution():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=table.schema)
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
    schema = SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=table.schema)
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
