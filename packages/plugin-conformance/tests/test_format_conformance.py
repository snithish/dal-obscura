from __future__ import annotations

from dataclasses import replace
from typing import cast

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import (
    ScanRequest,
    ScanTask,
    SchemaDescriptor,
    TableFormatPlugin,
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
    schema = SchemaDescriptor(arrow_schema=table.schema)

    result = run_format_checks(
        cast(TableFormatPlugin, _ConformingFormat()),
        ScanRequest(schema.arrow_schema, 64, None),
        _context(),
        required_capabilities={"nested_schema"},
        artifact_identity="sha256:fixture",
    )

    payload = result.to_dict()
    assert payload["status"] == "passed"
    assert payload["core_version"] == "0.2.0"
    assert payload["artifact_identity"] == "sha256:fixture"
    assert payload["capability_matrix"] == {"nested_schema": True, "splittable_scan": True}
    assert result.checks["bounded_plan"] == "passed"


def test_runner_requires_explicit_format_cleanup() -> None:
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(arrow_schema=table.schema)

    class NoClose(_ConformingFormat):
        close = None

    result = run_format_checks(
        cast(TableFormatPlugin, NoClose()), ScanRequest(schema.arrow_schema, 64, None), _context()
    )

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
    schema = SchemaDescriptor(arrow_schema=table.schema)

    result = run_format_checks(
        cast(TableFormatPlugin, plugin_type()),
        ScanRequest(schema.arrow_schema, 2, None),
        _context(),
    )

    assert result.to_dict()["status"] == "failed"
    assert any(message in failure for failure in result.failures)


def test_runner_closes_plugin_after_failure_and_serializes_skips():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(arrow_schema=table.schema)
    plugin = _CleanupFormat()

    result = run_format_checks(
        cast(TableFormatPlugin, plugin), ScanRequest(schema.arrow_schema, 64, None), _context()
    )
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
    schema = SchemaDescriptor(arrow_schema=table.schema)
    passing = run_format_checks(
        cast(TableFormatPlugin, _CoverageFormat()),
        ScanRequest(schema.arrow_schema, 64, None),
        _context(),
        expected_task_ids=("part-a", "part-b"),
        task_identity=lambda task: str(task.payload["id"]),
    )
    duplicate = run_format_checks(
        cast(TableFormatPlugin, _DuplicateCoverageFormat()),
        ScanRequest(schema.arrow_schema, 64, None),
        _context(),
        expected_task_ids=("part-a", "part-b"),
        task_identity=lambda task: str(task.payload["id"]),
    )

    assert passing.checks["task_coverage"] == "passed"
    assert duplicate.to_dict()["status"] == "failed"
    assert any("duplicate task identities" in failure for failure in duplicate.failures)


def test_runner_checks_cancellation_before_requesting_more_plan_work():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(arrow_schema=table.schema)
    cancelled = False

    requested = 0

    class _CancelledPlan(_ConformingFormat):
        def plan(self, request, context):
            del context
            nonlocal requested, cancelled
            requested += 1
            cancelled = True
            yield ScanTask({"id": "scan"})
            requested += 1
            yield ScanTask({"id": "scan"})

    result = run_format_checks(
        cast(TableFormatPlugin, _CancelledPlan()),
        ScanRequest(schema.arrow_schema, 2, None),
        replace(_context(), cancel_check=lambda: cancelled),
    )

    assert result.to_dict()["status"] == "failed"
    assert any("cancelled" in failure for failure in result.failures)
    assert requested == 1


def test_runner_checks_cancellation_before_requesting_more_output():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(arrow_schema=table.schema)
    cancelled = False

    requested = 0

    class _CancelledOutput(_ConformingFormat):
        def execute(self, task, context):
            del context

            def batches():
                nonlocal requested, cancelled
                requested += 1
                cancelled = True
                yield pa.RecordBatch.from_pylist([{"id": 1}], schema=schema.arrow_schema)
                requested += 1
                yield pa.RecordBatch.from_pylist([{"id": 2}], schema=schema.arrow_schema)

            return schema.arrow_schema, batches()

    result = run_format_checks(
        cast(TableFormatPlugin, _CancelledOutput()),
        ScanRequest(schema.arrow_schema, 64, None),
        replace(_context(), cancel_check=lambda: cancelled),
    )

    assert result.to_dict()["status"] == "failed"
    assert any("cancelled" in failure for failure in result.failures)
    assert requested == 1


def test_runner_honors_cancellation_before_plugin_execution():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(arrow_schema=table.schema)

    result = run_format_checks(
        cast(TableFormatPlugin, _ConformingFormat()),
        ScanRequest(schema.arrow_schema, 64, None),
        replace(_context(), cancel_check=lambda: True),
    )

    assert result.to_dict()["status"] == "failed"
    assert any("cancelled" in failure for failure in result.failures)


def test_runner_honors_cancellation_between_tasks():
    table = pa.table({"id": [1]})
    schema = SchemaDescriptor(arrow_schema=table.schema)
    cancelled = False
    executed = []

    class CancelBetweenTasks(_CoverageFormat):
        def execute(self, task, context):
            nonlocal cancelled
            executed.append(task.payload["id"])
            declared, batches = super().execute(task, context)

            def stream():
                nonlocal cancelled
                yield from batches
                cancelled = True

            return declared, stream()

    result = run_format_checks(
        cast(TableFormatPlugin, CancelBetweenTasks()),
        ScanRequest(schema.arrow_schema, 64, None),
        replace(_context(), cancel_check=lambda: cancelled),
        expected_task_ids=("part-a", "part-b"),
        task_identity=lambda task: str(task.payload["id"]),
    )

    assert result.to_dict()["status"] == "failed"
    assert any("cancelled" in failure for failure in result.failures)

    assert executed == ["part-a"]


def test_runner_checks_partial_projection_with_metadata_and_closes_overbudget_plan():
    from dal_obscura_plugin_api import ScanTask

    full = pa.schema(
        [("id", pa.int64()), ("private", pa.string())], metadata={b"source": b"fixture"}
    )
    projected = pa.schema([full.field("id")], metadata=full.metadata)
    closed = []

    class Projected(_ConformingFormat):
        def schema(self, context):
            return SchemaDescriptor(full)

        def plan(self, request, context):
            try:
                yield ScanTask({"rows": [{"id": 1}]})
                yield ScanTask({"rows": [{"id": 2}]})
            finally:
                closed.append("plan")

        def execute(self, task, context):
            return projected, [pa.RecordBatch.from_pylist(task.to_json()["rows"], schema=projected)]

    passing = run_format_checks(
        cast(TableFormatPlugin, Projected()), ScanRequest(projected, 2), _context()
    )
    assert passing.to_dict()["status"] == "passed"
    failed = run_format_checks(
        cast(TableFormatPlugin, Projected()), ScanRequest(projected, 1), _context()
    )
    assert any("more tasks" in reason for reason in failed.failures)
    assert closed == ["plan", "plan"]


def test_runner_closes_plan_when_iterator_construction_and_cleanup_fail():
    closed = []

    class BrokenPlan:
        def __iter__(self):
            raise ValueError("cannot start planning")

        def close(self):
            closed.append("plan")
            raise RuntimeError("plan close failed")

    class BrokenFormat(_ConformingFormat):
        def plan(self, request, context):
            return BrokenPlan()

        def close(self):
            closed.append("plugin")

    result = run_format_checks(
        cast(TableFormatPlugin, BrokenFormat()),
        ScanRequest(pa.schema([("id", pa.int64())]), 1),
        _context(),
    )
    assert any("cannot start planning" in reason for reason in result.failures)
    assert closed == ["plan", "plugin"]
