from __future__ import annotations

import pytest

from dal_obscura.observability import ServiceMetrics


def test_metrics_aggregate_success_and_error_without_dynamic_labels():
    metrics = ServiceMetrics()

    with metrics.measure("flight.get_schema"):
        pass
    with pytest.raises(RuntimeError), metrics.measure("flight.get_schema"):
        raise RuntimeError("synthetic failure")

    snapshot = metrics.snapshot()
    assert snapshot["flight.get_schema"]["success"]["count"] == 1
    assert snapshot["flight.get_schema"]["error"]["count"] == 1
    assert set(snapshot) == {"flight.get_schema"}


def test_metrics_reject_unbounded_or_high_cardinality_operation_names():
    metrics = ServiceMetrics()

    with pytest.raises(ValueError, match="invalid"):
        metrics.observe("flight.get_schema?principal=secret", "success")
    with pytest.raises(ValueError, match="invalid"):
        metrics.observe("x" * 65, "success")
    with pytest.raises(ValueError, match="outcome"):
        metrics.observe("flight.get_schema", "partial")


def test_metrics_limit_counts_operations_not_outcomes():
    metrics = ServiceMetrics()
    for index in range(64):
        metrics.observe(f"operation.{index}", "success")

    metrics.observe("operation.0", "error", 0.5)
    assert metrics.snapshot()["operation.0"]["error"] == {
        "count": 1,
        "duration_seconds": 0.5,
    }
    with pytest.raises(ValueError, match="operation limit"):
        metrics.observe("operation.64", "success")


@pytest.mark.parametrize("duration", [-1, float("nan"), float("inf"), float("-inf")])
def test_metrics_reject_invalid_durations_without_recording(duration):
    metrics = ServiceMetrics()
    with pytest.raises(ValueError, match="duration"):
        metrics.observe("flight.get_schema", "success", duration)
    assert metrics.snapshot() == {}
