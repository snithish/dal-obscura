"""Public scan boundary: immutable JSON work and an exact projected schema."""

import json
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import cast

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import (
    CatalogConfig,
    ExecutionContext,
    PluginDescriptor,
    ScanRequest,
    ScanTask,
)


def test_scan_task_detaches_nested_input_and_round_trips_without_live_objects():
    files = [{"path": "data.parquet", "groups": [0, 2]}]
    task = ScanTask({"files": files, "limit": None})
    files[0]["path"] = "other.parquet"
    restored = ScanTask.from_json(json.loads(json.dumps(task.to_json())))
    assert restored == task
    assert task.to_json()["files"] == [{"path": "data.parquet", "groups": [0, 2]}]
    nested = cast(tuple[Mapping[str, object], ...], task.payload["files"])[0]
    with pytest.raises(TypeError):
        cast(dict[str, object], nested)["path"] = "mutated.parquet"


@pytest.mark.parametrize(
    "payload", [None, [], {"file": object()}, {"rows": float("inf")}, {"count": 10**100}]
)
def test_scan_tasks_reject_nonportable_payloads(payload):
    with pytest.raises(ValueError):
        ScanTask.from_json(payload)


def test_scan_request_uses_exact_projected_arrow_names_in_order():
    schema = pa.schema([("literal.dot", pa.int64()), ("*", pa.string())])
    request = ScanRequest(schema=schema, max_tasks=2, row_filter="region = 'EU'")
    assert request.columns == ("literal.dot", "*")
    assert request.schema.equals(schema, check_metadata=True)


@pytest.mark.parametrize("maximum", [False, 0, -1])
def test_scan_request_rejects_invalid_work_budgets(maximum):
    with pytest.raises(ValueError, match="max_tasks"):
        ScanRequest(schema=pa.schema([]), max_tasks=maximum)


def test_catalog_config_and_descriptor_are_detached_before_admission():
    values = ["first"]
    config = CatalogConfig("manifest", "source", 1, {"nested": {"values": values}})
    fields = [{"name": "root", "required": True}]
    descriptor = PluginDescriptor(
        "catalog", "manifest", "2", 1, "fixture", "0.2.0", config_schema={"fields": fields}
    )
    values.append("later")
    fields[0]["required"] = False
    nested = cast(Mapping[str, object], config.options["nested"])
    assert nested["values"] == ("first",)
    assert descriptor.to_json()["config_schema"] == {"fields": [{"name": "root", "required": True}]}
    with pytest.raises(TypeError):
        cast(dict[str, object], nested)["values"] = ()


def test_operation_guard_rejects_expired_or_cancelled_work_before_io():
    with pytest.raises(TimeoutError, match="deadline"):
        ExecutionContext(
            datetime.now(timezone.utc) - timedelta(seconds=1), "expired"
        ).check_active()
    with pytest.raises(InterruptedError, match="cancelled"):
        ExecutionContext(
            datetime.now(timezone.utc) + timedelta(minutes=1),
            "cancelled",
            cancel_check=lambda: True,
        ).check_active()
