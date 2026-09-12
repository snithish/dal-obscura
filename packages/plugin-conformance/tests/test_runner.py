from __future__ import annotations

from datetime import datetime, timezone
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
        deadline=datetime.now(timezone.utc),
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
    )

    assert result.to_dict()["status"] == "passed"
    assert result.checks["bounded_plan"] == "passed"
