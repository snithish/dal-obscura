from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from dal_obscura_plugin_api import (
    ExecutionContext,
    PluginDescriptor,
    ScanTask,
    SchemaDescriptor,
)


def _descriptor() -> PluginDescriptor:
    return PluginDescriptor(
        kind="table_format",
        plugin_id="fixture",
        api_version="2",
        config_version=1,
        distribution="fixture-package",
        version="1.0.0",
        capabilities=frozenset({"nested_schema", "splittable_scan"}),
    )


def _context() -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1), correlation_id="fixture"
    )


class _ConformingFormat:
    descriptor = _descriptor()

    def close(self):
        return None

    def schema(self, context):
        return SchemaDescriptor(pa.schema([("id", pa.int64())]))

    def plan(self, request, context):
        del context
        return [ScanTask({"rows": [{"id": 1}]})]

    def execute(self, task, context):
        del context
        schema = pa.schema([("id", pa.int64())])
        return schema, [pa.RecordBatch.from_pylist(task.to_json()["rows"], schema=schema)]


class _EndlessFormat(_ConformingFormat):
    def plan(self, request, context):
        del context
        while True:
            yield ScanTask({"rows": [{"id": 1}]})


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
    def plan(self, request, context):
        del context
        return [ScanTask({"id": identity}) for identity in ("part-a", "part-b")]

    def execute(self, task, context):
        del task, context
        schema = pa.schema([pa.field("id", pa.int64())])
        return schema, [pa.RecordBatch.from_pylist([{"id": 1}], schema=schema)]


class _DuplicateCoverageFormat(_CoverageFormat):
    def plan(self, request, context):
        del request, context
        return [ScanTask({"id": "part-a"})] * 2


def _catalog_descriptor() -> PluginDescriptor:
    return PluginDescriptor(
        kind="catalog",
        plugin_id="fixture.catalog",
        api_version="2",
        config_version=1,
        distribution="fixture-catalog-package",
        version="1.0.0",
    )


def _catalog_context(**kwargs):
    return replace(_context(), **kwargs)
