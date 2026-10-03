"""Pinned native reads through the public SDK, with independent delete fixtures."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import (
    ExecutionContext,
    ScanRequest,
    ScanTask,
    TableHandle,
    TableIdentifier,
)
from pyiceberg.catalog import load_catalog
from pyiceberg.types import StringType

from dal_obscura.sources.iceberg_plugin import IcebergFormatPlugin
from dal_obscura.sources.paths import PathRuleEnforcer
from tests.support.iceberg import (
    create_iceberg_table,
    iceberg_sql_catalog_options,
    install_delete_file,
)


def context():
    return ExecutionContext(datetime.now(timezone.utc) + timedelta(minutes=2), "iceberg-test")


def handle(table):
    snapshot = table.current_snapshot()
    return TableHandle(
        "iceberg.sql",
        "fixture",
        1,
        TableIdentifier(("default",), "users"),
        "iceberg",
        1,
        str(snapshot.snapshot_id) if snapshot else None,
        {"metadata_location": table.metadata_location},
    )


@pytest.fixture
def table(tmp_path):
    identifier = create_iceberg_table(
        tmp_path, "sdk", "warehouse", append_batches=[[0, 1, 2], [3, 4, 5]]
    )
    catalog = load_catalog(
        "sdk",
        **{
            key: str(value)
            for key, value in iceberg_sql_catalog_options(tmp_path, "sdk", "warehouse").items()
        },
    )
    return catalog.load_table(identifier)


@pytest.mark.parametrize("mode", ["copy-on-write", "position", "equality"])
@pytest.mark.parametrize(
    "columns",
    [["id"], ["region", "id", "email"], ["email"]],
    ids=["key", "reordered", "hidden-delete-key"],
)
def test_native_parallel_reads_pin_cow_and_mor_updates(table, mode, columns):
    from tests.support.plugin_scans import parallel_rows

    # Deletes old id 1, then inserts its replacement. Equality deletes must not
    # remove the newer replacement with the same key.
    if mode == "copy-on-write":
        table.delete("id == 1")
    else:
        install_delete_file(table, kind=mode, ids=[1])
    table.append(
        pa.table(
            {"id": [1], "email": ["updated@example.com"], "region": ["eu"]},
            schema=table.schema().as_arrow(),
        )
    )
    captured = handle(table)
    plugin = IcebergFormatPlugin(captured, context())
    schema = plugin.schema(context()).arrow_schema
    projected = pa.schema([schema.field(name) for name in columns], metadata=schema.metadata)
    tasks = plugin.plan(ScanRequest(projected, 3), context())
    plugin.close()
    assert len(tasks) == (1 if mode == "equality" else 3)
    assert all(task.to_json()["parallelism"] == (3 if mode == "equality" else 1) for task in tasks)
    table.append(
        pa.table(
            {"id": [99], "email": ["later@example.com"], "region": ["us"]},
            schema=table.schema().as_arrow(),
        )
    )
    rows = parallel_rows(IcebergFormatPlugin, captured, tasks, context())
    expected = [
        {
            key: value
            for key, value in {
                "id": i,
                "email": "updated@example.com" if i == 1 else f"user{i}@example.com",
                "region": "us" if i % 2 == 0 else "eu",
            }.items()
            if key in columns
        }
        for i in range(6)
    ]
    assert sorted(rows, key=lambda row: sorted(row.items())) == sorted(
        expected, key=lambda row: sorted(row.items())
    )


def test_native_task_rejects_files_outside_snapshot(table):
    plugin = IcebergFormatPlugin(handle(table), context())
    with pytest.raises(ValueError, match="outside its pinned snapshot"):
        plugin.execute(
            ScanTask(
                {
                    "columns": ["id"],
                    "files": ["/other/file.parquet"],
                    "parallelism": 1,
                    "row_filter": None,
                }
            ),
            context(),
        )


def test_storage_allowlist_rejects_metadata_before_io(table):
    plugin = IcebergFormatPlugin(
        handle(table), context(), path_enforcer=PathRuleEnforcer([{"root": "/unrelated"}])
    )
    with pytest.raises(PermissionError):
        plugin.schema(context())


def test_iceberg_rejects_changed_snapshot_identity(table):
    plugin = IcebergFormatPlugin(replace(handle(table), snapshot_id="123"), context())
    with pytest.raises(ValueError, match="snapshot identity changed"):
        plugin.schema(context())


def test_empty_snapshot_has_no_tasks(tmp_path):
    create_iceberg_table(tmp_path, "empty", "empty-warehouse", append_tables=[])
    catalog = load_catalog(
        "empty", **iceberg_sql_catalog_options(tmp_path, "empty", "empty-warehouse")
    )
    plugin = IcebergFormatPlugin(handle(catalog.load_table("default.users")), context())
    assert plugin.plan(ScanRequest(plugin.schema(context()).arrow_schema, 4), context()) == []


def test_nested_schema_and_evolution_preserve_values_and_metadata(tmp_path):
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("literal.name", pa.string()),
            pa.field("profile", pa.struct([pa.field("tags", pa.list_(pa.string()))])),
            pa.field("attributes", pa.map_(pa.string(), pa.int64())),
        ]
    )
    rows = [
        {"id": 1, "literal.name": "a", "profile": {"tags": ["x", None]}, "attributes": [("k", 2)]},
        {"id": 2, "literal.name": None, "profile": None, "attributes": []},
    ]
    create_iceberg_table(
        tmp_path,
        "nested",
        "nested-warehouse",
        arrow_schema=schema,
        append_tables=[pa.Table.from_pylist(rows, schema=schema)],
    )
    catalog = load_catalog(
        "nested", **iceberg_sql_catalog_options(tmp_path, "nested", "nested-warehouse")
    )
    table = catalog.load_table("default.users")
    with table.update_schema() as update:
        update.add_column("new_field", StringType())
    plugin = IcebergFormatPlugin(handle(table), context())
    expected_schema = plugin.schema(context()).arrow_schema
    tasks = plugin.plan(ScanRequest(expected_schema, 2), context())
    output_schema, batches = plugin.execute(tasks[0], context())
    output = pa.Table.from_batches(batches, schema=output_schema)
    assert output.schema.equals(expected_schema, check_metadata=True)
    assert output.to_pylist() == [dict(row, new_field=None) for row in rows]


@pytest.mark.parametrize("mode", ["position", "equality"])
def test_native_filter_hint_retains_hidden_delete_keys(table, mode):
    install_delete_file(table, kind=mode, ids=[1])
    plugin = IcebergFormatPlugin(handle(table), context())
    schema = plugin.schema(context()).arrow_schema
    projected = pa.schema([schema.field("email")], metadata=schema.metadata)
    tasks = plugin.plan(ScanRequest(projected, 2, '"id" < 3 AND "region" IS NOT NULL'), context())
    from tests.support.plugin_scans import parallel_rows

    assert sorted(
        row["email"] for row in parallel_rows(IcebergFormatPlugin, handle(table), tasks, context())
    ) == ["user0@example.com", "user2@example.com"]


@pytest.mark.parametrize("stop", ["exhaustion", "early-close", "cancel", "failure"])
def test_native_stream_owns_reader_and_connection(table, monkeypatch, stop):
    from threading import Event

    import dal_obscura.sources.iceberg_plugin as native

    cancelled = Event()
    operation = replace(context(), cancel_check=cancelled.is_set)
    plugin = IcebergFormatPlugin(handle(table), operation)
    task = plugin.plan(ScanRequest(plugin.schema(operation).arrow_schema, 1), operation)[0]
    schema = table.schema().as_arrow()
    batch = pa.RecordBatch.from_pylist([{"id": 0, "email": "a", "region": "us"}], schema=schema)

    from tests.support.iceberg import NativeConnectionStub

    connection = NativeConnectionStub(batch, fail=stop == "failure")
    monkeypatch.setattr(native, "_connection", lambda *args: connection)
    _, batches = plugin.execute(task, operation)
    if stop == "failure":
        with pytest.raises(RuntimeError, match="native read failed"):
            next(batches)
    elif stop == "exhaustion":
        assert sum(item.num_rows for item in batches) == 2
    else:
        assert next(batches).to_pylist() == batch.to_pylist()
        if stop == "cancel":
            cancelled.set()
            with pytest.raises(InterruptedError):
                next(batches)
        else:
            batches.close()
    assert connection.closed == ["reader", "connection"]


@pytest.mark.parametrize("location", ["historical-metadata", "manifest-list", "delete-file"])
def test_native_storage_allowlist_checks_all_provider_locations(table, tmp_path, location):
    import json
    from pathlib import Path
    from urllib.parse import unquote, urlsplit

    outside = str(tmp_path / "outside.parquet")
    if location == "delete-file":
        install_delete_file(table, kind="equality", ids=[1], delete_path=outside)
    else:
        path = Path(unquote(urlsplit(table.metadata_location).path))
        metadata = json.loads(path.read_text())
        if location == "historical-metadata":
            metadata["metadata-log"].append({"timestamp-ms": 0, "metadata-file": outside})
        else:
            metadata["snapshots"][-1]["manifest-list"] = outside
        path.write_text(json.dumps(metadata))
    plugin = IcebergFormatPlugin(
        handle(table),
        context(),
        path_enforcer=PathRuleEnforcer([{"root": table.metadata.location}]),
    )
    with pytest.raises(PermissionError):
        plugin.plan(ScanRequest(table.schema().as_arrow(), 2), context())
