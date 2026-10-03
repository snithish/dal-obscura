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
from pyiceberg.types import LongType, StringType

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
    assert len(tasks) == 3
    assigned = []
    for task in tasks:
        files = task.to_json()["files"]
        assert isinstance(files, list)
        assigned.extend(files)
    assert len(assigned) == len(set(assigned))
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
                    "ranges": [[0, 1]],
                }
            ),
            context(),
        )


@pytest.mark.parametrize(
    "ranges",
    [[[True, 1]], [[-1, 1]], [[0, 0]], [[0, 2**63]], []],
    ids=["boolean", "negative", "empty", "outside-file", "missing"],
)
def test_native_task_rejects_invalid_byte_ranges(table, ranges):
    plugin = IcebergFormatPlugin(handle(table), context())
    task = plugin.plan(ScanRequest(plugin.schema(context()).arrow_schema, 2), context())[
        0
    ].to_json()
    task["ranges"] = ranges
    with pytest.raises(ValueError, match="Invalid Iceberg task"):
        plugin.execute(ScanTask(task), context())


def test_native_task_rejects_overlapping_ranges(table):
    plugin = IcebergFormatPlugin(handle(table), context())
    task = plugin.plan(ScanRequest(plugin.schema(context()).arrow_schema, 2), context())[
        0
    ].to_json()
    files, ranges = task["files"], task["ranges"]
    assert isinstance(files, list) and isinstance(ranges, list)
    task["files"], task["ranges"] = files * 2, ranges * 2
    with pytest.raises(ValueError, match="Overlapping Iceberg task ranges"):
        plugin.execute(ScanTask(task), context())


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


@pytest.mark.parametrize("change", ["rename-key", "replace-key"])
def test_schema_evolution_keeps_historical_equality_keys(table, change):
    from tests.support.plugin_scans import parallel_rows

    install_delete_file(table, kind="equality", ids=[1])
    with table.update_schema() as update:
        if change == "rename-key":
            update.rename_column("id", "customer_id")
        else:
            update.delete_column("id")
            update.add_column("id", LongType())
    key = "customer_id" if change == "rename-key" else "id"
    table.append(
        pa.table(
            {key: [99], "email": ["new@example.com"], "region": ["us"]},
            schema=table.schema().as_arrow(),
        )
    )
    plugin = IcebergFormatPlugin(handle(table), context())
    schema = plugin.schema(context()).arrow_schema
    tasks = plugin.plan(ScanRequest(schema, 3), context())
    plugin.close()
    rows = parallel_rows(IcebergFormatPlugin, handle(table), tasks, context())
    expected = [
        {
            key: i if change == "rename-key" else None,
            "email": f"user{i}@example.com",
            "region": "us" if i % 2 == 0 else "eu",
        }
        for i in (0, 2, 3, 4, 5)
    ] + [{key: 99, "email": "new@example.com", "region": "us"}]
    assert sorted(rows, key=lambda row: row["email"]) == sorted(
        expected, key=lambda row: row["email"]
    )


def test_native_declines_unsupported_sql_filter_hint(table):
    plugin = IcebergFormatPlugin(handle(table), context())
    schema = plugin.schema(context()).arrow_schema
    projected = pa.schema([schema.field("email")], metadata=schema.metadata)
    assert "filter_pushdown" not in plugin.descriptor.capabilities
    with pytest.raises(ValueError, match="does not support SQL filter pushdown"):
        plugin.plan(ScanRequest(projected, 2, '"id" < 3'), context())


def test_composite_nullable_equality_keys_are_hidden_from_output(tmp_path):
    from tests.support.plugin_scans import parallel_rows

    schema = pa.schema([("id", pa.int64()), ("email", pa.string()), ("region", pa.string())])
    rows = [
        {"id": i, "email": email, "region": region}
        for i, (email, region) in enumerate(
            [(None, "us"), (None, "eu"), ("a", "us"), ("a", "eu"), ("a", None), (None, None)]
        )
    ]
    create_iceberg_table(
        tmp_path,
        "nullable",
        "nullable-warehouse",
        arrow_schema=schema,
        append_tables=[pa.Table.from_pylist(rows, schema=schema)],
    )
    catalog = load_catalog(
        "nullable", **iceberg_sql_catalog_options(tmp_path, "nullable", "nullable-warehouse")
    )
    table = catalog.load_table("default.users")
    install_delete_file(
        table,
        kind="equality",
        equality_columns=("email", "region"),
        equality_records=[
            {"email": None, "region": "us"},
            {"email": "a", "region": "us"},
            {"email": None, "region": None},
        ],
    )
    plugin = IcebergFormatPlugin(handle(table), context())
    full_schema = plugin.schema(context()).arrow_schema
    projected = pa.schema([full_schema.field("id")], metadata=full_schema.metadata)
    tasks = plugin.plan(ScanRequest(projected, 3), context())
    plugin.close()
    assert sorted(
        parallel_rows(IcebergFormatPlugin, handle(table), tasks, context()),
        key=lambda row: row["id"],
    ) == [{"id": 1}, {"id": 3}, {"id": 4}]


def test_equality_deletes_respect_partition_scope(tmp_path):
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    from tests.support.plugin_scans import parallel_rows

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            ("email", pa.string()),
            ("region", pa.string()),
        ]
    )
    rows = [
        {"id": 1, "email": "us@example.com", "region": "us"},
        {"id": 1, "email": "eu@example.com", "region": "eu"},
    ]
    create_iceberg_table(
        tmp_path,
        "partitioned",
        "partitioned-warehouse",
        append_tables=[pa.Table.from_pylist(rows, schema=schema)],
        partition_spec=PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="region")
        ),
    )
    catalog = load_catalog(
        "partitioned",
        **iceberg_sql_catalog_options(tmp_path, "partitioned", "partitioned-warehouse"),
    )
    table = catalog.load_table("default.users")
    install_delete_file(table, kind="equality", ids=[1], partition=("us",))
    plugin = IcebergFormatPlugin(handle(table), context())
    tasks = plugin.plan(ScanRequest(plugin.schema(context()).arrow_schema, 2), context())
    plugin.close()
    assert len(tasks) == 2
    assert parallel_rows(IcebergFormatPlugin, handle(table), tasks, context()) == [rows[1]]


@pytest.mark.parametrize("mode", ["copy-on-write", "position", "equality"])
def test_one_file_splits_row_groups_with_exact_delete_coverage(tmp_path, mode):
    from tests.support.plugin_scans import distributed_rows

    create_iceberg_table(
        tmp_path,
        "groups",
        "groups-warehouse",
        append_batches=[list(range(16))],
        table_properties={"write.parquet.row-group-limit": 4},
    )
    catalog = load_catalog(
        "groups", **iceberg_sql_catalog_options(tmp_path, "groups", "groups-warehouse")
    )
    table = catalog.load_table("default.users")
    if mode == "copy-on-write":
        table.delete("id IN (1, 4, 9)")
    else:
        install_delete_file(table, kind=mode, ids=[1, 4, 9])
    plugin = IcebergFormatPlugin(handle(table), context())
    schema = plugin.schema(context()).arrow_schema
    projected = pa.schema([schema.field("id")], metadata=schema.metadata)
    tasks = plugin.plan(ScanRequest(projected, 3), context())
    plugin.close()
    assert len(tasks) == 3
    assert sorted(
        distributed_rows(IcebergFormatPlugin, handle(table), tasks), key=lambda row: row["id"]
    ) == [{"id": i} for i in range(16) if i not in (1, 4, 9)]


@pytest.mark.parametrize("stop", ["exhaustion", "early-close", "cancel", "failure"])
def test_native_stream_owns_reader(table, stop):
    from threading import Event

    cancelled = Event()
    operation = replace(context(), cancel_check=cancelled.is_set)
    plugin = IcebergFormatPlugin(handle(table), operation)
    task = plugin.plan(ScanRequest(plugin.schema(operation).arrow_schema, 1), operation)[0]
    schema = table.schema().as_arrow()
    batch = pa.RecordBatch.from_pylist([{"id": 0, "email": "a", "region": "us"}], schema=schema)

    from tests.support.iceberg import NativeReaderStub

    reader = NativeReaderStub(batch, fail=stop == "failure")
    plugin._reader = reader
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
    assert reader.closed == ["reader"]


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
