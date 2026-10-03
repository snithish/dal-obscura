"""Delta behavior through the independent SDK boundary, with real transaction logs."""

import json
import shutil
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event

import pyarrow as pa
import pytest
from dal_obscura_delta.catalog import delta_catalog_factory
from dal_obscura_delta.format import delta_format_factory
from dal_obscura_plugin_api import (
    CatalogConfig,
    ScanRequest,
    ScanTask,
    TableHandle,
    TableIdentifier,
)
from deltalake import DeltaTable, WriterProperties, write_deltalake
from deltalake.exceptions import DeltaError

from tests.support.delta import install_deletion_vector


def catalog_for(root, context, entries=None):
    registry = root / "delta-tables.json"
    registry.write_text(
        json.dumps(
            entries
            if entries is not None
            else [{"namespace": ["retail"], "name": "sales", "path": "sales"}]
        )
    )
    return delta_catalog_factory(
        CatalogConfig(
            "delta.directory",
            "fixture",
            1,
            {"root": str(root), "tables_path": str(registry)},
        ),
        context,
    )


def open_format(root, context):
    with catalog_for(root, context) as catalog:
        handle = catalog.resolve_table(TableIdentifier(("retail",), "sales"), context)
    return handle, delta_format_factory(handle, context)


def read_parallel(handle, tasks, context):
    def read(task):
        with delta_format_factory(TableHandle.from_json(handle.to_json()), context) as format:
            schema, batches = format.execute(ScanTask.from_json(task.to_json()), context)
            return pa.Table.from_batches(list(batches), schema=schema).to_pylist()

    with ThreadPoolExecutor(max_workers=4) as pool:
        return sorted(
            (row for rows in pool.map(read, tasks) for row in rows), key=lambda r: r["id"]
        )


def test_parallel_snapshot_survives_updates_deletes_and_later_commits(tmp_path, context):
    path = tmp_path / "sales"
    for start in (0, 10, 20):
        write_deltalake(
            path,
            pa.table(
                {"id": list(range(start, start + 10)), "amount": list(range(start, start + 10))}
            ),
            mode="append",
            writer_properties=WriterProperties(max_row_group_size=3),
        )
    table = DeltaTable(path)
    table.delete("id IN (1, 11, 21)", writer_properties=WriterProperties(max_row_group_size=3))
    table.update(
        updates={"amount": "amount + 100"},
        predicate="id IN (2, 12, 22)",
        writer_properties=WriterProperties(max_row_group_size=3),
    )
    handle, format = open_format(tmp_path, context)
    request = ScanRequest(format.schema(context).arrow_schema, 4)
    tasks = format.plan(request, context)
    assert len(tasks) == 4
    write_deltalake(path, pa.table({"id": [999], "amount": [999]}), mode="append")
    expected = [
        {"id": i, "amount": i + (100 if i in (2, 12, 22) else 0)}
        for i in range(30)
        if i not in (1, 11, 21)
    ]
    assert read_parallel(handle, tasks, context) == expected
    assert (
        catalog_for(tmp_path, context).resolve_table(handle.identifier, context).snapshot_id
        != handle.snapshot_id
    )


@pytest.mark.parametrize("budget", [1, 4, 100], ids=["grouped", "parallel", "all-row-groups"])
@pytest.mark.parametrize("storage", ["i", "u", "p"], ids=["inline", "uuid-file", "absolute-file"])
def test_deletion_vectors_respect_file_row_positions(tmp_path, context, budget, storage):
    path = tmp_path / "sales"
    write_deltalake(
        path,
        pa.table({"id": list(range(40)), "amount": list(range(40))}),
        configuration={"delta.enableDeletionVectors": "true"},
        writer_properties=WriterProperties(max_row_group_size=7),
    )
    install_deletion_vector(path, [3, 4, 7, 11, 18, 29], storage=storage)
    # An update rewrites a DV-backed file; old file and inline DV must remain readable.
    handle, format = open_format(tmp_path, context)
    tasks = format.plan(ScanRequest(format.schema(context).arrow_schema, budget), context)
    assert len(tasks) == min(budget, 6)
    DeltaTable(path).update(updates={"amount": "999"}, predicate="id = 2")
    assert read_parallel(handle, tasks, context) == [
        {"id": i, "amount": i} for i in range(40) if i not in (3, 4, 7, 11, 18, 29)
    ]
    latest_handle, latest = open_format(tmp_path, context)
    latest_tasks = latest.plan(ScanRequest(latest.schema(context).arrow_schema, budget), context)
    assert read_parallel(latest_handle, latest_tasks, context) == [
        {"id": i, "amount": 999 if i == 2 else i}
        for i in range(40)
        if i not in (3, 4, 7, 11, 18, 29)
    ]


def test_partition_values_nested_projection_and_schema_evolution(tmp_path, context):
    path = tmp_path / "sales"
    write_deltalake(
        path,
        pa.table({"id": [1, 2], "region": ["east", None], "profile": [{"name": "a"}, None]}),
        partition_by=["region"],
    )
    write_deltalake(
        path,
        pa.table({"id": [3], "region": ["west"], "profile": [{"name": "b"}], "new": ["value"]}),
        partition_by=["region"],
        mode="append",
        schema_mode="merge",
    )
    handle, format = open_format(tmp_path, context)
    schema = format.schema(context).arrow_schema
    projection = pa.schema(
        [schema.field(n) for n in ("new", "profile", "region", "id")], metadata=schema.metadata
    )
    tasks = format.plan(ScanRequest(projection, 10), context)
    assert read_parallel(handle, tasks, context) == [
        {"new": None, "profile": {"name": "a"}, "region": "east", "id": 1},
        {"new": None, "profile": None, "region": None, "id": 2},
        {"new": "value", "profile": {"name": "b"}, "region": "west", "id": 3},
    ]


def test_empty_snapshot_returns_no_tasks(tmp_path, context):
    write_deltalake(tmp_path / "sales", pa.table({"id": pa.array([], type=pa.int64())}))
    _, format = open_format(tmp_path, context)
    assert format.plan(ScanRequest(format.schema(context).arrow_schema, 3), context) == []


def test_replaced_table_cannot_reinterpret_pinned_handle(tmp_path, context):
    write_deltalake(tmp_path / "sales", pa.table({"id": [1]}))
    handle, _ = open_format(tmp_path, context)
    shutil.rmtree(tmp_path / "sales")
    write_deltalake(tmp_path / "sales", pa.table({"id": [999]}))
    with pytest.raises(ValueError, match="identity"):
        delta_format_factory(handle, context)


def test_invalid_projection_and_foreign_tasks_fail_closed(tmp_path, context):
    write_deltalake(tmp_path / "sales", pa.table({"id": [1]}))
    _, format = open_format(tmp_path, context)
    with pytest.raises(ValueError, match="projection"):
        format.plan(ScanRequest(pa.schema([pa.field("id", pa.string())]), 1), context)
    task = format.plan(ScanRequest(format.schema(context).arrow_schema, 1), context)[0].to_json()
    task["row_groups"][0]["path"] = "../outside.parquet"
    with pytest.raises(ValueError, match="snapshot"):
        format.execute(ScanTask(task), context)


def test_catalog_discovery_preserves_segments_and_rejects_symlinks(tmp_path, context):
    (tmp_path / "sales").mkdir()
    entries = [
        {"namespace": ["retail.eu"], "name": "sales", "path": "sales"},
        {"namespace": ["retail", "eu"], "name": "sales", "path": "sales"},
    ]
    catalog = catalog_for(tmp_path, context, entries)
    first = catalog.list_tables(context, limit=1)
    second = catalog.list_tables(context, continuation=first.continuation, limit=1)
    assert {first.entries[0], second.entries[0]} == {
        TableIdentifier(("retail.eu",), "sales"),
        TableIdentifier(("retail", "eu"), "sales"),
    }
    assert second.continuation is None
    (tmp_path / "link").symlink_to(tmp_path / "sales", target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        catalog_for(tmp_path, context, [{"namespace": [], "name": "bad", "path": "link"}])


def test_merge_on_read_update_keeps_old_file_and_hides_old_value(tmp_path, context):
    path = tmp_path / "sales"
    write_deltalake(
        path,
        pa.table({"id": list(range(40)), "amount": list(range(40))}),
        configuration={"delta.enableDeletionVectors": "true"},
        writer_properties=WriterProperties(max_row_group_size=7),
    )
    install_deletion_vector(path, [3, 4, 7, 11, 18, 29])
    old_handle, old = open_format(tmp_path, context)
    old_tasks = old.plan(ScanRequest(old.schema(context).arrow_schema, 4), context)
    write_deltalake(path, pa.table({"id": [2], "amount": [999]}), mode="append")
    install_deletion_vector(path, [2, 3, 4, 7, 11, 18, 29], storage="u")
    handle, format = open_format(tmp_path, context)
    tasks = format.plan(ScanRequest(format.schema(context).arrow_schema, 4), context)
    assert len(tasks) == 4
    assert read_parallel(handle, tasks, context) == [
        {"id": i, "amount": 999 if i == 2 else i}
        for i in range(40)
        if i not in (3, 4, 7, 11, 18, 29)
    ]
    assert read_parallel(old_handle, old_tasks, context) == [
        {"id": i, "amount": i} for i in range(40) if i not in (3, 4, 7, 11, 18, 29)
    ]


def test_deleted_row_groups_and_completely_deleted_snapshot_are_omitted(tmp_path, context):
    path = tmp_path / "sales"
    write_deltalake(
        path,
        pa.table({"id": list(range(8))}),
        configuration={"delta.enableDeletionVectors": "true"},
        writer_properties=WriterProperties(max_row_group_size=2),
    )
    install_deletion_vector(path, [0, 1, 2, 3])
    handle, format = open_format(tmp_path, context)
    tasks = format.plan(ScanRequest(format.schema(context).arrow_schema, 8), context)
    assert len(tasks) == 2
    assert read_parallel(handle, tasks, context) == [{"id": i} for i in range(4, 8)]
    install_deletion_vector(path, range(8))
    _, empty = open_format(tmp_path, context)
    assert empty.plan(ScanRequest(empty.schema(context).arrow_schema, 8), context) == []


def test_checkpoint_retains_deletion_vectors_and_escaped_partition_paths(tmp_path, context):
    path = tmp_path / "sales"
    write_deltalake(
        path,
        pa.table({"id": list(range(8)), "region": ["east # 10%"] * 8}),
        partition_by=["region"],
        configuration={"delta.enableDeletionVectors": "true"},
        writer_properties=WriterProperties(max_row_group_size=2),
    )
    install_deletion_vector(path, [1, 4], storage="p")
    DeltaTable(path).create_checkpoint()
    handle, format = open_format(tmp_path, context)
    tasks = format.plan(ScanRequest(format.schema(context).arrow_schema, 4), context)
    assert read_parallel(handle, tasks, context) == [
        {"id": i, "region": "east # 10%"} for i in range(8) if i not in (1, 4)
    ]


def test_vacuumed_snapshot_fails_without_reading_latest(tmp_path, context):
    path = tmp_path / "sales"
    write_deltalake(path, pa.table({"id": [1, 2]}))
    handle, format = open_format(tmp_path, context)
    tasks = format.plan(ScanRequest(format.schema(context).arrow_schema, 2), context)
    table = DeltaTable(path)
    table.update(updates={"id": "99"})
    # Avoid racing the zero-hour retention cutoff against the writer's clock.
    # Age this fixture's tombstone explicitly before running the real vacuum.
    commit = path / "_delta_log/00000000000000000001.json"
    actions = [json.loads(line) for line in commit.read_text().splitlines()]
    for action in actions:
        if "remove" in action:
            action["remove"]["deletionTimestamp"] = 0
    commit.write_text("\n".join(json.dumps(action) for action in actions) + "\n")
    DeltaTable(path).vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
    with pytest.raises(ValueError, match=r"root|snapshot"):
        read_parallel(handle, tasks, context)


def test_cancelled_stream_stops_and_can_be_closed_early(tmp_path, context):
    write_deltalake(tmp_path / "sales", pa.table({"id": list(range(20_000))}))
    _, format = open_format(tmp_path, context)
    task = format.plan(ScanRequest(format.schema(context).arrow_schema, 1), context)[0]
    cancelled = Event()
    active = replace(context, cancel_check=cancelled.is_set)
    _, batches = format.execute(task, active)
    assert next(batches).num_rows == 8192
    cancelled.set()
    with pytest.raises(InterruptedError):
        next(batches)
    batches.close()
    _, early = format.execute(task, context)
    assert next(early).num_rows == 8192
    early.close()
    format.close()


@pytest.mark.parametrize("storage", ["u", "p"])
def test_deletion_vector_paths_cannot_escape_table_root(tmp_path, context, storage):
    path = tmp_path / "sales"
    write_deltalake(
        path,
        pa.table({"id": list(range(8))}),
        configuration={"delta.enableDeletionVectors": "true"},
    )
    dv = install_deletion_vector(path, [1], storage=storage)
    commit = path / "_delta_log/00000000000000000001.json"
    rows = [json.loads(line) for line in commit.read_text().splitlines()]
    rows[1]["add"]["deletionVector"]["pathOrInlineDv"] = (
        (tmp_path / "outside.bin").as_uri() if storage == "p" else "../" + dv["pathOrInlineDv"]
    )
    commit.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    with pytest.raises(ValueError, match="root"):
        open_format(tmp_path, context)


def test_corrupt_deletion_vector_fails_planning(tmp_path, context):
    path = tmp_path / "sales"
    write_deltalake(
        path,
        pa.table({"id": list(range(8))}),
        configuration={"delta.enableDeletionVectors": "true"},
    )
    install_deletion_vector(path, [1], storage="u", corrupt=True)
    _, format = open_format(tmp_path, context)
    with pytest.raises(DeltaError, match=r"checksum|CRC"):
        format.plan(ScanRequest(format.schema(context).arrow_schema, 4), context)


def test_deletion_vector_budget_is_checked_before_materialization(tmp_path, context, monkeypatch):
    path = tmp_path / "sales"
    write_deltalake(
        path,
        pa.table({"id": list(range(8))}),
        configuration={"delta.enableDeletionVectors": "true"},
    )
    install_deletion_vector(path, [1])
    _, format = open_format(tmp_path, context)
    monkeypatch.setattr("dal_obscura_delta.format.MAX_DV_ROWS", 4)

    def forbidden_materialization(self):
        pytest.fail("kernel mask materialization must not run after budget rejection")

    monkeypatch.setattr(DeltaTable, "deletion_vectors", forbidden_materialization)
    with pytest.raises(ValueError, match="physical-row budget"):
        format.plan(ScanRequest(format.schema(context).arrow_schema, 4), context)


def test_unsupported_reader_features_are_rejected(tmp_path, context):
    path = tmp_path / "sales"
    write_deltalake(path, pa.table({"id": [1]}))
    commit = path / "_delta_log/00000000000000000000.json"
    rows = [json.loads(line) for line in commit.read_text().splitlines()]
    for row in rows:
        if "protocol" in row:
            row["protocol"] = {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["columnMapping"],
                "writerFeatures": ["columnMapping"],
            }
    commit.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    with pytest.raises(ValueError, match=r"Unsupported|snapshot"):
        open_format(tmp_path, context)


def test_plugin_passes_public_conformance_with_exact_task_coverage(tmp_path, context):
    from dal_obscura_plugin_conformance import run_catalog_checks, run_format_checks

    path = tmp_path / "sales"
    write_deltalake(
        path,
        pa.table({"id": list(range(10))}),
        configuration={"delta.enableDeletionVectors": "true"},
        writer_properties=WriterProperties(max_row_group_size=2),
    )
    install_deletion_vector(path, [1, 3])
    catalog = catalog_for(tmp_path, context)
    catalog_result = run_catalog_checks(
        catalog, context, expected_identifiers=[TableIdentifier(("retail",), "sales")]
    )
    _, format = open_format(tmp_path, context)
    result = run_format_checks(
        format,
        ScanRequest(format.schema(context).arrow_schema, 10),
        context,
        required_capabilities=["splittable_scan", "delete_files", "snapshot_reads"],
        expected_task_ids=[str(i) for i in range(5)],
        task_identity=lambda t: str(t.to_json()["row_groups"][0]["row_group"]),
    )
    assert catalog_result.failures == catalog_result.skips == []
    assert result.failures == result.skips == []
