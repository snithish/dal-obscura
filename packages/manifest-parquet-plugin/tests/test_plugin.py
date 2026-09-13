from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from dal_obscura_manifest_parquet.catalog import ManifestCatalog
from dal_obscura_manifest_parquet.format import ParquetDatasetFormat
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext, TableIdentifier


def _context() -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        correlation_id="manifest-fixture",
    )


def _write_fixture(tmp_path):
    root = tmp_path / "dataset"
    root.mkdir()
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "profile": pa.array(
                [{"email": "a@example.com"}, {"email": "b@example.com"}, None],
                type=pa.struct([pa.field("email", pa.string())]),
            ),
        }
    )
    first = root / "part-0.parquet"
    second = root / "part-1.parquet"
    pq.write_table(table.slice(0, 2), first, row_group_size=1)
    pq.write_table(table.slice(2), second, row_group_size=1)
    schema_ipc = base64.b64encode(table.schema.serialize().to_pybytes()).decode("ascii")
    manifest = root / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "revision": "snapshot-1",
                "tables": {
                    "default.users": {
                        "files": ["part-0.parquet", "part-1.parquet"],
                        "schema_ipc": schema_ipc,
                        "field_ids": ["id", "profile"],
                    }
                },
            }
        )
    )
    return root, manifest, table


def test_manifest_catalog_and_parquet_format_split_nested_rows(tmp_path):
    root, manifest, table = _write_fixture(tmp_path)
    context = _context()
    catalog = ManifestCatalog(
        CatalogConfig(
            plugin_id="manifest",
            instance_id="fixture",
            revision=1,
            options={"root": str(root), "manifest_path": str(manifest)},
        ),
        context,
    )
    page = catalog.list_tables(context, limit=1)
    assert page.entries == (TableIdentifier(namespace=("default",), name="users"),)
    assert page.continuation is None
    handle = catalog.resolve_table(page.entries[0], context)
    format_plugin = ParquetDatasetFormat(handle, context)
    schema = format_plugin.schema(handle, context)
    assert schema.arrow_schema == table.schema
    tasks = format_plugin.plan(
        handle,
        schema,
        context,
        projection=("profile.email",),
        row_filter=None,
        max_tasks=4,
    )
    assert len(tasks) == 3
    output_schema, batches = format_plugin.execute(tasks[0], context)
    assert output_schema.names == ["profile"]
    assert pa.Table.from_batches(batches).to_pylist() == [{"profile": {"email": "a@example.com"}}]


def test_manifest_rejects_member_escape_and_schema_drift(tmp_path):
    root, manifest, _table = _write_fixture(tmp_path)
    payload = json.loads(manifest.read_text())
    payload["tables"]["default.users"]["files"] = ["../outside.parquet"]
    manifest.write_text(json.dumps(payload))
    try:
        ManifestCatalog(
            CatalogConfig(
                plugin_id="manifest",
                instance_id="fixture",
                revision=1,
                options={"root": str(root), "manifest_path": str(manifest)},
            ),
            _context(),
        )
    except ValueError as exc:
        assert "escapes" in str(exc)
    else:
        raise AssertionError("expected manifest member escape rejection")
