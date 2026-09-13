from __future__ import annotations

import base64
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dal_obscura_manifest_parquet.catalog import ManifestCatalog, _schema_identities
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
    identities = dict(
        cast(tuple[tuple[str, str], ...], handle.metadata["schema_identities"])
    )
    assert identities["id"] == "id"
    assert identities["profile.email"].startswith("synthetic:")


def test_manifest_catalog_exposes_namespace_and_config_lifecycle(tmp_path):
    root, manifest, _table = _write_fixture(tmp_path)
    catalog = ManifestCatalog(
        CatalogConfig(
            plugin_id="manifest",
            instance_id="fixture",
            revision=1,
            options={"root": str(root), "manifest_path": str(manifest)},
        ),
        _context(),
    )
    catalog.validate_config(_context())
    assert catalog.list_namespaces(_context()) == (("default",),)
    assert catalog.list_namespaces(_context(), namespace=("default",)) == (("default",),)


def test_manifest_catalog_paginates_with_string_continuation_tokens(tmp_path):
    root, manifest, _table = _write_fixture(tmp_path)
    payload = json.loads(manifest.read_text())
    payload["tables"]["default.orders"] = dict(payload["tables"]["default.users"])
    manifest.write_text(json.dumps(payload))

    catalog = ManifestCatalog(
        CatalogConfig(
            plugin_id="manifest",
            instance_id="fixture",
            revision=1,
            options={"root": str(root), "manifest_path": str(manifest)},
        ),
        _context(),
    )
    first = catalog.list_tables(_context(), limit=1)
    assert first.continuation == "default.orders"
    second = catalog.list_tables(_context(), continuation=first.continuation, limit=1)
    assert second.entries == (TableIdentifier(namespace=("default",), name="users"),)
    assert second.continuation is None


def test_manifest_catalog_accepts_structured_dotted_table_names(tmp_path):
    root, manifest, _table = _write_fixture(tmp_path)
    payload = json.loads(manifest.read_text())
    entry = payload["tables"].pop("default.users")
    entry["namespace"] = ["default"]
    entry["name"] = "users.with.dot"
    payload["tables"]["opaque-key"] = entry
    manifest.write_text(json.dumps(payload))

    catalog = ManifestCatalog(
        CatalogConfig(
            plugin_id="manifest",
            instance_id="fixture",
            revision=1,
            options={"root": str(root), "manifest_path": str(manifest)},
        ),
        _context(),
    )
    page = catalog.list_tables(_context(), limit=1)
    assert page.entries == (
        TableIdentifier(namespace=("default",), name="users.with.dot"),
    )


def test_manifest_identity_paths_use_core_collection_markers(tmp_path):
    schema = pa.schema(
        [
            pa.field("tags", pa.large_list(pa.field("item", pa.string()))),
            pa.field("attributes", pa.map_(pa.string(), pa.int64())),
            pa.field("fixed", pa.list_(pa.field("item", pa.bool_()), 2)),
        ]
    )
    identities = dict(_schema_identities(schema, ("tags", "attributes", "fixed")))
    assert "tags.$element" in identities
    assert "attributes.$key" in identities
    assert "attributes.$value" in identities
    assert "fixed.$element" in identities


def test_parquet_format_rejects_forged_schema_identity_metadata(tmp_path):
    root, manifest, _table = _write_fixture(tmp_path)
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
    handle = catalog.resolve_table(TableIdentifier(namespace=("default",), name="users"), context)
    metadata = dict(handle.metadata)
    metadata["schema_identities"] = (("id", "id"),)
    forged = replace(handle, metadata=metadata)
    with pytest.raises(ValueError, match="schema identities"):
        ParquetDatasetFormat(forged, context)


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


def test_manifest_rejects_symlinked_member(tmp_path):
    root, manifest, _table = _write_fixture(tmp_path)
    link = root / "linked.parquet"
    try:
        link.symlink_to(root / "part-0.parquet")
    except OSError:
        pytest.skip("symlinks are unavailable on this runner")
    payload = json.loads(manifest.read_text())
    payload["tables"]["default.users"]["files"] = ["linked.parquet"]
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="symlink"):
        ManifestCatalog(
            CatalogConfig(
                plugin_id="manifest",
                instance_id="fixture",
                revision=1,
                options={"root": str(root), "manifest_path": str(manifest)},
            ),
            _context(),
        )


def test_manifest_and_format_reject_expired_context(tmp_path):
    root, manifest, _table = _write_fixture(tmp_path)
    expired = ExecutionContext(
        deadline=datetime.now(timezone.utc) - timedelta(seconds=1),
        correlation_id="expired",
    )
    with pytest.raises(TimeoutError, match="deadline"):
        ManifestCatalog(
            CatalogConfig(
                plugin_id="manifest",
                instance_id="fixture",
                revision=1,
                options={"root": str(root), "manifest_path": str(manifest)},
            ),
            expired,
        )


def test_parquet_format_accepts_wildcard_projection(tmp_path):
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
    handle = catalog.resolve_table(
        TableIdentifier(namespace=("default",), name="users"),
        context,
    )
    plugin = ParquetDatasetFormat(handle, context)
    schema = plugin.schema(handle, context)
    tasks = plugin.plan(handle, schema, context, projection=["*"], row_filter=None, max_tasks=4)
    assert tasks
    output_schema, batches = plugin.execute(tasks[0], context)
    assert output_schema == table.schema
    assert pa.Table.from_batches(batches).num_rows == 1


def test_parquet_format_rejects_member_schema_drift(tmp_path):
    root, manifest, table = _write_fixture(tmp_path)
    drifted = root / "drifted.parquet"
    pq.write_table(
        pa.table(
            {"id": pa.array([4], type=pa.int32()), "profile": table["profile"].slice(0, 1)}
        ),
        drifted,
    )
    payload = json.loads(manifest.read_text())
    payload["tables"]["default.users"]["files"].append("drifted.parquet")
    manifest.write_text(json.dumps(payload))
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
    handle = catalog.resolve_table(TableIdentifier(namespace=("default",), name="users"), context)
    plugin = ParquetDatasetFormat(handle, context)
    with pytest.raises(ValueError, match="schema"):
        plugin.plan(
            handle,
            plugin.schema(handle, context),
            context,
            projection=("*",),
            row_filter=None,
            max_tasks=8,
        )


def test_parquet_format_rejects_corrupt_member_during_plan(tmp_path):
    root, manifest, _table = _write_fixture(tmp_path)
    corrupt = root / "corrupt.parquet"
    corrupt.write_bytes(b"not parquet")
    payload = json.loads(manifest.read_text())
    payload["tables"]["default.users"]["files"] = ["corrupt.parquet"]
    manifest.write_text(json.dumps(payload))
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
    handle = catalog.resolve_table(TableIdentifier(namespace=("default",), name="users"), context)
    plugin = ParquetDatasetFormat(handle, context)
    with pytest.raises((ValueError, OSError)):
        plugin.plan(
            handle,
            plugin.schema(handle, context),
            context,
            projection=("*",),
            row_filter=None,
            max_tasks=8,
        )
