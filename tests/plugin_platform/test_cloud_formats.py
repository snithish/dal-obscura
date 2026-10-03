"""Cloud scans through SDK factories, including a fresh reader per parallel task."""

import base64
import json
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dal_obscura_delta.catalog import delta_catalog_factory
from dal_obscura_delta.format import delta_format_factory
from dal_obscura_manifest_parquet.catalog import manifest_factory
from dal_obscura_manifest_parquet.format import parquet_factory
from dal_obscura_plugin_api import (
    CatalogConfig,
    ExecutionContext,
    ScanRequest,
    TableHandle,
    TableIdentifier,
)
from deltalake import DeltaTable, QueryBuilder, WriterProperties, write_deltalake

from tests.support.delta import install_deletion_vector
from tests.support.plugin_scans import parallel_rows
from tests.support.s3 import s3_bucket, upload_directory

pytestmark = pytest.mark.socket


@pytest.mark.parametrize("mode", ["copy-on-write", "merge-on-read-inline", "merge-on-read-file"])
def test_delta_s3_parallel_reads_match_native_pinned_snapshot(tmp_path, monkeypatch, mode):
    path = tmp_path / "sales"
    data = pa.table({"id": list(range(30)), "amount": list(range(30))})
    write_deltalake(
        path,
        data,
        configuration={"delta.enableDeletionVectors": "true"} if mode.startswith("merge") else None,
        writer_properties=WriterProperties(max_row_group_size=4),
    )
    if mode.startswith("merge"):
        install_deletion_vector(
            path, [1, 2, 3, 4, 11, 21], storage="u" if mode.endswith("file") else "i"
        )
        write_deltalake(
            path,
            pa.table({"id": [2], "amount": [102]}),
            mode="append",
            writer_properties=WriterProperties(max_row_group_size=1),
        )
    else:
        table = DeltaTable(path)
        table.delete(
            "id IN (1, 3, 4, 11, 21)", writer_properties=WriterProperties(max_row_group_size=4)
        )
        table.update(
            updates={"amount": "102"},
            predicate="id = 2",
            writer_properties=WriterProperties(max_row_group_size=4),
        )
    expected = [
        {"id": i, "amount": 102 if i == 2 else i} for i in range(30) if i not in (1, 3, 4, 11, 21)
    ]
    context = ExecutionContext(datetime.now(timezone.utc) + timedelta(minutes=2), "s3-test")
    with s3_bucket(monkeypatch) as (client, bucket):
        upload_directory(client, bucket, path, "root/sales")
        client.put_object(
            Bucket=bucket,
            Key="root/tables.json",
            Body=json.dumps([{"namespace": ["retail"], "name": "sales", "path": "sales"}]).encode(),
        )
        native = DeltaTable(f"s3://{bucket}/root/sales")
        native_rows = pa.table(
            QueryBuilder().register("sales", native).execute("SELECT * FROM sales").read_all()
        ).to_pylist()
        assert sorted(native_rows, key=lambda r: r["id"]) == expected
        catalog = delta_catalog_factory(
            CatalogConfig(
                "delta.directory",
                "s3-fixture",
                1,
                {"root": f"s3://{bucket}/root", "tables_path": "tables.json"},
            ),
            context,
        )
        try:
            handle = catalog.resolve_table(TableIdentifier(("retail",), "sales"), context)
        finally:
            catalog.close()
        plugin = delta_format_factory(handle, context)
        try:
            schema = plugin.schema(context).arrow_schema
            tasks = plugin.plan(ScanRequest(schema, 4), context)
        finally:
            plugin.close()
        assert len(tasks) == 4
        # Latest commit cannot affect workers that reopen the captured version.
        write_deltalake(
            f"s3://{bucket}/root/sales", pa.table({"id": [999], "amount": [999]}), mode="append"
        )
        assert (
            sorted(
                parallel_rows(delta_format_factory, handle, tasks, context), key=lambda r: r["id"]
            )
            == expected
        )
        assert "dal-test-secret" not in json.dumps(handle.to_json())


def test_manifest_s3_parallel_nested_projection(tmp_path, monkeypatch):
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("profile", pa.struct([pa.field("name", pa.string())])),
        ]
    )
    rows = [{"id": i, "profile": {"name": f"user-{i}"}} for i in range(12)]
    data = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(data, tmp_path / "part # %.parquet", row_group_size=2)
    manifest = {
        "revision": "pinned",
        "tables": {
            "users": {
                "namespace": ["retail"],
                "name": "users",
                "files": ["part # %.parquet"],
                "schema_ipc": base64.b64encode(schema.serialize()).decode(),
            }
        },
    }
    context = ExecutionContext(datetime.now(timezone.utc) + timedelta(minutes=2), "manifest-s3")
    with s3_bucket(monkeypatch) as (client, bucket):
        upload_directory(client, bucket, tmp_path, "root")
        client.put_object(
            Bucket=bucket, Key="root/manifest.json", Body=json.dumps(manifest).encode()
        )
        catalog = manifest_factory(
            CatalogConfig(
                "manifest",
                "s3-fixture",
                1,
                {"root": f"s3://{bucket}/root", "manifest_path": "manifest.json"},
            ),
            context,
        )
        handle = catalog.resolve_table(TableIdentifier(("retail",), "users"), context)
        catalog.close()
        plugin = parquet_factory(handle, context)
        tasks = plugin.plan(ScanRequest(schema, 4), context)
        plugin.close()
        assert len(tasks) == 4
        assert (
            sorted(parallel_rows(parquet_factory, handle, tasks, context), key=lambda r: r["id"])
            == rows
        )


@pytest.mark.parametrize("mode", ["copy-on-write", "position", "equality"])
def test_iceberg_s3_reads_cow_and_mor_snapshots(tmp_path, monkeypatch, mode):
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    from dal_obscura.sources.iceberg_plugin import IcebergFormatPlugin
    from tests.support.iceberg import install_delete_file

    context = ExecutionContext(datetime.now(timezone.utc) + timedelta(minutes=2), "iceberg-s3")
    with s3_bucket(monkeypatch) as (client, bucket):
        endpoint = client.meta.endpoint_url
        catalog = load_catalog(
            "s3",
            type="sql",
            uri=f"sqlite:///{tmp_path}/catalog.db",
            warehouse=f"s3://{bucket}/warehouse",
            **{"s3.endpoint": endpoint, "s3.region": "us-east-1"},
        )
        catalog.create_namespace("retail")
        table = catalog.create_table(
            "retail.sales",
            Schema(
                NestedField(1, "id", LongType(), required=True),
                NestedField(2, "email", StringType()),
            ),
            properties={"format-version": "2"},
        )
        for ids in ([0, 1, 2], [3, 4, 5]):
            table.append(
                pa.table(
                    {"id": ids, "email": [f"user{i}@example.com" for i in ids]},
                    schema=table.schema().as_arrow(),
                )
            )
        if mode == "copy-on-write":
            table.delete("id == 1")
        else:
            install_delete_file(table, kind=mode, ids=[1])
        table.append(
            pa.table(
                {"id": [1], "email": ["updated@example.com"]}, schema=table.schema().as_arrow()
            )
        )
        handle = TableHandle(
            "iceberg.sql",
            "s3",
            1,
            TableIdentifier(("retail",), "sales"),
            "iceberg",
            1,
            str(table.metadata.current_snapshot_id),
            {"metadata_location": table.metadata_location},
        )
        plugin = IcebergFormatPlugin(handle, context)
        schema = plugin.schema(context).arrow_schema
        tasks = plugin.plan(ScanRequest(schema, 3), context)
        plugin.close()
        assert len(tasks) == (1 if mode == "equality" else 3)
        assert sorted(
            parallel_rows(IcebergFormatPlugin, handle, tasks, context), key=lambda r: r["id"]
        ) == [
            {"id": i, "email": "updated@example.com" if i == 1 else f"user{i}@example.com"}
            for i in range(6)
        ]


def test_s3_storage_admission_contains_decoded_members(monkeypatch):
    from dal_obscura_plugin_api.storage import StorageRoot

    with s3_bucket(monkeypatch) as (client, bucket):
        client.put_object(Bucket=bucket, Key="root/a # %.json", Body=b"12345")
        storage = StorageRoot(f"s3://{bucket}/root")
        member = storage.member("a%20%23%20%25.json", encoded=True)
        assert storage.location(member) == f"s3://{bucket}/root/a%20%23%20%25.json"
        assert storage.read(member, limit=5) == b"12345"
        with pytest.raises(ValueError, match="byte budget"):
            storage.read(member, limit=4)
        for path in (
            "../outside",
            "%2e%2e/outside",
            f"s3://{bucket}/root-other/file",
            f"s3://{bucket}/root/file?token=x",
            "s3://user:secret@other/file",
        ):
            with pytest.raises(ValueError):
                storage.member(path, encoded=True, exists=False)
