"""Create 101 physical S3 tables and explicit MoR deletes/replacement updates."""

import hashlib
import json
import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dal_obscura_plugin_conformance.iceberg_fixtures import append_parquet
from dal_obscura_plugin_conformance.native_delta import delete_delta_rows
from dal_obscura_plugin_conformance.native_iceberg import write_deletes
from deltalake import WriterProperties, write_deltalake
from fixture import (
    BUCKET,
    LATER_ID,
    PARTITIONS,
    ROWS,
    ROWS_PER_PARTITION,
    SCHEMA,
    SMOKE,
    TARGET,
    row,
)
from pyiceberg.catalog import load_catalog
from pyiceberg.manifest import ManifestContent


def iceberg_catalog():
    return load_catalog(
        "scenario",
        type="sql",
        uri=os.environ["ICEBERG_URI"],
        warehouse=f"s3://{BUCKET}/iceberg",
        **{"s3.endpoint": os.environ["AWS_ENDPOINT_URL"], "s3.region": "us-east-1"},
    )


def append_later(mode):
    value = pa.Table.from_pylist([row(LATER_ID)], schema=SCHEMA)
    if mode == "delta":
        write_deltalake(
            f"s3://{BUCKET}/delta/customers",
            value,
            mode="append",
            partition_by=["partition"],
            storage_options={
                "AWS_ENDPOINT_URL": os.environ["AWS_ENDPOINT_URL"],
                "AWS_ALLOW_HTTP": "true",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            },
        )
    else:
        table = iceberg_catalog().load_table(TARGET)
        append_parquet(table, value)


def seed_delta(client):
    entries = []
    with TemporaryDirectory() as directory:
        root = Path(directory)
        target = root / "customers"
        for partition in range(PARTITIONS):
            write_deltalake(
                target,
                pa.RecordBatchReader.from_batches(SCHEMA, batches(partition)),
                mode="error" if partition == 0 else "append",
                partition_by=["partition"],
                configuration={"delta.enableDeletionVectors": "true"} if partition == 0 else None,
                writer_properties=WriterProperties(max_row_group_size=16384),
            )
            print(
                f"Delta partition {partition}: {ROWS_PER_PARTITION:,} physical rows written",
                flush=True,
            )
        commit = target / "_delta_log/00000000000000000000.json"
        action = next(
            json.loads(line)["add"]
            for line in commit.read_text().splitlines()
            if "add" in json.loads(line)
        )
        original = target / action["path"]
        with pq.ParquetFile(original) as parquet:
            ids = next(parquet.iter_batches(batch_size=3, columns=["id"])).column(0).to_pylist()
        updated_id, deleted_id = ids[1:3]
        digest = hashlib.sha256(original.read_bytes()).hexdigest()
        delete_delta_rows(target, [updated_id, deleted_id])
        write_deltalake(
            target,
            pa.Table.from_pylist([row(updated_id, updated=True)], schema=SCHEMA),
            mode="append",
            partition_by=["partition"],
        )
        assert hashlib.sha256(original.read_bytes()).hexdigest() == digest
        for index in range(100):
            name = f"other_{index:03d}"
            write_deltalake(root / name, pa.table({"id": [index], "sentinel": ["UNRELATED_TABLE"]}))
            entries.append({"namespace": ["retail"], "name": name, "path": name})
        for path in root.rglob("*"):
            if path.is_file():
                client.upload_file(str(path), BUCKET, "delta/" + path.relative_to(root).as_posix())
        entries.append({"namespace": ["retail"], "name": "customers", "path": "customers"})
        client.put_object(Bucket=BUCKET, Key="delta/tables.json", Body=json.dumps(entries).encode())
        return {
            "original_data_sha256": digest,
            "unchanged_after_mor_update": True,
            "physical_base_rows": ROWS,
            "dv_deleted_positions": [1, 2],
            "replacement_id": updated_id,
            "deleted_id": deleted_id,
            "registry_entries": len(entries),
        }


def seed_iceberg():
    catalog = iceberg_catalog()
    try:
        catalog.create_namespace("retail")
        table = catalog.create_table(
            TARGET,
            schema=SCHEMA,
            properties={
                "format-version": "2",
                "write.parquet.row-group-limit": "16384",
                "write.target-file-size-bytes": "67108864",
            },
        )
        with table.update_spec() as update:
            update.add_identity("partition")
        for partition in range(PARTITIONS):
            source = pa.Table.from_batches(batches(partition), schema=SCHEMA).cast(
                table.schema().as_arrow()
            )
            append_parquet(table, source, row_group_size=16384)
            del source
            print(
                f"Iceberg partition {partition}: {ROWS_PER_PARTITION:,} physical rows written",
                flush=True,
            )
        base = [
            (task.file.file_path, task.file.file_size_in_bytes)
            for task in table.scan().plan_files()
        ]
        write_deletes(table, kind="position", ids=[2], partition=("0",))
        write_deletes(table, kind="equality", ids=[1], partition=("0",))
        append_parquet(table, pa.Table.from_pylist([row(1, updated=True)], schema=SCHEMA))
        # Metadata/deletes changed; the original physical data file remains active.
        assert set(base).issubset(
            {
                (entry.data_file.file_path, entry.data_file.file_size_in_bytes)
                for manifest in table.current_snapshot().manifests(table.io)
                if manifest.content == ManifestContent.DATA
                for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True)
            }
        )
        for index in range(100):
            other = catalog.create_table(
                f"retail.other_{index:03d}", schema=pa.schema([pa.field("id", pa.int64())])
            )
            other.append(pa.table({"id": [index]}).cast(other.schema().as_arrow()))
        assert len(catalog.list_tables("retail")) == 101
        return {
            "physical_base_rows": ROWS,
            "position_deleted_id": 2,
            "equality_deleted_id": 1,
            "replacement_id": 1,
            "deleted_id": 2,
            "base_files_preserved": True,
            "snapshot": str(table.metadata.current_snapshot_id),
            "catalog_tables": 101,
        }
    finally:
        catalog.close()


def batches(partition):
    begin = partition * ROWS_PER_PARTITION
    end = begin + ROWS_PER_PARTITION
    for offset in range(begin, end, 8192):
        yield pa.RecordBatch.from_pylist(
            [row(index) for index in range(offset, min(end, offset + 8192))], schema=SCHEMA
        )


def main():
    mode = os.environ["SCENARIO"]
    started = time.perf_counter()
    client = boto3.client("s3", endpoint_url=os.environ["AWS_ENDPOINT_URL"])
    client.create_bucket(Bucket=BUCKET)
    detail = seed_delta(client) if mode == "delta" else seed_iceberg()
    result = {
        "scenario": mode,
        "rows": ROWS,
        "rows_per_partition": ROWS_PER_PARTITION,
        "partitions": PARTITIONS,
        "scale_requirement_met": not SMOKE,
        "table_count": 101,
        "seed_seconds": time.perf_counter() - started,
        "mor": detail,
    }
    Path("/state/fixture.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
