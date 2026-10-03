from __future__ import annotations

from contextlib import suppress
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


def _generated_column_values(field: pa.Field, batch_values: list[int]) -> list[Any]:
    if pa.types.is_integer(field.type) or field.name.endswith("_id") or field.name == "id":
        return batch_values
    if field.name == "email":
        return [f"user{i}@example.com" for i in batch_values]
    if field.name == "region":
        return ["us" if i % 2 == 0 else "eu" for i in batch_values]
    if pa.types.is_string(field.type):
        return [f"{field.name}-{i}" for i in batch_values]
    return batch_values


def _generated_batch_table(
    batch_values: list[int],
    *,
    schema: pa.Schema,
) -> pa.Table:
    return pa.table(
        {field.name: _generated_column_values(field, batch_values) for field in schema},
        schema=schema,
    )


def create_iceberg_table(
    tmp_path: Path,
    catalog_name: str,
    warehouse_name: str,
    values: list[int] | None = None,
    *,
    identifier: str = "default.users",
    append_batches: list[list[int]] | None = None,
    arrow_schema: pa.Schema | None = None,
    append_tables: list[pa.Table] | None = None,
    table_properties: dict[str, object] | None = None,
    partition_spec: PartitionSpec | None = None,
) -> str:
    warehouse = tmp_path / warehouse_name
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=f"sqlite:///{tmp_path / f'{catalog_name}.db'}",
        warehouse=str(warehouse),
    )
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="email", field_type=StringType(), required=False),
        NestedField(field_id=3, name="region", field_type=StringType(), required=False),
    )
    table_schema: Schema | pa.Schema = arrow_schema if arrow_schema is not None else schema
    namespace = ".".join(identifier.split(".")[:-1])
    with suppress(Exception):
        catalog.create_namespace(namespace)
    properties = {"format-version": "2", **(table_properties or {})}
    if partition_spec is None:
        table = catalog.create_table(
            identifier=identifier,
            schema=table_schema,
            properties=properties,
        )
    else:
        table = catalog.create_table(
            identifier=identifier,
            schema=table_schema,
            partition_spec=partition_spec,
            properties=properties,
        )
    default_arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("email", pa.string(), nullable=True),
            pa.field("region", pa.string(), nullable=True),
        ]
    )
    if append_tables is not None:
        for append_table in append_tables:
            table.append(append_table)
    else:
        append_schema = arrow_schema if arrow_schema is not None else default_arrow_schema
        batches = append_batches or [values or []]
        for batch_values in batches:
            table.append(_generated_batch_table(batch_values, schema=append_schema))
    return identifier


def iceberg_sql_catalog_options(
    tmp_path: Path,
    catalog_name: str,
    warehouse_name: str,
) -> dict[str, str]:
    return {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path / f'{catalog_name}.db'}",
        "warehouse": str(tmp_path / warehouse_name),
    }


def install_delete_file(
    table,
    *,
    kind,
    ids=(),
    delete_path=None,
    equality_columns=("id",),
    equality_records=None,
    partition=(),
):
    """Write a v2 MoR fixture with upstream Avro writers, never production readers.

    PyIceberg has no public MoR writer. This fixture supplies explicit delete
    records and commits a snapshot; Apache Iceberg independently interprets its semantics.
    """
    import uuid

    import pyarrow.parquet as pq
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
        ManifestContent,
        ManifestEntry,
        ManifestEntryStatus,
        ManifestWriterV2,
        write_manifest_list,
    )
    from pyiceberg.table.snapshots import Operation, Snapshot, Summary
    from pyiceberg.table.update import AddSnapshotUpdate, SetSnapshotRefUpdate
    from pyiceberg.typedef import Record

    snapshot = table.current_snapshot()
    snapshot_id = uuid.uuid4().int & ((1 << 63) - 1)
    sequence = table.metadata.last_sequence_number + 1
    stem = table.metadata.location + "/metadata/delete-" + uuid.uuid4().hex
    delete_path = delete_path or stem + ".parquet"
    if kind == "equality":
        schema = pa.schema([table.schema().as_arrow().field(name) for name in equality_columns])
        records = equality_records if equality_records is not None else [{"id": i} for i in ids]
        content = DataFileContent.EQUALITY_DELETES
    else:
        schema = pa.schema(
            [
                pa.field("file_path", pa.string(), metadata={b"PARQUET:field_id": b"2147483546"}),
                pa.field("pos", pa.int64(), metadata={b"PARQUET:field_id": b"2147483545"}),
            ]
        )
        records = []
        for task in table.scan().plan_files():
            with table.io.new_input(task.file.file_path).open() as stream:
                rows = pq.read_table(stream).to_pylist()
            records += [
                {"file_path": task.file.file_path, "pos": position}
                for position, row in enumerate(rows)
                if row["id"] in ids
            ]
        content = DataFileContent.POSITION_DELETES
    with table.io.new_output(delete_path).create() as output:
        pq.write_table(pa.Table.from_pylist(records, schema=schema), output)
    file = DataFile.from_args(
        content=content,
        file_path=delete_path,
        file_format=FileFormat.PARQUET,
        partition=Record(*partition),
        record_count=len(records),
        file_size_in_bytes=len(table.io.new_input(delete_path)),
        equality_ids=[table.schema().find_field(name).field_id for name in equality_columns]
        if kind == "equality"
        else None,
    )
    file.spec_id = table.spec().spec_id

    class DeleteManifestWriter(ManifestWriterV2):
        def content(self):
            return ManifestContent.DELETES

        @property
        def _meta(self):
            return {**super()._meta, "content": "deletes"}

    with DeleteManifestWriter(
        table.spec(), table.schema(), table.io.new_output(stem + ".avro"), snapshot_id, "null"
    ) as writer:
        writer.add(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                sequence_number=sequence,
                file_sequence_number=sequence,
                data_file=file,
            )
        )
    manifest = writer.to_manifest_file()
    manifest_list = stem + "-list.avro"
    with write_manifest_list(
        2, table.io.new_output(manifest_list), snapshot_id, snapshot.snapshot_id, sequence, "null"
    ) as writer:
        writer.add_manifests([*snapshot.manifests(table.io), manifest])
    updated = Snapshot.model_validate(
        {
            "snapshot-id": snapshot_id,
            "parent-snapshot-id": snapshot.snapshot_id,
            "sequence-number": sequence,
            "manifest-list": manifest_list,
            "summary": Summary(operation=Operation.OVERWRITE),
            "schema-id": table.schema().schema_id,
        }
    )
    with table.transaction() as transaction:
        transaction._apply(
            (
                AddSnapshotUpdate(snapshot=updated),
                SetSnapshotRefUpdate.model_validate(
                    {"ref-name": "main", "type": "branch", "snapshot-id": snapshot_id}
                ),
            )
        )
    return table


class NativeReaderStub:
    """Native batch stream fake with explicit failure and cleanup ownership."""

    def __init__(self, batch, *, fail=False):
        self.remaining = iter([batch, batch])
        self.closed = []
        self.fail = fail

    def start(self, files):
        return self

    def next(self, seconds):
        if self.fail:
            raise RuntimeError("native read failed")
        return next(self.remaining, None)

    def close(self):
        self.closed.append("reader")
        if self.fail:
            raise ValueError("reader cleanup failed")
