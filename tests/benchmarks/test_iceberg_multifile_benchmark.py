from __future__ import annotations

import pickle
from dataclasses import dataclass

import pyarrow as pa
import pytest

from dal_obscura.infrastructure.table_formats.iceberg import (
    IcebergInputPartition,
    IcebergTableFormat,
)


@dataclass(frozen=True, kw_only=True)
class _FakeProjectedSchema:
    schema: pa.Schema

    def as_arrow(self) -> pa.Schema:
        return self.schema

    def select(self, *columns: str) -> _FakeProjectedSchema:
        return _FakeProjectedSchema(
            schema=pa.schema([self.schema.field(column) for column in columns])
        )


@dataclass(frozen=True, kw_only=True)
class _FakeTable:
    schema_value: pa.Schema
    metadata: object
    io: object

    def schema(self) -> _FakeProjectedSchema:
        return _FakeProjectedSchema(schema=self.schema_value)


class _BatchingArrowScan:
    def __init__(self, *, table_metadata, io, projected_schema, row_filter) -> None:
        del table_metadata, io, row_filter
        self._schema = projected_schema.as_arrow()

    def to_record_batches(self, file_tasks):
        for index, _task in enumerate(file_tasks):
            yield pa.record_batch(
                [
                    pa.array([index], type=pa.int64()),
                    pa.array([f"region-{index % 8}"], type=pa.string()),
                ],
                schema=self._schema,
            )


@pytest.mark.benchmark(group="iceberg-multifile")
def test_benchmark_iceberg_multifile_scan_baseline(benchmark, monkeypatch):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    file_count = 512
    table_format = IcebergTableFormat(
        catalog_name="analytics",
        table_name="events",
        metadata_location="/tmp/metadata.json",
        io_options={},
    )
    monkeypatch.setattr(
        IcebergTableFormat,
        "_load_table",
        lambda self: _FakeTable(schema_value=schema, metadata=object(), io=object()),
    )
    monkeypatch.setattr(
        "dal_obscura.infrastructure.table_formats.iceberg.ArrowScan",
        _BatchingArrowScan,
    )

    partition = IcebergInputPartition(
        columns=["id", "region"],
        tasks=[pickle.dumps({"file": index}) for index in range(file_count)],
    )

    def run() -> pa.Table:
        output_schema, batches = table_format.execute(partition)
        return pa.Table.from_batches(list(batches), schema=output_schema)

    table = benchmark(run)

    benchmark.extra_info["scenario"] = "large-iceberg-multifile-scan"
    benchmark.extra_info["planned_files"] = file_count
    benchmark.extra_info["output_rows"] = file_count
    assert table.num_rows == file_count
