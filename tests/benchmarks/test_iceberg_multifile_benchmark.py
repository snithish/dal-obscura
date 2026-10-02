from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table import FileScanTask
from pyiceberg.typedef import Record

from dal_obscura.sources.iceberg import (
    IcebergInputPartition,
    IcebergTableFormat,
)
from dal_obscura.sources.iceberg_tasks import encode_scan_task
from tests.support.iceberg_schema import _FakeProjectedSchema

pytestmark = pytest.mark.heavy


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

    def _record_batches_from_scan_tasks_and_deletes(self, file_tasks, deletes):
        for task in file_tasks:
            index = task.file.record_count
            yield pa.record_batch(
                [
                    pa.array([index], type=pa.int64()),
                    pa.array([f"region-{index % 8}"], type=pa.string()),
                ],
                schema=self._schema,
            )


@pytest.mark.benchmark(group="iceberg-multifile")
def test_benchmark_iceberg_multifile_scan_baseline(benchmark, monkeypatch):
    # Measure dispatch overhead only; the large streaming probe uses real files.
    monkeypatch.setattr(
        "dal_obscura.sources.iceberg._read_all_delete_files",
        lambda io, tasks: {},
    )
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
        "dal_obscura.sources.iceberg.ArrowScan",
        _BatchingArrowScan,
    )

    encoded = []
    for index in range(file_count):
        file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=f"/tmp/{index}.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=index,
            file_size_in_bytes=100,
        )
        file.spec_id = 0
        encoded.append(encode_scan_task(FileScanTask(file)))
    partition = IcebergInputPartition(
        columns=["id", "region"],
        tasks=encoded,
    )

    def run() -> pa.Table:
        output_schema, batches = table_format.execute(partition)
        return pa.Table.from_batches(list(batches), schema=output_schema)

    table = benchmark(run)

    benchmark.extra_info["scenario"] = "simulated-iceberg-multifile-dispatch"
    benchmark.extra_info["planned_files"] = file_count
    benchmark.extra_info["output_rows"] = file_count
    assert table.num_rows == file_count
    assert table.column("id").to_pylist() == list(range(file_count))
