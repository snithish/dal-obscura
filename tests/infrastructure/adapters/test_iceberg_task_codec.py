import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from pyiceberg.expressions import And, EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table import FileScanTask
from pyiceberg.typedef import Record

from dal_obscura.sources.iceberg_tasks import decode_scan_task, encode_scan_task


def test_native_task_round_trip_preserves_delete_files_metrics_and_partition_types():
    data = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path="s3://warehouse/data.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(
            None,
            7,
            "EU",
            b"\x00\xff",
            Decimal("12.340"),
            UUID("12345678-1234-5678-1234-567812345678"),
            date(2026, 10, 2),
            time(12, 34, 56),
            datetime(2026, 10, 2, tzinfo=timezone.utc),
        ),
        record_count=42,
        file_size_in_bytes=2048,
        column_sizes={1: 128},
        value_counts={1: 42},
        null_value_counts={1: 0},
        nan_value_counts={2: 3},
        lower_bounds={1: b"\x00\x00"},
        upper_bounds={1: b"\xff\xff"},
        key_metadata=b"encryption-key",
        split_offsets=[4, 128],
        equality_ids=[1],
        sort_order_id=2,
    )
    data.spec_id = 3
    delete = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path="s3://warehouse/delete.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=2,
        file_size_in_bytes=64,
    )
    delete.spec_id = 3
    task = FileScanTask(
        data,
        {delete},
        And(
            EqualTo(term=Reference(name="id"), value=literal(7)),
            EqualTo(term=Reference(name="region"), value=literal("EU")),
        ),
    )

    encoded = encode_scan_task(task)
    decoded = decode_scan_task(json.loads(json.dumps(encoded)))

    assert decoded.file.partition == data.partition
    assert decoded.file.spec_id == 3
    assert decoded.file.content == DataFileContent.DATA
    assert decoded.file.file_format == FileFormat.PARQUET
    assert decoded.file.column_sizes == {1: 128}
    assert decoded.file.value_counts == {1: 42}
    assert decoded.file.null_value_counts == {1: 0}
    assert decoded.file.nan_value_counts == {2: 3}
    assert decoded.file.lower_bounds == {1: b"\x00\x00"}
    assert decoded.file.upper_bounds == {1: b"\xff\xff"}
    assert decoded.file.key_metadata == b"encryption-key"
    assert decoded.file.split_offsets == [4, 128]
    assert decoded.file.equality_ids == [1]
    assert decoded.file.sort_order_id == 2
    assert [(file.file_path, file.content, file.spec_id) for file in decoded.delete_files] == [
        ("s3://warehouse/delete.parquet", DataFileContent.POSITION_DELETES, 3)
    ]
    assert decoded.residual == task.residual
    assert encode_scan_task(decoded) == encoded


@pytest.mark.parametrize("payload", [None, {}, {"version": 0}, {"version": 1, "module": "os"}])
def test_native_task_rejects_unknown_or_incomplete_envelopes(payload):
    with pytest.raises(ValueError, match="Invalid Iceberg task"):
        decode_scan_task(payload)


@pytest.mark.parametrize("values", [[1, 2, 3], [Decimal("1.20"), Decimal("2.30")]])
def test_native_task_codec_preserves_set_residuals(values):
    from pyiceberg.expressions import In, Not

    from tests.infrastructure.adapters.test_scan_envelopes import envelope

    _, task = envelope()
    file = decode_scan_task(task.partition.task["tasks"][0]).file
    residual = Not(In(term=Reference(name="value"), values={literal(value) for value in values}))
    decoded = decode_scan_task(
        json.loads(json.dumps(encode_scan_task(FileScanTask(file, residual=residual))))
    )
    assert decoded.residual == residual
