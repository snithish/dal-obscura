import json
import subprocess
import sys
import textwrap
from collections.abc import Generator
from typing import cast

import pyarrow as pa

import dal_obscura.infrastructure.adapters.duckdb_transform as duckdb_transform
from dal_obscura.domain.access_control.models import MaskRule
from dal_obscura.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)


def test_mask_select_list_basic():
    selection = DefaultMaskingAdapter().apply(
        ["id", "name"],
        {"name": MaskRule(type="redact", value="***")},
    )
    assert "name" in selection.masked_columns
    assert "'***'" in selection.select_list[1]


def test_nested_mask_expression():
    selection = DefaultMaskingAdapter().apply(
        ["user.address.zip"],
        {"user.address.zip": MaskRule(type="hash")},
    )
    assert "struct_update" in selection.select_list[0]


def test_default_mask_renders_literal():
    selection = DefaultMaskingAdapter().apply(
        ["status"],
        {"status": MaskRule(type="default", value="unknown")},
    )
    assert "'unknown'" in selection.select_list[0]


def test_duckdb_transform_uses_single_arrow_stream_query(monkeypatch):
    result_batch = pa.record_batch([pa.array([10, 20])], names=["id"])

    class FakeRelation:
        def __init__(self) -> None:
            self.query_calls: list[tuple[str, str]] = []
            self.batch_size: int | None = None

        def query(self, table_name: str, query: str):
            self.query_calls.append((table_name, query))
            return self

        def to_arrow_reader(self, batch_size: int):
            self.batch_size = batch_size
            return iter([result_batch])

    class FakeConnection:
        def __init__(self) -> None:
            self.relation = FakeRelation()
            self.from_arrow_calls: list[pa.RecordBatchReader] = []
            self.register_calls = 0
            self.unregister_calls = 0
            self.closed = 0

        def from_arrow(self, reader: pa.RecordBatchReader):
            self.from_arrow_calls.append(reader)
            return self.relation

        def register(self, *args, **kwargs):
            self.register_calls += 1

        def unregister(self, *args, **kwargs):
            self.unregister_calls += 1

        def close(self) -> None:
            self.closed += 1

    fake_connection = FakeConnection()
    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda: fake_connection)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    input_batches = [
        pa.record_batch([pa.array([1, 2])], names=["id"]),
        pa.record_batch([pa.array([3, 4])], names=["id"]),
    ]

    result = list(adapter.apply_filters_and_masks_stream(input_batches, ["id"], "id > 1", {}))

    assert len(result) == 1
    assert result[0].equals(result_batch)
    assert len(fake_connection.from_arrow_calls) == 1
    assert fake_connection.relation.query_calls == [
        ("input", 'SELECT id AS "id" FROM input WHERE id > 1')
    ]
    assert fake_connection.relation.batch_size == duckdb_transform._DUCKDB_ARROW_OUTPUT_BATCH_SIZE
    assert fake_connection.register_calls == 0
    assert fake_connection.unregister_calls == 0
    assert fake_connection.closed == 1


def test_duckdb_transform_closes_connection_when_consumer_stops_early(monkeypatch):
    result_batches = [
        pa.record_batch([pa.array([1, 2])], names=["id"]),
        pa.record_batch([pa.array([3, 4])], names=["id"]),
    ]

    class FakeRelation:
        def query(self, table_name: str, query: str):
            return self

        def to_arrow_reader(self, batch_size: int):
            return iter(result_batches)

    class FakeConnection:
        def __init__(self) -> None:
            self.closed = 0

        def from_arrow(self, reader: pa.RecordBatchReader):
            return FakeRelation()

        def close(self) -> None:
            self.closed += 1

    fake_connection = FakeConnection()
    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda: fake_connection)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    stream = adapter.apply_filters_and_masks_stream(
        [pa.record_batch([pa.array([1, 2, 3, 4])], names=["id"])],
        ["id"],
        None,
        {},
    )
    stream_iter = cast(Generator[pa.RecordBatch, None, None], stream)

    assert next(stream_iter).num_rows == 2
    assert fake_connection.closed == 0

    stream_iter.close()

    assert fake_connection.closed == 1


def test_duckdb_transform_returns_empty_iterator_without_connecting(monkeypatch):
    connect_calls = 0

    def fake_connect():
        nonlocal connect_calls
        connect_calls += 1
        raise AssertionError("connect should not be called for empty input")

    monkeypatch.setattr(duckdb_transform.duckdb, "connect", fake_connect)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())

    assert list(adapter.apply_filters_and_masks_stream([], ["id"], None, {})) == []
    assert connect_calls == 0


def test_duckdb_transform_streams_chunked_output(monkeypatch):
    monkeypatch.setattr(duckdb_transform, "_DUCKDB_ARROW_OUTPUT_BATCH_SIZE", 2)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    input_batch = pa.record_batch([pa.array(list(range(6)))], names=["id"])

    result_batches = list(adapter.apply_filters_and_masks_stream([input_batch], ["id"], None, {}))

    assert [batch.num_rows for batch in result_batches] == [2, 2, 2]
    assert sum(batch.num_rows for batch in result_batches) == 6


def test_duckdb_transform_memory_is_bounded_in_subprocess():
    script = textwrap.dedent(
        """
        import json
        import sys

        import psutil
        import pyarrow as pa

        from dal_obscura.infrastructure.adapters.duckdb_transform import DuckDBRowTransformAdapter, DefaultMaskingAdapter

        total_batches = int(sys.argv[1])
        rows_per_batch = int(sys.argv[2])
        process = psutil.Process()
        adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())

        def source():
            for batch_index in range(total_batches):
                start = batch_index * rows_per_batch
                yield pa.record_batch(
                    [
                        pa.array(range(start, start + rows_per_batch), type=pa.int64()),
                        pa.array(range(rows_per_batch), type=pa.int64()),
                    ],
                    names=["id", "value"],
                )

        baseline_rss = process.memory_info().rss
        peak_rss = baseline_rss
        row_count = 0
        for batch in adapter.apply_filters_and_masks_stream(source(), ["id", "value"], "id >= 0", {}):
            row_count += batch.num_rows
            peak_rss = max(peak_rss, process.memory_info().rss)

        print(
            json.dumps(
                {
                    "baseline_rss": baseline_rss,
                    "peak_rss": peak_rss,
                    "rows": row_count,
                }
            )
        )
        """
    )

    def run_probe(total_batches: int, rows_per_batch: int) -> dict[str, int]:
        completed = subprocess.run(
            [sys.executable, "-c", script, str(total_batches), str(rows_per_batch)],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)

    rows_per_batch = 50_000
    medium = run_probe(total_batches=32, rows_per_batch=rows_per_batch)
    large = run_probe(total_batches=256, rows_per_batch=rows_per_batch)
    medium_delta = medium["peak_rss"] - medium["baseline_rss"]
    large_delta = large["peak_rss"] - large["baseline_rss"]

    assert medium["rows"] == 32 * rows_per_batch
    assert large["rows"] == 256 * rows_per_batch
    assert large_delta >= medium_delta
    assert large_delta < medium_delta * 6
    assert large_delta < 256 * 1024 * 1024
