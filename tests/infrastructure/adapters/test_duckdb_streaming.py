import textwrap
from collections.abc import Generator
from typing import cast

import pyarrow as pa
import pytest

import dal_obscura.read.transform as duckdb_transform
from dal_obscura.policy.filters import parse_row_filter
from dal_obscura.policy.models import MaskRule
from dal_obscura.read.transform import (
    DuckDBRowTransformAdapter,
    InputBatchLimitError,
    StreamAdmissionError,
    StreamMemoryLimitError,
)
from tests.support.memory_probe import run_memory_probe


def test_filters_and_masks_preserve_results_across_batch_boundaries():
    schema = pa.schema([pa.field("id", pa.int64())])
    batches = [
        pa.record_batch([pa.array(values)], schema=schema) for values in ([0, 1], [2, 3], [4, 5])
    ]
    adapter = DuckDBRowTransformAdapter()
    output = pa.Table.from_batches(
        list(
            adapter.apply_filters_and_masks_stream(
                batches,
                ["id"],
                parse_row_filter("id > 1", schema),
                {"id": MaskRule(type="redact", value="hidden")},
            )
        )
    )
    assert output.column("id").to_pylist() == ["hidden"] * 4


def test_duckdb_transform_disables_external_access(monkeypatch):
    result_batch = pa.record_batch([pa.array([1])], names=["id"])
    captured_configs: list[dict[str, object]] = []

    class FakeRelation:
        def query(self, table_name: str, query: str):
            del table_name, query
            return self

        def to_arrow_reader(self, batch_size: int):
            del batch_size
            return iter([result_batch])

    class FakeConnection:
        def execute(self, query: str) -> None:
            del query

        def from_arrow(self, reader: pa.RecordBatchReader):
            del reader
            return FakeRelation()

        def close(self) -> None:
            return None

    def fake_connect(**kwargs):
        captured_configs.append(kwargs)
        return FakeConnection()

    monkeypatch.setattr(duckdb_transform.duckdb, "connect", fake_connect)
    adapter = DuckDBRowTransformAdapter()

    list(
        adapter.apply_filters_and_masks_stream(
            [pa.record_batch([pa.array([1], type=pa.int64())], names=["id"])],
            ["id"],
            None,
            {},
        )
    )

    assert captured_configs == [
        {
            "config": {
                "enable_external_access": "false",
                "autoload_known_extensions": "false",
                "autoinstall_known_extensions": "false",
                "threads": 1,
                "memory_limit": "512MB",
            }
        }
    ]


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

        def execute(self, query: str) -> None:
            del query

        def from_arrow(self, reader: pa.RecordBatchReader):
            return FakeRelation()

        def close(self) -> None:
            self.closed += 1

    fake_connection = FakeConnection()
    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda **_kwargs: fake_connection)
    adapter = DuckDBRowTransformAdapter()
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


def test_duckdb_transform_rejects_streams_beyond_configured_admission_limit():
    adapter = DuckDBRowTransformAdapter(max_active_streams=1)
    batch = pa.record_batch([pa.array([1, 2, 3])], names=["id"])
    first = adapter.apply_filters_and_masks_stream([batch], ["id"], None, {})

    assert next(cast(Generator[pa.RecordBatch, None, None], first)).num_rows == 3
    second = adapter.apply_filters_and_masks_stream([batch], ["id"], None, {})
    with pytest.raises(StreamAdmissionError, match="capacity"):
        next(cast(Generator[pa.RecordBatch, None, None], second))

    cast(Generator[pa.RecordBatch, None, None], first).close()
    assert list(adapter.apply_filters_and_masks_stream([batch], ["id"], None, {}))


def test_stream_admission_precedes_source_reads_and_creation_is_lazy():
    adapter = DuckDBRowTransformAdapter(max_active_streams=1)
    batch = pa.record_batch([pa.array([1])], names=["id"])
    consumed = []

    def source():
        consumed.append(True)
        yield batch

    first = iter(adapter.apply_filters_and_masks_stream(source(), ["id"], None, {}))
    assert consumed == []
    next(first)
    second = iter(adapter.apply_filters_and_masks_stream(source(), ["id"], None, {}))
    with pytest.raises(StreamAdmissionError):
        next(second)
    assert consumed == [True]
    cast(Generator[pa.RecordBatch, None, None], first).close()


def test_input_budget_counts_retained_slice_buffers():
    batch = pa.record_batch([pa.array(range(10000), type=pa.int64())], names=["id"]).slice(0, 1)
    assert batch.nbytes == 8
    adapter = DuckDBRowTransformAdapter(max_input_batch_bytes=100)
    with pytest.raises(InputBatchLimitError, match="limit"):
        list(adapter.apply_filters_and_masks_stream([batch], ["id"], None, {}))


def test_input_failure_closes_source_and_releases_admission():
    closed = []

    def source():
        try:
            yield pa.record_batch([pa.array(["oversized"])], names=["id"])
        finally:
            closed.append(True)

    adapter = DuckDBRowTransformAdapter(max_active_streams=1, max_input_batch_bytes=8)
    with pytest.raises(InputBatchLimitError):
        list(adapter.apply_filters_and_masks_stream(source(), ["id"], None, {}))
    assert closed == [True]
    assert list(
        adapter.apply_filters_and_masks_stream(
            [pa.record_batch([pa.array([1])], names=["id"])], ["id"], None, {}
        )
    )


@pytest.mark.parametrize("limit", ["-1", "0B", "unlimited", "80%", "nanGB", "garbage"])
def test_duckdb_memory_limit_must_be_a_positive_finite_size(limit):
    with pytest.raises(ValueError, match="memory_limit"):
        DuckDBRowTransformAdapter(duckdb_memory_limit=limit)


def test_real_duckdb_oom_releases_slot_and_does_not_expose_query():
    adapter = DuckDBRowTransformAdapter(max_active_streams=1, duckdb_memory_limit="1KB")
    batch = pa.record_batch([pa.array(range(10000))], names=["id"])
    for _ in range(2):
        with pytest.raises(StreamMemoryLimitError, match="memory budget exhausted") as error:
            list(
                adapter.apply_filters_and_masks_stream(
                    [batch], ["id"], None, {"id": MaskRule(type="hash")}
                )
            )
        assert "SELECT" not in str(error.value)


def test_later_input_limit_failure_keeps_its_type_and_closes_source():
    closed = []

    def source():
        try:
            yield pa.record_batch([pa.array([1])], names=["id"])
            yield pa.record_batch([pa.array(range(100))], names=["id"])
        finally:
            closed.append(True)

    adapter = DuckDBRowTransformAdapter(max_input_batch_bytes=8)
    with pytest.raises(InputBatchLimitError):
        list(adapter.apply_filters_and_masks_stream(source(), ["id"], None, {}))
    assert closed == [True]


@pytest.mark.parametrize("limit", [0, -1])
def test_duckdb_transform_rejects_non_positive_admission_limits(limit):
    with pytest.raises(ValueError, match="max_active_streams"):
        DuckDBRowTransformAdapter(max_active_streams=limit)


def test_duckdb_transform_rejects_an_oversized_input_batch_before_query_execution():
    adapter = DuckDBRowTransformAdapter(max_input_batch_bytes=1)
    batch = pa.record_batch([pa.array([1])], names=["id"])

    with pytest.raises(InputBatchLimitError, match="limit"):
        list(adapter.apply_filters_and_masks_stream([batch], ["id"], None, {}))


def test_duckdb_transform_streams_chunked_output(monkeypatch):
    monkeypatch.setattr(duckdb_transform, "_DUCKDB_ARROW_OUTPUT_BATCH_SIZE", 2)
    adapter = DuckDBRowTransformAdapter()
    input_batch = pa.record_batch([pa.array(list(range(6)))], names=["id"])

    result_batches = list(adapter.apply_filters_and_masks_stream([input_batch], ["id"], None, {}))

    assert [batch.num_rows for batch in result_batches] == [2, 2, 2]
    assert sum(batch.num_rows for batch in result_batches) == 6


@pytest.mark.heavy
@pytest.mark.parametrize("payload_bytes", [0, 256])
def test_duckdb_transform_memory_is_bounded_in_subprocess(payload_bytes):
    script = textwrap.dedent(
        """
        import json
        import sys

        from tests.support.memory_probe import begin_memory_probe
        import pyarrow as pa

        from dal_obscura.policy.filters import parse_row_filter
        from dal_obscura.read.transform import (
                        DuckDBRowTransformAdapter,
        )

        total_batches = int(sys.argv[1])
        rows_per_batch = int(sys.argv[2])
        payload_bytes = int(sys.argv[3])
        adapter = DuckDBRowTransformAdapter()

        def source():
            for batch_index in range(total_batches):
                start = batch_index * rows_per_batch
                yield pa.record_batch(
                    [
                        pa.array(range(start, start + rows_per_batch), type=pa.int64()),
                        pa.array(range(rows_per_batch), type=pa.int64()),
                        pa.array(["x" * payload_bytes] * rows_per_batch),
                    ],
                    names=["id", "value", "payload"],
                )

        begin_memory_probe()
        row_count = 0
        for batch in adapter.apply_filters_and_masks_stream(
            source(),
            ["id", "value", "payload"],
            parse_row_filter(
                "id >= 0",
                pa.schema(
                    [
                        pa.field("id", pa.int64()),
                        pa.field("value", pa.int64()),
                    ]
                ),
            ),
            {},
        ):
            row_count += batch.num_rows

        print(
            json.dumps(
                {
                    "rows": row_count,
                }
            )
        )
        """
    )

    def run_probe(total_batches: int, rows_per_batch: int) -> dict[str, int]:
        return run_memory_probe(
            script, [str(total_batches), str(rows_per_batch), str(payload_bytes)]
        )

    rows_per_batch = 50_000
    medium = run_probe(total_batches=32, rows_per_batch=rows_per_batch)
    large = run_probe(total_batches=256, rows_per_batch=rows_per_batch)
    medium_delta = medium["peak_rss"] - medium["baseline_rss"]
    large_delta = large["peak_rss"] - large["baseline_rss"]

    assert medium["rows"] == 32 * rows_per_batch
    assert large["rows"] == 256 * rows_per_batch
    assert medium["rss_samples"] > 2 and large["rss_samples"] > 2
    assert large_delta < medium_delta + 64 * 1024 * 1024
    assert large_delta < 256 * 1024 * 1024


def test_output_batch_limit_rejects_oversized_result_batch():
    batch = pa.record_batch([pa.array(["payload"])], names=["value"])

    with pytest.raises(duckdb_transform.OutputBatchLimitError, match="output batch"):
        list(duckdb_transform._bounded_output_batches([batch], max_output_batch_bytes=1))


def test_first_output_does_not_consume_later_input_batches():
    consumed = []

    def source():
        for index in range(10):
            consumed.append(index)
            yield pa.record_batch([pa.array([index])], names=["id"])

    adapter = DuckDBRowTransformAdapter()
    stream = cast(
        Generator[pa.RecordBatch, None, None],
        adapter.apply_filters_and_masks_stream(source(), ["id"], None, {}),
    )
    try:
        assert next(stream).column(0).to_pylist() == [0]
        assert consumed == [0]
    finally:
        stream.close()


def test_input_schema_changes_are_rejected():
    adapter = DuckDBRowTransformAdapter()
    batches = [
        pa.record_batch([pa.array([1])], names=["id"]),
        pa.record_batch([pa.array(["changed"])], names=["id"]),
    ]
    with pytest.raises(ValueError, match="schema changed"):
        list(adapter.apply_filters_and_masks_stream(batches, ["id"], None, {}))


def test_connection_setup_failure_closes_connection(monkeypatch):
    closed = []

    class Connection:
        def execute(self, query):
            raise RuntimeError("setup failed")

        def close(self):
            closed.append(True)

    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda **kwargs: Connection())
    with pytest.raises(RuntimeError, match="setup failed"):
        duckdb_transform.connect()
    assert closed == [True]
