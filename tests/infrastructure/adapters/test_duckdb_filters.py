import pyarrow as pa

import dal_obscura.data_plane.infrastructure.adapters.duckdb_transform as duckdb_transform
from dal_obscura.common.access_control.filters import parse_row_filter
from dal_obscura.common.access_control.models import MaskRule
from dal_obscura.data_plane.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)


def test_duckdb_transform_filters_on_hidden_execution_column():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    input_batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["us", "eu", "us"], type=pa.string()),
        ],
        names=["id", "region"],
    )

    result_batches = list(
        adapter.apply_filters_and_masks_stream(
            [input_batch],
            ["id"],
            parse_row_filter("region = 'us'", input_batch.schema),
            {},
        )
    )
    result = pa.Table.from_batches(result_batches)

    assert result.schema.names == ["id"]
    assert result.column("id").to_pylist() == [1, 3]


def test_duckdb_transform_filters_original_masked_values_before_output_mask():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    input_batch = pa.record_batch(
        [pa.array(["alice@example.com", "bob@example.com"], type=pa.string())],
        names=["email"],
    )
    result_batches = list(
        adapter.apply_filters_and_masks_stream(
            [input_batch],
            ["email"],
            parse_row_filter("email = 'alice@example.com'", input_batch.schema),
            {"email": MaskRule(type="redact", value="[hidden]")},
        )
    )
    result = pa.Table.from_batches(result_batches)
    assert result.column("email").to_pylist() == ["[hidden]"]


def test_duckdb_transform_uses_canonical_sql_for_function_filters(monkeypatch):
    result_batch = pa.record_batch([pa.array([10])], names=["id"])

    class FakeRelation:
        def __init__(self) -> None:
            self.query_calls: list[tuple[str, str]] = []

        def query(self, table_name: str, query: str):
            self.query_calls.append((table_name, query))
            return self

        def to_arrow_reader(self, batch_size: int):
            del batch_size
            return iter([result_batch])

    class FakeConnection:
        def __init__(self) -> None:
            self.relation = FakeRelation()

        def execute(self, query: str) -> None:
            del query

        def from_arrow(self, reader: pa.RecordBatchReader):
            del reader
            return self.relation

        def close(self) -> None:
            return None

    fake_connection = FakeConnection()
    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda **_kwargs: fake_connection)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    input_batch = pa.record_batch(
        [pa.array([1], type=pa.int64()), pa.array(["us"], type=pa.string())],
        names=["id", "region"],
    )

    list(
        adapter.apply_filters_and_masks_stream(
            [input_batch],
            ["id"],
            parse_row_filter("lower(region) = 'us'", input_batch.schema),
            {},
        )
    )

    assert fake_connection.relation.query_calls == [
        ("input", 'SELECT "id" AS "id" FROM input WHERE LOWER(region) = \'us\'')
    ]
