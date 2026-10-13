import pyarrow as pa

from dal_obscura.policy.filters import parse_row_filter
from dal_obscura.policy.models import MaskRule
from dal_obscura.read.transform import (
    DuckDBRowTransformAdapter,
)


def test_duckdb_transform_filters_on_hidden_execution_column():
    adapter = DuckDBRowTransformAdapter()
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
    adapter = DuckDBRowTransformAdapter()
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


def test_duckdb_transform_evaluates_function_filters_on_hidden_values():
    adapter = DuckDBRowTransformAdapter()
    input_batch = pa.record_batch(
        [
            pa.array([1, 2, 3, 4], type=pa.int64()),
            pa.array(["US", "eu", None, "us"], type=pa.string()),
        ],
        names=["id", "region"],
    )

    batches = list(
        adapter.apply_filters_and_masks_stream(
            [input_batch],
            ["id"],
            parse_row_filter("lower(region) = 'us'", input_batch.schema),
            {},
        )
    )

    result = pa.Table.from_batches(batches)
    assert result.schema.equals(pa.schema([pa.field("id", pa.int64())]), check_metadata=True)
    assert result.to_pylist() == [{"id": 1}, {"id": 4}]
