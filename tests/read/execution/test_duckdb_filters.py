import pyarrow as pa
import pytest

from dal_obscura.adapters.duckdb.compiler import DuckDBProjectionCompiler
from dal_obscura.adapters.duckdb.transform import (
    DuckDBRowTransformAdapter,
)
from dal_obscura.kernel.policy.filters import parse_row_filter
from dal_obscura.kernel.policy.models import MaskRule


def test_duckdb_transform_filters_original_masked_values_before_output_mask():
    adapter = DuckDBRowTransformAdapter()
    input_batch = pa.record_batch(
        [pa.array(["alice@example.com", "bob@example.com"], type=pa.string())],
        names=["email"],
    )
    result_batches = list(
        adapter.apply_filters_and_masks_stream(
            [input_batch],
            DuckDBProjectionCompiler().compile(
                input_batch.schema,
                ["email"],
                {"email": MaskRule(type="redact", value="[hidden]")},
            ),
            parse_row_filter("email = 'alice@example.com'", input_batch.schema),
        )
    )
    result = pa.Table.from_batches(result_batches)
    assert result.column("email").to_pylist() == ["[hidden]"]


@pytest.mark.parametrize(
    "regions,expression,expected_ids",
    [
        (["us", "eu", "us"], "region = 'us'", [1, 3]),
        (["US", "eu", None, "us"], "lower(region) = 'us'", [1, 4]),
    ],
    ids=["equality", "function-and-null"],
)
def test_duckdb_transform_filters_hidden_execution_columns(regions, expression, expected_ids):
    input_batch = pa.record_batch(
        [
            pa.array(range(1, len(regions) + 1), type=pa.int64()),
            pa.array(regions, type=pa.string()),
        ],
        names=["id", "region"],
    )
    batches = DuckDBRowTransformAdapter().apply_filters_and_masks_stream(
        [input_batch],
        DuckDBProjectionCompiler().compile(input_batch.schema, ["id"], {}),
        parse_row_filter(expression, input_batch.schema),
    )
    result = pa.Table.from_batches(batches)
    assert result.schema.equals(pa.schema([pa.field("id", pa.int64())]), check_metadata=True)
    assert result.to_pylist() == [{"id": identifier} for identifier in expected_ids]
