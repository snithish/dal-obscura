import pyarrow as pa
import pytest

from dal_obscura.common.access_control.filters import parse_row_filter
from dal_obscura.common.access_control.models import MaskRule
from dal_obscura.data_plane.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from tests.support.benchmark_data import (
    nested_masking_batches,
    record_benchmark_info,
    scalar_masking_batches,
)


@pytest.mark.benchmark(group="row-filter-mask")
def test_benchmark_row_filter_only(benchmark):
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    batch_count = 8
    rows_per_batch = 4_096
    batches = scalar_masking_batches(batch_count=batch_count, rows_per_batch=rows_per_batch)
    expected_rows = batch_count * (rows_per_batch // 2)
    row_filter = parse_row_filter("region = 'us'", batches[0].schema)

    def run() -> pa.Table:
        result = list(
            adapter.apply_filters_and_masks_stream(
                batches,
                ["id", "email", "region"],
                row_filter,
                {},
            )
        )
        return pa.Table.from_batches(result)

    table = benchmark(run)

    record_benchmark_info(
        benchmark,
        scenario="filter-only",
        input_rows=batch_count * rows_per_batch,
        output_rows=expected_rows,
    )
    assert table.num_rows == expected_rows
    assert table.schema.names == ["id", "email", "region"]


@pytest.mark.benchmark(group="row-filter-mask")
def test_benchmark_mask_only(benchmark):
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    batch_count = 8
    rows_per_batch = 4_096
    batches = scalar_masking_batches(batch_count=batch_count, rows_per_batch=rows_per_batch)

    def run() -> pa.Table:
        result = list(
            adapter.apply_filters_and_masks_stream(
                batches,
                ["id", "email", "region"],
                None,
                {
                    "id": MaskRule(type="hash"),
                    "email": MaskRule(type="redact", value="[hidden]"),
                },
            )
        )
        return pa.Table.from_batches(result)

    table = benchmark(run)

    record_benchmark_info(
        benchmark,
        scenario="mask-only",
        input_rows=batch_count * rows_per_batch,
        output_rows=batch_count * rows_per_batch,
    )
    assert table.num_rows == batch_count * rows_per_batch
    assert table.schema.field("id").type == pa.string()
    assert table.column("email").to_pylist()[:3] == ["[hidden]", "[hidden]", "[hidden]"]


@pytest.mark.benchmark(group="row-filter-mask")
def test_benchmark_row_filter_and_top_level_masks(benchmark):
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    batches = scalar_masking_batches(batch_count=8, rows_per_batch=4_096)
    expected_rows = 8 * 2_048
    row_filter = parse_row_filter("region = 'us'", batches[0].schema)

    def run() -> pa.Table:
        result = list(
            adapter.apply_filters_and_masks_stream(
                batches,
                ["id", "email", "region"],
                row_filter,
                {
                    "id": MaskRule(type="hash"),
                    "email": MaskRule(type="redact", value="[hidden]"),
                },
            )
        )
        return pa.Table.from_batches(result)

    table = benchmark(run)

    record_benchmark_info(
        benchmark,
        scenario="filter-plus-mask",
        input_rows=8 * 4_096,
        output_rows=expected_rows,
    )
    assert table.num_rows == expected_rows
    assert table.schema.field("id").type == pa.string()
    assert table.column("email").to_pylist()[:3] == ["[hidden]", "[hidden]", "[hidden]"]


@pytest.mark.benchmark(group="row-filter-mask")
def test_benchmark_row_filter_and_nested_masks(benchmark):
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    batch_count = 8
    rows_per_batch = 2_048
    batches = nested_masking_batches(batch_count=batch_count, rows_per_batch=rows_per_batch)
    expected_rows = sum(value % 3 != 0 for value in range(batch_count * rows_per_batch))
    row_filter = parse_row_filter("active = true", batches[0].schema)

    def run() -> pa.Table:
        result = list(
            adapter.apply_filters_and_masks_stream(
                batches,
                ["id", "user"],
                row_filter,
                {"user.address.zip": MaskRule(type="hash")},
            )
        )
        return pa.Table.from_batches(result)

    table = benchmark(run)

    record_benchmark_info(
        benchmark,
        scenario="nested-field-mask",
        input_rows=batch_count * rows_per_batch,
        output_rows=expected_rows,
    )
    assert table.num_rows == expected_rows
    user_field = table.schema.field("user")
    assert user_field.type.field("address").type.field("zip").type == pa.string()
