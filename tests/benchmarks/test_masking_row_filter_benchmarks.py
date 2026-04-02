import pyarrow as pa
import pytest

from dal_obscura.domain.access_control.filters import parse_row_filter
from dal_obscura.domain.access_control.models import MaskRule
from dal_obscura.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)


def _scalar_batches(batch_count: int, rows_per_batch: int) -> list[pa.RecordBatch]:
    batches: list[pa.RecordBatch] = []
    for batch_index in range(batch_count):
        start = batch_index * rows_per_batch
        ids = list(range(start, start + rows_per_batch))
        batches.append(
            pa.record_batch(
                [
                    pa.array(ids, type=pa.int64()),
                    pa.array([f"user{i}@example.com" for i in ids], type=pa.string()),
                    pa.array(
                        ["us" if value % 2 == 0 else "eu" for value in ids],
                        type=pa.string(),
                    ),
                ],
                names=["id", "email", "region"],
            )
        )
    return batches


def _nested_batches(batch_count: int, rows_per_batch: int) -> list[pa.RecordBatch]:
    user_type = pa.struct(
        [
            pa.field("email", pa.string()),
            pa.field(
                "address",
                pa.struct(
                    [
                        pa.field("zip", pa.int64()),
                        pa.field("city", pa.string()),
                    ]
                ),
            ),
        ]
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("active", pa.bool_()),
            pa.field("user", user_type),
        ]
    )
    batches: list[pa.RecordBatch] = []
    for batch_index in range(batch_count):
        start = batch_index * rows_per_batch
        ids = list(range(start, start + rows_per_batch))
        batches.append(
            pa.record_batch(
                [
                    pa.array(ids, type=pa.int64()),
                    pa.array([value % 3 != 0 for value in ids], type=pa.bool_()),
                    pa.array(
                        [
                            {
                                "email": f"user{value}@example.com",
                                "address": {
                                    "zip": 10_000 + value,
                                    "city": f"city-{value % 16}",
                                },
                            }
                            for value in ids
                        ],
                        type=user_type,
                    ),
                ],
                schema=schema,
            )
        )
    return batches


@pytest.mark.benchmark(group="row-filter-mask")
def test_benchmark_row_filter_only(benchmark):
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    batch_count = 8
    rows_per_batch = 4_096
    batches = _scalar_batches(batch_count=batch_count, rows_per_batch=rows_per_batch)
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

    benchmark.extra_info["scenario"] = "filter-only"
    benchmark.extra_info["input_rows"] = batch_count * rows_per_batch
    benchmark.extra_info["output_rows"] = expected_rows
    assert table.num_rows == expected_rows
    assert table.schema.names == ["id", "email", "region"]


@pytest.mark.benchmark(group="row-filter-mask")
def test_benchmark_mask_only(benchmark):
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    batch_count = 8
    rows_per_batch = 4_096
    batches = _scalar_batches(batch_count=batch_count, rows_per_batch=rows_per_batch)

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

    benchmark.extra_info["scenario"] = "mask-only"
    benchmark.extra_info["input_rows"] = batch_count * rows_per_batch
    benchmark.extra_info["output_rows"] = batch_count * rows_per_batch
    assert table.num_rows == batch_count * rows_per_batch
    assert table.schema.field("id").type == pa.string()
    assert table.column("email").to_pylist()[:3] == ["[hidden]", "[hidden]", "[hidden]"]


@pytest.mark.benchmark(group="row-filter-mask")
def test_benchmark_row_filter_and_top_level_masks(benchmark):
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    batches = _scalar_batches(batch_count=8, rows_per_batch=4_096)
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

    benchmark.extra_info["scenario"] = "filter-plus-mask"
    benchmark.extra_info["input_rows"] = 8 * 4_096
    benchmark.extra_info["output_rows"] = expected_rows
    assert table.num_rows == expected_rows
    assert table.schema.field("id").type == pa.string()
    assert table.column("email").to_pylist()[:3] == ["[hidden]", "[hidden]", "[hidden]"]


@pytest.mark.benchmark(group="row-filter-mask")
def test_benchmark_row_filter_and_nested_masks(benchmark):
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    batch_count = 8
    rows_per_batch = 2_048
    batches = _nested_batches(batch_count=batch_count, rows_per_batch=rows_per_batch)
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

    benchmark.extra_info["scenario"] = "nested-field-mask"
    benchmark.extra_info["input_rows"] = batch_count * rows_per_batch
    benchmark.extra_info["output_rows"] = expected_rows
    assert table.num_rows == expected_rows
    user_field = table.schema.field("user")
    assert user_field.type.field("address").type.field("zip").type == pa.string()
