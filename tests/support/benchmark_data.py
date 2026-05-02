from __future__ import annotations

import pyarrow as pa


def scalar_masking_batches(batch_count: int, rows_per_batch: int) -> list[pa.RecordBatch]:
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


def nested_masking_batches(batch_count: int, rows_per_batch: int) -> list[pa.RecordBatch]:
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


def record_benchmark_info(
    benchmark,
    *,
    scenario: str,
    input_rows: int,
    output_rows: int,
) -> None:
    benchmark.extra_info["scenario"] = scenario
    benchmark.extra_info["input_rows"] = input_rows
    benchmark.extra_info["output_rows"] = output_rows
