import pyarrow as pa
import pytest

from tests.support.flight import (
    StubTableFormat,
    build_flight_service,
    command_descriptor,
    flight_call_options,
    running_flight_client,
)

JWT_SECRET = "benchmark-jwt-secret-32-characters"


def _complex_batches(batch_count: int, rows_per_batch: int) -> tuple[pa.RecordBatch, ...]:
    preference_type = pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("theme", pa.string()),
            pa.field("notifications", pa.bool_()),
        ]
    )
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
            pa.field("preferences", pa.list_(preference_type)),
        ]
    )
    account_type = pa.struct(
        [
            pa.field("status", pa.string()),
            pa.field("tier", pa.string()),
        ]
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("active", pa.bool_()),
            pa.field("user", user_type),
            pa.field("account", account_type),
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
                    pa.array(
                        ["us" if value % 2 == 0 else "eu" for value in ids],
                        type=pa.string(),
                    ),
                    pa.array([value % 3 != 0 for value in ids], type=pa.bool_()),
                    pa.array(
                        [
                            {
                                "email": f"user{value}@example.com",
                                "address": {
                                    "zip": 10_000 + value,
                                    "city": f"city-{value % 32}",
                                },
                                "preferences": [
                                    {
                                        "name": "web",
                                        "theme": "dark" if value % 2 == 0 else "light",
                                        "notifications": value % 5 != 0,
                                    },
                                    {
                                        "name": "mobile",
                                        "theme": "light",
                                        "notifications": value % 7 == 0,
                                    },
                                ],
                            }
                            for value in ids
                        ],
                        type=user_type,
                    ),
                    pa.array(
                        [
                            {
                                "status": "gold" if value % 4 == 0 else "silver",
                                "tier": f"tier-{value % 3}",
                            }
                            for value in ids
                        ],
                        type=account_type,
                    ),
                ],
                schema=schema,
            )
        )
    return tuple(batches)


@pytest.mark.benchmark(group="ticket-to-response", min_rounds=5, max_time=1)
def test_benchmark_ticket_to_response_complex_schema(tmp_path, benchmark):
    batch_count = 8
    rows_per_batch = 4_096
    batches = _complex_batches(batch_count=batch_count, rows_per_batch=rows_per_batch)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="bench.table",
        format="stub_format",
        schema=batches[0].schema,
        batches=batches,
    )
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        """
version: 1
catalogs:
  analytics:
    targets:
      "bench.table":
        rules:
          - principals: ["group:analyst"]
            columns: ["id", "user", "account"]
            masks:
              id:
                type: "hash"
              "user.email":
                type: "redact"
                value: "[hidden]"
              "user.address.zip":
                type: "hash"
            row_filter: "region = 'us'"
          - principals: ["user1"]
            columns: ["id", "user", "account"]
            masks:
              "account.status":
                type: "default"
                value: "standard"
            row_filter: "active = true"
"""
    )

    server = build_flight_service(
        table_format=table_format,
        policy_path=policy_path,
        jwt_secret=JWT_SECRET,
        ticket_secret="benchmark-ticket-secret",
    )
    expected_rows = sum(
        value % 2 == 0 and value % 3 != 0 for value in range(batch_count * rows_per_batch)
    )

    with running_flight_client(server) as client:
        options = flight_call_options("user1", groups=["analyst"], jwt_secret=JWT_SECRET)
        descriptor = command_descriptor(
            {
                "catalog": "analytics",
                "target": "bench.table",
                "columns": ["id", "user", "account"],
            }
        )
        info = client.get_flight_info(descriptor, options=options)
        ticket = info.endpoints[0].ticket

        def run() -> pa.Table:
            return client.do_get(ticket, options=options).read_all()

        table = benchmark(run)

    benchmark.extra_info["input_rows"] = batch_count * rows_per_batch
    benchmark.extra_info["output_rows"] = expected_rows
    assert table.num_rows == expected_rows
    assert table.schema.field("id").type == pa.string()
    user_field = table.schema.field("user")
    assert user_field.type.field("address").type.field("zip").type == pa.string()
    assert table.column("account").to_pylist()[0]["status"] == "standard"
    assert table.column("user").to_pylist()[0]["email"] == "[hidden]"
