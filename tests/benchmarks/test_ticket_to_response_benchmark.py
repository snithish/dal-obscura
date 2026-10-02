from typing import Any

import pyarrow as pa
import pytest

from dal_obscura.read.transform import (
    _DUCKDB_ARROW_OUTPUT_BATCH_SIZE,
)
from dal_obscura.sources.planning import ScanTask
from tests.support.flight import (
    StubTableFormat,
    build_flight_service,
    command_descriptor,
    flight_call_options,
    running_flight_client,
)
from tests.support.policy import allow_rule
from tests.support.stream_benchmark import (
    JWT_SECRET,
    LARGE_BENCHMARK_FILE_COUNT,
    LARGE_BENCHMARK_MAX_TICKETS,
    LARGE_BENCHMARK_ROWS_PER_FILE,
    LARGE_BENCHMARK_RSS_LIMIT_BYTES,
    LARGE_BENCHMARK_TOTAL_ROWS,
    _complex_batches,
    _run_streaming_probe_in_subprocess,
)

pytestmark = pytest.mark.heavy


class _BenchmarkTaskCodec:
    """Keep fixture data outside tickets, as a real source keeps rows in storage.

    This lane measures fetch authentication, ticket integrity, transformation and
    Flight delivery. The multifile lane measures production scan-task restoration.
    """

    def __init__(self) -> None:
        self.task: ScanTask | None = None

    def encode(self, task: ScanTask) -> str:
        self.task = task
        return "benchmark-source-task"

    def decode(self, payload: str) -> ScanTask:
        if payload != "benchmark-source-task" or self.task is None:
            raise ValueError("Unknown benchmark source task")
        return self.task


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
    policy_rules = [
        allow_rule(
            ["id", "user", "account"],
            principals=["group:analyst"],
            masks={
                "id": {"type": "hash"},
                "user.email": {"type": "redact", "value": "[hidden]"},
                "user.address.zip": {"type": "hash"},
            },
            row_filter="region = 'us'",
        ),
        allow_rule(
            ["id", "user", "account"],
            masks={"account.status": {"type": "default", "value": "standard"}},
            row_filter="active = true",
        ),
    ]

    expected_rows = 10_922

    server = build_flight_service(
        table_format=table_format,
        policy_rules=policy_rules,
        jwt_secret=JWT_SECRET,
        ticket_secret="benchmark-ticket-secret",
        max_ticket_exchanges=10_000,
        task_codec=_BenchmarkTaskCodec(),
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


@pytest.mark.benchmark(group="ticket-to-response-streaming")
def test_ticket_to_response_streaming_is_chunked_with_bounded_rss(tmp_path, benchmark):
    def run() -> tuple[dict[str, Any], dict[str, Any]]:
        smaller = _run_streaming_probe_in_subprocess(
            tmp_path,
            total_rows=10_000_000,
            rows_per_file=5_000_000,
            max_tickets=LARGE_BENCHMARK_MAX_TICKETS,
        )
        larger = _run_streaming_probe_in_subprocess(
            tmp_path,
            total_rows=LARGE_BENCHMARK_TOTAL_ROWS,
            rows_per_file=LARGE_BENCHMARK_ROWS_PER_FILE,
            max_tickets=LARGE_BENCHMARK_MAX_TICKETS,
        )
        return smaller, larger

    smaller, larger = benchmark.pedantic(run, rounds=1, iterations=1, warmup_rounds=0)
    benchmark.extra_info["input_rows"] = 35_000_000
    benchmark.extra_info["output_rows"] = 35_000_000
    benchmark.extra_info["large_probe_planned_files"] = larger["planned_file_count"]
    benchmark.extra_info["large_probe_rows_per_file"] = larger["rows_per_file"]
    benchmark.extra_info["large_probe_endpoint_count"] = larger["endpoint_count"]
    benchmark.extra_info["large_probe_stream_chunks"] = larger["chunk_count"]
    benchmark.extra_info["large_probe_max_chunk_rows"] = larger["max_chunk_rows"]
    benchmark.extra_info["small_probe_rss_delta"] = smaller["rss_delta"]
    benchmark.extra_info["large_probe_rss_delta"] = larger["rss_delta"]
    benchmark.extra_info["large_probe_rss_limit"] = LARGE_BENCHMARK_RSS_LIMIT_BYTES

    for probe in (smaller, larger):
        assert probe["rss_samples"] > 2
        assert probe["chunk_count"] > probe["endpoint_count"]
        assert probe["max_chunk_rows"] <= _DUCKDB_ARROW_OUTPUT_BATCH_SIZE
        assert probe["first_chunk_elapsed_s"] < probe["total_read_elapsed_s"] * 0.5

    assert smaller["rows"] == 10_000_000
    assert smaller["planned_file_count"] == 2
    assert smaller["endpoint_count"] == 2
    assert larger["rows"] == LARGE_BENCHMARK_TOTAL_ROWS
    assert larger["planned_file_count"] == LARGE_BENCHMARK_FILE_COUNT
    assert larger["rows_per_file"] == LARGE_BENCHMARK_ROWS_PER_FILE
    assert larger["endpoint_count"] == LARGE_BENCHMARK_MAX_TICKETS
    assert larger["rss_delta"] < LARGE_BENCHMARK_RSS_LIMIT_BYTES
    assert larger["rss_delta"] < smaller["rss_delta"] + 256 * 1024 * 1024

    schema = larger["schema_types"]
    assert schema["id"] == "string"
    assert schema["created_at"] == "timestamp[us]"
    assert schema["birth_date"] == "date32[day]"
    assert schema["nickname"] in {"string", "large_string"}
    assert schema["user_email"] in {"string", "large_string"}
    assert schema["user_zip"] == "string"

    first_row = larger["first_row"]
    assert first_row["id"] != "0"
    assert len(first_row["id"]) == 64
    assert first_row["account_number"] != "ACCT-000000000000"
    assert first_row["account_number"].endswith("0000")
    assert first_row["nickname"] is None
    assert first_row["notes"] == "[redacted-note]"
    assert first_row["status"] == "benchmark-default"
    assert first_row["user_email"] == "u***@example.com"
    assert len(first_row["user_zip"]) == 64
