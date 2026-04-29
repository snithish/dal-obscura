import json
import subprocess
import sys
import textwrap
import time
from collections.abc import Iterable
from contextlib import suppress
from datetime import date
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight
import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    ListType,
    LongType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)

from dal_obscura.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    DynamicCatalogRegistry,
    ServiceConfig,
)
from dal_obscura.infrastructure.adapters.duckdb_transform import _DUCKDB_ARROW_OUTPUT_BATCH_SIZE
from tests.support.flight import (
    StubTableFormat,
    build_flight_service,
    command_descriptor,
    flight_call_options,
    running_flight_client,
)

JWT_SECRET = "benchmark-jwt-secret-32-characters"
LARGE_BENCHMARK_TOTAL_ROWS = 25_000_000
LARGE_BENCHMARK_ROWS_PER_FILE = 5_000_000
LARGE_BENCHMARK_FILE_COUNT = LARGE_BENCHMARK_TOTAL_ROWS // LARGE_BENCHMARK_ROWS_PER_FILE
LARGE_BENCHMARK_MAX_TICKETS = 4
# Empirical delta from the current 25M-row subprocess run was ~1.586 GB.
# Keep a modest buffer for machine-to-machine variance without allowing
# accidental full-result materialization to slip through.
LARGE_BENCHMARK_RSS_LIMIT_BYTES = 1_900_000_000
PC = cast(Any, pc)


def _benchmark_complex_schema() -> pa.Schema:
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
    account_type = pa.struct(
        [
            pa.field("status", pa.string()),
            pa.field("tier", pa.string()),
        ]
    )
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string()),
            pa.field("active", pa.bool_()),
            pa.field("score", pa.float64()),
            pa.field("created_at", pa.timestamp("us")),
            pa.field("birth_date", pa.date32()),
            pa.field("account_number", pa.string()),
            pa.field("nickname", pa.string()),
            pa.field("notes", pa.string()),
            pa.field("status", pa.string()),
            pa.field("user", user_type),
            pa.field("account", account_type),
            pa.field("tags", pa.list_(pa.string())),
        ]
    )


def _benchmark_complex_iceberg_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="region", field_type=StringType(), required=False),
        NestedField(field_id=3, name="active", field_type=BooleanType(), required=False),
        NestedField(field_id=4, name="score", field_type=DoubleType(), required=False),
        NestedField(field_id=5, name="created_at", field_type=TimestampType(), required=False),
        NestedField(field_id=6, name="birth_date", field_type=DateType(), required=False),
        NestedField(field_id=7, name="account_number", field_type=StringType(), required=False),
        NestedField(field_id=8, name="nickname", field_type=StringType(), required=False),
        NestedField(field_id=9, name="notes", field_type=StringType(), required=False),
        NestedField(field_id=10, name="status", field_type=StringType(), required=False),
        NestedField(
            field_id=11,
            name="user",
            field_type=StructType(
                NestedField(field_id=12, name="email", field_type=StringType(), required=False),
                NestedField(
                    field_id=13,
                    name="address",
                    field_type=StructType(
                        NestedField(field_id=14, name="zip", field_type=LongType(), required=False),
                        NestedField(
                            field_id=15,
                            name="city",
                            field_type=StringType(),
                            required=False,
                        ),
                    ),
                    required=False,
                ),
            ),
            required=False,
        ),
        NestedField(
            field_id=16,
            name="account",
            field_type=StructType(
                NestedField(field_id=17, name="status", field_type=StringType(), required=False),
                NestedField(field_id=18, name="tier", field_type=StringType(), required=False),
            ),
            required=False,
        ),
        NestedField(
            field_id=19,
            name="tags",
            field_type=ListType(
                element_id=20,
                element_type=StringType(),
                element_required=False,
            ),
            required=False,
        ),
    )


def _remainder(values: pa.Array, divisor: int) -> pa.Array:
    divisor_scalar = pa.scalar(divisor, type=pa.int64())
    quotients = PC.cast(PC.floor(PC.divide(values, divisor_scalar)), pa.int64())
    return cast(pa.Array, PC.subtract(values, PC.multiply(quotients, divisor_scalar)))


def _iter_complex_batches(batch_count: int, rows_per_batch: int) -> Iterable[pa.RecordBatch]:
    schema = _benchmark_complex_schema()
    list_type = pa.list_(pa.string())
    base_created_at = pa.scalar(1_700_000_000_000_000, type=pa.int64())
    one_thousand = pa.scalar(1_000, type=pa.int64())
    ten_thousand = pa.scalar(10_000, type=pa.int64())
    base_birth_date = pa.scalar(date(2000, 1, 1), type=pa.date32())

    for batch_index in range(batch_count):
        start = batch_index * rows_per_batch
        ids = pa.array(range(start, start + rows_per_batch), type=pa.int64())
        is_even = PC.equal(PC.bit_wise_and(ids, pa.scalar(1, type=pa.int64())), 0)
        rem3 = _remainder(ids, 3)
        rem5 = _remainder(ids, 5)
        rem37 = _remainder(ids, 37)
        rem128 = _remainder(ids, 128)
        rem997 = _remainder(ids, 997)
        rem1_000 = _remainder(ids, 1_000)
        rem90_000 = _remainder(ids, 90_000)

        active = PC.not_equal(rem3, pa.scalar(0, type=pa.int64()))
        region = PC.if_else(
            is_even,
            pa.repeat(pa.scalar("us"), rows_per_batch),
            pa.repeat(pa.scalar("eu"), rows_per_batch),
        )
        score = PC.divide(PC.cast(rem1_000, pa.float64()), pa.scalar(100.0))
        created_at = PC.cast(
            PC.add(base_created_at, PC.multiply(ids, one_thousand)),
            pa.timestamp("us"),
        )
        birth_date = pa.repeat(base_birth_date, rows_per_batch)
        account_number = PC.binary_join_element_wise(
            pa.repeat(pa.scalar("ACCT-"), rows_per_batch),
            PC.cast(ids, pa.string()),
            "",
        )
        nickname = PC.binary_join_element_wise(
            pa.repeat(pa.scalar("nick-"), rows_per_batch),
            PC.cast(rem997, pa.string()),
            "",
        )
        notes = PC.binary_join_element_wise(
            pa.repeat(pa.scalar(f"note-{batch_index}-"), rows_per_batch),
            PC.cast(rem37, pa.string()),
            "",
        )
        status = PC.if_else(
            PC.equal(rem5, pa.scalar(0, type=pa.int64())),
            pa.repeat(pa.scalar("vip"), rows_per_batch),
            pa.repeat(pa.scalar("standard"), rows_per_batch),
        )
        email = PC.binary_join_element_wise(
            pa.repeat(pa.scalar("u"), rows_per_batch),
            PC.cast(rem1_000, pa.string()),
            pa.repeat(pa.scalar("@example.com"), rows_per_batch),
            "",
        )
        city = PC.binary_join_element_wise(
            pa.repeat(pa.scalar("city-"), rows_per_batch),
            PC.cast(rem128, pa.string()),
            "",
        )
        user_array = PC.make_struct(
            email,
            PC.make_struct(
                PC.add(ten_thousand, rem90_000),
                city,
                field_names=["zip", "city"],
            ),
            field_names=["email", "address"],
        )
        account_array = PC.make_struct(
            PC.if_else(
                PC.equal(_remainder(ids, 4), pa.scalar(0, type=pa.int64())),
                pa.repeat(pa.scalar("gold"), rows_per_batch),
                pa.repeat(pa.scalar("silver"), rows_per_batch),
            ),
            PC.binary_join_element_wise(
                pa.repeat(pa.scalar("tier-"), rows_per_batch),
                PC.cast(rem3, pa.string()),
                "",
            ),
            field_names=["status", "tier"],
        )
        tags = PC.if_else(
            is_even,
            pa.repeat(pa.scalar(["tag-a", "segment-a"], type=list_type), rows_per_batch),
            pa.repeat(pa.scalar(["tag-b", "segment-b"], type=list_type), rows_per_batch),
        )
        yield pa.record_batch(
            [
                ids,
                region,
                active,
                score,
                created_at,
                birth_date,
                account_number,
                nickname,
                notes,
                status,
                user_array,
                account_array,
                tags,
            ],
            schema=schema,
        )


def _complex_batches(batch_count: int, rows_per_batch: int) -> tuple[pa.RecordBatch, ...]:
    return tuple(_iter_complex_batches(batch_count=batch_count, rows_per_batch=rows_per_batch))


def _consume_streamed_info(
    client: flight.FlightClient,
    info: flight.FlightInfo,
    options: flight.FlightCallOptions,
    *,
    process: Any | None = None,
) -> dict[str, Any]:
    rows = 0
    schema: pa.Schema | None = None
    first_row: dict[str, object] | None = None
    chunk_count = 0
    max_chunk_rows = 0
    first_chunk_elapsed_s: float | None = None
    read_started_at = time.perf_counter()
    peak_rss = None if process is None else process.memory_info().rss

    for endpoint in info.endpoints:
        reader = client.do_get(endpoint.ticket, options=options).to_reader()
        for batch in reader:
            if first_chunk_elapsed_s is None:
                first_chunk_elapsed_s = time.perf_counter() - read_started_at
            chunk_count += 1
            max_chunk_rows = max(max_chunk_rows, batch.num_rows)
            if schema is None:
                schema = batch.schema
            rows += batch.num_rows
            if first_row is None and batch.num_rows:
                first_row = batch.slice(0, 1).to_pylist()[0]
            if process is not None:
                peak_rss = max(cast(int, peak_rss), process.memory_info().rss)

    total_read_elapsed_s = time.perf_counter() - read_started_at

    return {
        "rows": rows,
        "schema": schema,
        "first_row": first_row,
        "chunk_count": chunk_count,
        "max_chunk_rows": max_chunk_rows,
        "first_chunk_elapsed_s": (
            total_read_elapsed_s if first_chunk_elapsed_s is None else first_chunk_elapsed_s
        ),
        "total_read_elapsed_s": total_read_elapsed_s,
        "endpoint_count": len(info.endpoints),
        "peak_rss": peak_rss,
    }


def _run_iceberg_stream_scenario(
    tmp_path: Path,
    *,
    total_rows: int,
    rows_per_file: int,
    max_tickets: int,
    sample_rss: bool = False,
) -> dict[str, Any]:
    if total_rows % rows_per_file != 0:
        raise ValueError("total_rows must be divisible by rows_per_file")

    batch_count = total_rows // rows_per_file
    catalog_name = "ice_bench"
    identifier = "default.massive_users"
    catalog_uri, warehouse = _create_benchmark_iceberg_table(
        tmp_path,
        catalog_name=catalog_name,
        identifier=identifier,
        batch_count=batch_count,
        rows_per_batch=rows_per_file,
    )
    policy_rules: list[dict[str, object]] = [
        {
            "principals": ["group:analyst"],
            "columns": [
                "id",
                "region",
                "active",
                "score",
                "created_at",
                "birth_date",
                "account_number",
                "nickname",
                "notes",
                "status",
                "user",
                "account",
                "tags",
            ],
            "masks": {
                "id": {"type": "hash"},
                "account_number": {"type": "keep_last", "value": 4},
                "nickname": {"type": "null"},
                "notes": {"type": "redact", "value": "[redacted-note]"},
                "status": {"type": "default", "value": "benchmark-default"},
                "user.email": {"type": "email"},
                "user.address.zip": {"type": "hash"},
            },
            "effect": "allow",
        }
    ]

    loaded_catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=catalog_uri,
        warehouse=str(warehouse),
    )
    planned_file_count = len(list(loaded_catalog.load_table(identifier).scan().plan_files()))
    catalog_registry = DynamicCatalogRegistry(
        ServiceConfig(
            catalogs={
                catalog_name: CatalogConfig(
                    name=catalog_name,
                    module="dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                    options={"type": "sql", "uri": catalog_uri, "warehouse": str(warehouse)},
                    targets={},
                )
            },
            paths=(),
        )
    )
    server = build_flight_service(
        catalog_registry=catalog_registry,
        policy_rules=policy_rules,
        jwt_secret=JWT_SECRET,
        ticket_secret="benchmark-ticket-secret",
        max_tickets=max_tickets,
    )
    with running_flight_client(server) as client:
        options = flight_call_options("user1", groups=["analyst"], jwt_secret=JWT_SECRET)
        descriptor = command_descriptor(
            {
                "catalog": catalog_name,
                "target": identifier,
                "columns": [
                    "id",
                    "region",
                    "active",
                    "score",
                    "created_at",
                    "birth_date",
                    "account_number",
                    "nickname",
                    "notes",
                    "status",
                    "user",
                    "account",
                    "tags",
                ],
            }
        )
        info = client.get_flight_info(descriptor, options=options)
        baseline_rss = None
        if sample_rss:
            import psutil

            process = psutil.Process()
            baseline_rss = process.memory_info().rss
            metrics = _consume_streamed_info(client, info, options, process=process)
        else:
            metrics = _consume_streamed_info(client, info, options)
        return {
            **metrics,
            "planned_file_count": planned_file_count,
            "baseline_rss": baseline_rss,
            "peak_rss": metrics["peak_rss"],
            "total_rows": total_rows,
            "rows_per_file": rows_per_file,
        }


def _run_large_iceberg_stream_scenario(
    tmp_path: Path,
    *,
    sample_rss: bool = False,
) -> dict[str, Any]:
    return _run_iceberg_stream_scenario(
        tmp_path,
        total_rows=LARGE_BENCHMARK_TOTAL_ROWS,
        rows_per_file=LARGE_BENCHMARK_ROWS_PER_FILE,
        max_tickets=LARGE_BENCHMARK_MAX_TICKETS,
        sample_rss=sample_rss,
    )


def _create_benchmark_iceberg_table(
    tmp_path: Path,
    *,
    catalog_name: str,
    identifier: str,
    batch_count: int,
    rows_per_batch: int,
) -> tuple[str, Path]:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog_uri = f"sqlite:///{tmp_path / f'{catalog_name}.db'}"
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=catalog_uri,
        warehouse=str(warehouse),
    )
    namespace = ".".join(identifier.split(".")[:-1])
    with suppress(Exception):
        catalog.create_namespace(namespace)

    table = catalog.create_table(
        identifier=identifier,
        schema=_benchmark_complex_iceberg_schema(),
        properties={
            "format-version": "2",
            "write.target-file-size-bytes": str(8 * 1024 * 1024 * 1024),
            "write.parquet.row-group-limit": str(rows_per_batch),
        },
    )

    for batch in _iter_complex_batches(batch_count=batch_count, rows_per_batch=rows_per_batch):
        table.append(pa.Table.from_batches([batch], schema=batch.schema))

    return catalog_uri, warehouse


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
    policy_rules: list[dict[str, object]] = [
        {
            "principals": ["group:analyst"],
            "columns": ["id", "user", "account"],
            "masks": {
                "id": {"type": "hash"},
                "user.email": {"type": "redact", "value": "[hidden]"},
                "user.address.zip": {"type": "hash"},
            },
            "row_filter": "region = 'us'",
            "effect": "allow",
        },
        {
            "principals": ["user1"],
            "columns": ["id", "user", "account"],
            "masks": {"account.status": {"type": "default", "value": "standard"}},
            "row_filter": "active = true",
            "effect": "allow",
        },
    ]

    expected_rows = sum(
        value % 2 == 0 and value % 3 != 0 for value in range(batch_count * rows_per_batch)
    )

    server = build_flight_service(
        table_format=table_format,
        policy_rules=policy_rules,
        jwt_secret=JWT_SECRET,
        ticket_secret="benchmark-ticket-secret",
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


@pytest.mark.benchmark(group="ticket-to-response", min_rounds=1, max_time=1)
def test_benchmark_ticket_to_response_streams_twenty_five_million_masked_rows(tmp_path, benchmark):
    def run() -> dict[str, object]:
        return _run_large_iceberg_stream_scenario(tmp_path)

    stream_result = benchmark.pedantic(run, rounds=1, iterations=1, warmup_rounds=0)

    benchmark.extra_info["input_rows"] = LARGE_BENCHMARK_TOTAL_ROWS
    benchmark.extra_info["output_rows"] = LARGE_BENCHMARK_TOTAL_ROWS
    benchmark.extra_info["storage_backend"] = "iceberg-sql-catalog"
    benchmark.extra_info["planned_files"] = cast(int, stream_result["planned_file_count"])
    benchmark.extra_info["endpoint_count"] = cast(int, stream_result["endpoint_count"])
    benchmark.extra_info["stream_chunks"] = cast(int, stream_result["chunk_count"])
    benchmark.extra_info["max_chunk_rows"] = cast(int, stream_result["max_chunk_rows"])
    assert cast(int, stream_result["planned_file_count"]) == LARGE_BENCHMARK_FILE_COUNT
    assert cast(int, stream_result["rows_per_file"]) == LARGE_BENCHMARK_ROWS_PER_FILE
    assert cast(int, stream_result["endpoint_count"]) > 1
    assert stream_result["rows"] == LARGE_BENCHMARK_TOTAL_ROWS
    schema = cast(pa.Schema, stream_result["schema"])
    first_row = cast(dict[str, object], stream_result["first_row"])
    assert schema.field("id").type == pa.string()
    assert schema.field("created_at").type == pa.timestamp("us")
    assert schema.field("birth_date").type == pa.date32()
    assert pa.types.is_string(schema.field("nickname").type) or pa.types.is_large_string(
        schema.field("nickname").type
    )
    assert pa.types.is_string(
        schema.field("user").type.field("email").type
    ) or pa.types.is_large_string(schema.field("user").type.field("email").type)
    assert schema.field("user").type.field("address").type.field("zip").type == pa.string()
    assert first_row["id"] != "0"
    assert len(cast(str, first_row["id"])) == 64
    assert first_row["account_number"] != "ACCT-000000000000"
    assert cast(str, first_row["account_number"]).endswith("0000")
    assert first_row["nickname"] is None
    assert first_row["notes"] == "[redacted-note]"
    assert first_row["status"] == "benchmark-default"
    assert cast(dict[str, object], first_row["user"])["email"] == "u***@example.com"
    assert cast(int, stream_result["chunk_count"]) >= cast(int, stream_result["endpoint_count"])
    assert cast(int, stream_result["max_chunk_rows"]) <= _DUCKDB_ARROW_OUTPUT_BATCH_SIZE
    assert (
        cast(float, stream_result["first_chunk_elapsed_s"])
        < cast(float, stream_result["total_read_elapsed_s"]) * 0.5
    )


def _run_streaming_probe_in_subprocess(
    tmp_path: Path,
    *,
    total_rows: int,
    rows_per_file: int,
    max_tickets: int,
) -> dict[str, Any]:
    script = textwrap.dedent(
        """
        import json
        import sys
        import tempfile
        from pathlib import Path

        from tests.benchmarks.test_ticket_to_response_benchmark import (
            _run_iceberg_stream_scenario,
        )

        workspace = Path(tempfile.mkdtemp(dir=sys.argv[1]))
        result = _run_iceberg_stream_scenario(
            workspace,
            total_rows=int(sys.argv[2]),
            rows_per_file=int(sys.argv[3]),
            max_tickets=int(sys.argv[4]),
            sample_rss=True,
        )
        print(
            json.dumps(
                {
                    "rows": result["rows"],
                    "planned_file_count": result["planned_file_count"],
                    "endpoint_count": result["endpoint_count"],
                    "chunk_count": result["chunk_count"],
                    "max_chunk_rows": result["max_chunk_rows"],
                    "first_chunk_elapsed_s": result["first_chunk_elapsed_s"],
                    "total_read_elapsed_s": result["total_read_elapsed_s"],
                    "baseline_rss": result["baseline_rss"],
                    "peak_rss": result["peak_rss"],
                }
            )
        )
        """
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(tmp_path),
            str(total_rows),
            str(rows_per_file),
            str(max_tickets),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    payload["rss_delta"] = payload["peak_rss"] - payload["baseline_rss"]
    return payload


def test_ticket_to_response_streaming_proves_chunked_delivery_in_subprocess(tmp_path):
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

    for probe in (smaller, larger):
        assert probe["chunk_count"] > probe["endpoint_count"]
        assert probe["max_chunk_rows"] <= _DUCKDB_ARROW_OUTPUT_BATCH_SIZE
        assert probe["first_chunk_elapsed_s"] < probe["total_read_elapsed_s"] * 0.5

    assert smaller["rows"] == 10_000_000
    assert smaller["planned_file_count"] == 2
    assert smaller["endpoint_count"] == 2
    assert larger["rows"] == LARGE_BENCHMARK_TOTAL_ROWS
    assert larger["planned_file_count"] == LARGE_BENCHMARK_FILE_COUNT
    assert larger["endpoint_count"] == LARGE_BENCHMARK_MAX_TICKETS
    assert larger["rss_delta"] < smaller["rss_delta"] * 2.25


def test_ticket_to_response_streaming_rss_is_bounded_in_subprocess(tmp_path):
    payload = _run_streaming_probe_in_subprocess(
        tmp_path,
        total_rows=LARGE_BENCHMARK_TOTAL_ROWS,
        rows_per_file=LARGE_BENCHMARK_ROWS_PER_FILE,
        max_tickets=LARGE_BENCHMARK_MAX_TICKETS,
    )
    rss_delta = payload["rss_delta"]

    assert payload["rows"] == LARGE_BENCHMARK_TOTAL_ROWS
    assert payload["planned_file_count"] == LARGE_BENCHMARK_FILE_COUNT
    assert payload["endpoint_count"] > 1
    assert payload["chunk_count"] >= payload["endpoint_count"]
    assert rss_delta < LARGE_BENCHMARK_RSS_LIMIT_BYTES
