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

from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from tests.support.flight import (
    build_flight_service,
    command_descriptor,
    flight_call_options,
    running_flight_client,
)
from tests.support.memory_probe import begin_memory_probe, run_memory_probe
from tests.support.policy import allow_rule

JWT_SECRET = "benchmark-jwt-secret-32-characters"


LARGE_BENCHMARK_TOTAL_ROWS = 25_000_000


LARGE_BENCHMARK_ROWS_PER_FILE = 5_000_000


LARGE_BENCHMARK_FILE_COUNT = LARGE_BENCHMARK_TOTAL_ROWS // LARGE_BENCHMARK_ROWS_PER_FILE


LARGE_BENCHMARK_MAX_TICKETS = 4


LARGE_BENCHMARK_RSS_LIMIT_BYTES = 1024 * 1024 * 1024


PC = cast(Any, pc)


COMPLEX_BENCHMARK_COLUMNS = [
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
]


COMPLEX_BENCHMARK_MASKS = {
    "id": {"type": "hash"},
    "account_number": {"type": "keep_last", "value": 4},
    "nickname": {"type": "null"},
    "notes": {"type": "redact", "value": "[redacted-note]"},
    "status": {"type": "default", "value": "benchmark-default"},
    "user.email": {"type": "email"},
    "user.address.zip": {"type": "hash"},
}


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
) -> dict[str, Any]:
    rows = 0
    schema: pa.Schema | None = None
    first_row: dict[str, object] | None = None
    chunk_count = 0
    max_chunk_rows = 0
    first_chunk_elapsed_s: float | None = None
    read_started_at = time.perf_counter()

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
    policy_rules = [
        allow_rule(
            COMPLEX_BENCHMARK_COLUMNS,
            principals=["group:analyst"],
            masks=cast(dict[str, object], COMPLEX_BENCHMARK_MASKS),
        )
    ]

    loaded_catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=catalog_uri,
        warehouse=str(warehouse),
    )
    planned_file_count = len(list(loaded_catalog.load_table(identifier).scan().plan_files()))
    catalog_registry = CatalogRegistry(
        ServiceConfig(
            catalogs={
                catalog_name: CatalogConfig(
                    name=catalog_name,
                    type="iceberg",
                    options={"type": "sql", "uri": catalog_uri, "warehouse": str(warehouse)},
                )
            },
        )
    )
    server = build_flight_service(
        catalog_registry=catalog_registry,
        policy_rules=policy_rules,
        jwt_secret=JWT_SECRET,
        ticket_secret="benchmark-ticket-secret",
        max_tickets=max_tickets,
        max_ticket_exchanges=2,
    )
    with running_flight_client(server) as client:
        options = flight_call_options("user1", groups=["analyst"], jwt_secret=JWT_SECRET)
        descriptor = command_descriptor(
            {
                "catalog": catalog_name,
                "target": identifier,
                "columns": COMPLEX_BENCHMARK_COLUMNS,
            }
        )
        info = client.get_flight_info(descriptor, options=options)
        if sample_rss:
            begin_memory_probe()
        metrics = _consume_streamed_info(client, info, options)
        return {
            **metrics,
            "planned_file_count": planned_file_count,
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
            "write.parquet.row-group-limit": "65536",
        },
    )

    for batch in _iter_complex_batches(batch_count=batch_count, rows_per_batch=rows_per_batch):
        table.append(pa.Table.from_batches([batch], schema=batch.schema))

    return catalog_uri, warehouse


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

        from tests.support.stream_benchmark import (
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
        schema = result["schema"]
        first_row = result["first_row"]
        user = first_row["user"]
        address = user["address"]
        print(
            json.dumps(
                {
                    "rows": result["rows"],
                    "planned_file_count": result["planned_file_count"],
                    "rows_per_file": result["rows_per_file"],
                    "endpoint_count": result["endpoint_count"],
                    "chunk_count": result["chunk_count"],
                    "max_chunk_rows": result["max_chunk_rows"],
                    "first_chunk_elapsed_s": result["first_chunk_elapsed_s"],
                    "total_read_elapsed_s": result["total_read_elapsed_s"],
                    "schema_types": {
                        "id": str(schema.field("id").type),
                        "created_at": str(schema.field("created_at").type),
                        "birth_date": str(schema.field("birth_date").type),
                        "nickname": str(schema.field("nickname").type),
                        "user_email": str(schema.field("user").type.field("email").type),
                        "user_zip": str(
                            schema.field("user").type.field("address").type.field("zip").type
                        ),
                    },
                    "first_row": {
                        "id": first_row["id"],
                        "account_number": first_row["account_number"],
                        "nickname": first_row["nickname"],
                        "notes": first_row["notes"],
                        "status": first_row["status"],
                        "user_email": user["email"],
                        "user_zip": address["zip"],
                    },
                }
            )
        )
        """
    )
    return run_memory_probe(
        script, [str(tmp_path), str(total_rows), str(rows_per_file), str(max_tickets)], timeout=300
    )
