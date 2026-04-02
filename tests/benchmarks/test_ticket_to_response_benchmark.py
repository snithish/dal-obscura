import json
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

import jwt
import pyarrow as pa
import pyarrow.flight as flight
import pytest

from dal_obscura.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.domain.catalog.ports import TableFormat
from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.domain.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)
from dal_obscura.infrastructure.adapters.identity_default import (
    AuthConfig,
    DefaultIdentityAdapter,
)
from dal_obscura.infrastructure.adapters.policy_file_authorizer import PolicyFileAuthorizer
from dal_obscura.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter
from dal_obscura.interfaces.flight.server import DataAccessFlightService

JWT_SECRET = "benchmark-jwt-secret-32-characters"


@dataclass(frozen=True, kw_only=True)
class StubInputPartition(InputPartition):
    payload: bytes = b"payload"


@dataclass(frozen=True, kw_only=True)
class StubTableFormat(TableFormat):
    schema: pa.Schema
    batches: tuple[pa.RecordBatch, ...]

    def get_schema(self) -> pa.Schema:
        return self.schema

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        del max_tickets
        return Plan(
            schema=self.schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=self.schema,
                    partition=StubInputPartition(),
                )
            ],
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[Any]]:
        if not isinstance(partition, StubInputPartition):
            raise TypeError("StubTableFormat requires a StubInputPartition")
        return self.schema, iter(self.batches)


class StubCatalogRegistry:
    def __init__(self, table_format: StubTableFormat) -> None:
        self._table_format = table_format

    def describe(self, catalog: str | None, target: str) -> TableFormat:
        del catalog, target
        return self._table_format


def _build_server(table_format: StubTableFormat, policy_path) -> DataAccessFlightService:
    identity = DefaultIdentityAdapter(AuthConfig(jwt_secret=JWT_SECRET))
    authorizer = PolicyFileAuthorizer(policy_path)
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(masking)
    ticket_codec = HmacTicketCodecAdapter("benchmark-ticket-secret")
    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=cast(Any, StubCatalogRegistry(table_format)),
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=1,
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=authorizer,
        masking=masking,
        row_transform=row_transform,
        ticket_codec=ticket_codec,
    )
    return DataAccessFlightService(
        location="grpc+tcp://0.0.0.0:0",
        plan_access_use_case=plan_access,
        fetch_stream_use_case=fetch_stream,
    )


def _start_server(server: DataAccessFlightService) -> threading.Thread:
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    deadline = time.time() + 5
    while time.time() < deadline:
        if server.port > 0:
            return thread
        time.sleep(0.05)
    raise RuntimeError("Flight server failed to start")


def _flight_options() -> flight.FlightCallOptions:
    token = jwt.encode({"sub": "user1", "groups": ["analyst"]}, JWT_SECRET, algorithm="HS256")
    return flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {token}".encode())])


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

    server = _build_server(table_format=table_format, policy_path=policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    options = _flight_options()
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "catalog": "analytics",
                "target": "bench.table",
                "columns": ["id", "user", "account"],
            }
        ).encode("utf-8")
    )
    info = client.get_flight_info(descriptor, options=options)
    ticket = info.endpoints[0].ticket
    expected_rows = sum(
        value % 2 == 0 and value % 3 != 0 for value in range(batch_count * rows_per_batch)
    )

    def run() -> pa.Table:
        return client.do_get(ticket, options=options).read_all()

    try:
        table = benchmark(run)
    finally:
        server.shutdown()
        thread.join(timeout=2)

    benchmark.extra_info["input_rows"] = batch_count * rows_per_batch
    benchmark.extra_info["output_rows"] = expected_rows
    assert table.num_rows == expected_rows
    assert table.schema.field("id").type == pa.string()
    user_field = table.schema.field("user")
    assert user_field.type.field("address").type.field("zip").type == pa.string()
    assert table.column("account").to_pylist()[0]["status"] == "standard"
    assert table.column("user").to_pylist()[0]["email"] == "[hidden]"
