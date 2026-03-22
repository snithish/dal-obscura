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
from dal_obscura.domain.catalog.ports import ResolvedTable
from dal_obscura.domain.format_handler.ports import FormatHandler, InputPartition, Plan, ScanTask
from dal_obscura.domain.query_planning.models import PlanRequest
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
from dal_obscura.interfaces.flight.contracts import parse_descriptor
from dal_obscura.interfaces.flight.server import DataAccessFlightService

JWT_SECRET = "test-jwt-secret-32-characters-long"


class StubCatalogRegistry:
    def describe(self, catalog: str | None, target: str) -> ResolvedTable:
        from dal_obscura.infrastructure.adapters.duckdb_handler import FileTable

        return FileTable(
            catalog_name=catalog or "",
            table_name=target,
            format="duckdb_file",
            file_format="duckdb_file",
            paths=(),
            options={},
        )


@dataclass(frozen=True)
class StubInputPartition(InputPartition):
    payload: bytes = b"payload"
    _table: ResolvedTable | None = None

    @property
    def table(self) -> ResolvedTable:
        from dal_obscura.infrastructure.adapters.duckdb_handler import FileTable

        return self._table or FileTable(
            catalog_name="",
            table_name="test",
            format="test",
            file_format="test",
            paths=(),
            options={},
        )


class StubFormatHandler(FormatHandler):
    def __init__(self, schema: pa.Schema, batches: list[pa.RecordBatch]) -> None:
        self._schema = schema
        self._batches = batches

    @property
    def supported_format(self):
        return "duckdb_file"

    def get_schema(self, table: ResolvedTable) -> pa.Schema:
        return self._schema

    def plan(self, table: ResolvedTable, request: PlanRequest, max_tickets: int) -> Plan:
        payload = json.dumps(
            {
                "catalog": request.catalog,
                "target": request.target,
                "columns": list(request.columns),
            }
        ).encode("utf-8")
        return Plan(
            schema=self._schema,
            tasks=[
                ScanTask(
                    format="duckdb_file",
                    schema=self._schema,
                    partition=StubInputPartition(payload=payload),
                )
            ],
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[Any]]:
        return self._schema, iter(self._batches)


class StubFormatRegistry:
    def __init__(self, handler: StubFormatHandler):
        self._handler = handler

    def get_handler(self, format: str):
        return self._handler


class DummyContext:
    def __init__(self, headers):
        self.headers = headers


def _make_jwt(principal_id: str) -> str:
    return jwt.encode({"sub": principal_id}, JWT_SECRET, algorithm="HS256")


def _authorization_header(principal_id: str) -> tuple[bytes, bytes]:
    value = f"Bearer {_make_jwt(principal_id)}".encode()
    return b"authorization", value


def _build_server(
    format_registry: StubFormatRegistry,
    policy_path,
    jwt_secret: str = JWT_SECRET,
    ticket_secret: str = "secret",
    ticket_ttl_seconds: int = 300,
    max_tickets: int = 1,
) -> DataAccessFlightService:
    identity = DefaultIdentityAdapter(AuthConfig(jwt_secret=jwt_secret))
    authorizer = PolicyFileAuthorizer(policy_path)
    catalog_registry = StubCatalogRegistry()
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(masking)
    ticket_codec = HmacTicketCodecAdapter(ticket_secret)
    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
        format_registry=cast(Any, format_registry),
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=ticket_ttl_seconds,
        max_tickets=max_tickets,
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=authorizer,
        format_registry=cast(Any, format_registry),
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


def _catalog_policy(targets: str) -> str:
    return f"""
version: 1
catalogs:
  analytics:
    targets:
{targets}
"""


def test_parse_descriptor_rejects_path_descriptor():
    descriptor = flight.FlightDescriptor.for_path("analytics", "users")
    with pytest.raises(ValueError):
        parse_descriptor(descriptor)


def test_streaming_contract_emits_multiple_batches(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "dal_obscura.infrastructure.adapters.duckdb_transform._DUCKDB_ARROW_OUTPUT_BATCH_SIZE",
        2,
    )
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch1 = pa.record_batch([pa.array([1, 2]), pa.array(["us", "eu"])], schema=schema)
    batch2 = pa.record_batch([pa.array([3, 4]), pa.array(["us", "us"])], schema=schema)
    handler = StubFormatHandler(schema, [batch1, batch2])
    registry = StubFormatRegistry(handler)

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    server = _build_server(format_registry=registry, policy_path=policy_path)
    thread = _start_server(server)

    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id", "region"],
            }
        ).encode("utf-8")
    )
    options = flight.FlightCallOptions(headers=[_authorization_header("user1")])
    info = client.get_flight_info(descriptor, options=options)
    reader = client.do_get(info.endpoints[0].ticket, options=options)
    result = reader.read_all()

    assert result.num_rows == 4
    assert result.column("id").num_chunks >= 2

    server.shutdown()
    thread.join(timeout=2)


def test_flight_logs_include_resident_memory(tmp_path, caplog, monkeypatch):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1, 2]), pa.array(["us", "eu"])], schema=schema)
    handler = StubFormatHandler(schema, [batch])
    registry = StubFormatRegistry(handler)

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    monkeypatch.setattr(
        "dal_obscura.interfaces.flight.server.get_resident_memory_bytes",
        lambda: 4_242,
    )
    server = _build_server(format_registry=registry, policy_path=policy_path)
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id", "region"],
            }
        ).encode("utf-8")
    )
    context = DummyContext(headers=[_authorization_header("user1")])

    with caplog.at_level("INFO"):
        info = server.get_flight_info(context, descriptor)
        server.do_get(context, info.endpoints[0].ticket)

    records = [record for record in caplog.records if record.message in {"plan_request", "do_get"}]
    assert [record.message for record in records] == ["plan_request", "do_get"]
    assert all(record.resident_memory_bytes == 4_242 for record in records)


def test_do_get_rejects_principal_mismatch(tmp_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    handler = StubFormatHandler(schema, [batch])
    registry = StubFormatRegistry(handler)

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    server = _build_server(format_registry=registry, policy_path=policy_path)
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id", "region"],
            }
        ).encode("utf-8")
    )
    plan_context = DummyContext(headers=[_authorization_header("user1")])
    info = server.get_flight_info(plan_context, descriptor)

    do_get_context = DummyContext(headers=[_authorization_header("user2")])
    with pytest.raises(flight.FlightUnauthorizedError):
        server.do_get(do_get_context, info.endpoints[0].ticket)


def test_descriptor_authorization_field_is_not_accepted(tmp_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    handler = StubFormatHandler(schema, [batch])
    registry = StubFormatRegistry(handler)

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    server = _build_server(format_registry=registry, policy_path=policy_path)
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id", "region"],
                "authorization": f"Bearer {_make_jwt('user1')}",
            }
        ).encode("utf-8")
    )

    with pytest.raises(flight.FlightUnauthorizedError):
        server.get_flight_info(DummyContext(headers=[]), descriptor)


def test_policy_version_is_per_dataset(tmp_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    handler = StubFormatHandler(schema, [batch])
    registry = StubFormatRegistry(handler)

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "table_a":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
      "table_b":
        rules:
          - principals: ["user1"]
            columns: ["id"]
"""
        )
    )
    server = _build_server(format_registry=registry, policy_path=policy_path)
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "catalog": "analytics",
                "target": "table_a",
                "columns": ["id", "region"],
            }
        ).encode("utf-8")
    )
    plan_context = DummyContext(headers=[_authorization_header("user1")])
    info = server.get_flight_info(plan_context, descriptor)
    ticket = info.endpoints[0].ticket

    policy_path.write_text(
        _catalog_policy(
            """
      "table_a":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
      "table_b":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
            row_filter: "region = 'us'"
"""
        )
    )
    do_get_context = DummyContext(headers=[_authorization_header("user1")])
    server.do_get(do_get_context, ticket)

    policy_path.write_text(
        _catalog_policy(
            """
      "table_a":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
            row_filter: "region = 'us'"
      "table_b":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    with pytest.raises(flight.FlightUnauthorizedError):
        server.do_get(do_get_context, ticket)


def test_do_get_requires_authorization_header(tmp_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    handler = StubFormatHandler(schema, [batch])
    registry = StubFormatRegistry(handler)

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "region"]
"""
        )
    )
    server = _build_server(format_registry=registry, policy_path=policy_path)
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id", "region"],
            }
        ).encode("utf-8")
    )
    plan_context = DummyContext(headers=[_authorization_header("user1")])
    info = server.get_flight_info(plan_context, descriptor)

    with pytest.raises(flight.FlightUnauthorizedError):
        server.do_get(DummyContext(headers=[]), info.endpoints[0].ticket)
