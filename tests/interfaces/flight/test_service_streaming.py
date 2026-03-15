import json
import threading
import time

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from dal_obscura.application.use_cases import FetchStreamUseCase, PlanAccessUseCase
from dal_obscura.domain.query_planning import Plan, ReadPayload, ReadSpec
from dal_obscura.infrastructure.adapters import (
    AuthConfig,
    DefaultIdentityAdapter,
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
    HmacTicketCodecAdapter,
    PolicyFileAuthorizer,
)
from dal_obscura.interfaces.flight import DataAccessFlightService


class StubBackend:
    def __init__(self, schema: pa.Schema, batches: list[pa.RecordBatch]) -> None:
        self._schema = schema
        self._batches = batches

    def get_schema(self, table: str) -> pa.Schema:
        return self._schema

    def plan(self, table: str, columns, max_tickets: int) -> Plan:
        payload = json.dumps({"table": table, "columns": list(columns)}).encode("utf-8")
        return Plan(schema=self._schema, tasks=[ReadPayload(payload=payload)])

    def read_spec(self, read_payload: bytes) -> ReadSpec:
        raw = json.loads(read_payload.decode("utf-8"))
        return ReadSpec(table=str(raw["table"]), columns=list(raw["columns"]))

    def read_stream(self, read_payload: bytes):
        return iter(self._batches)


class DummyContext:
    def __init__(self, headers):
        self.headers = headers


def _build_server(
    backend: StubBackend,
    policy_path,
    api_keys: dict[str, str],
    ticket_secret: str = "secret",
    ticket_ttl_seconds: int = 300,
    max_tickets: int = 1,
) -> DataAccessFlightService:
    identity = DefaultIdentityAdapter(AuthConfig(api_keys=api_keys))
    authorizer = PolicyFileAuthorizer(policy_path)
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(masking)
    ticket_codec = HmacTicketCodecAdapter(ticket_secret)
    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=authorizer,
        planning_backend=backend,
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=ticket_ttl_seconds,
        max_tickets=max_tickets,
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=authorizer,
        planning_backend=backend,
        read_backend=backend,
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


def test_streaming_contract_emits_multiple_batches(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "dal_obscura.infrastructure.adapters.duckdb_transform._DUCKDB_ARROW_OUTPUT_BATCH_SIZE",
        2,
    )
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch1 = pa.record_batch([pa.array([1, 2]), pa.array(["us", "eu"])], schema=schema)
    batch2 = pa.record_batch([pa.array([3, 4]), pa.array(["us", "us"])], schema=schema)
    backend = StubBackend(schema, [batch1, batch2])

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        """
version: 1
datasets:
  - table: "test.table"
    rules:
      - principals: ["user1"]
        columns: ["id", "region"]
"""
    )
    server = _build_server(
        backend=backend, policy_path=policy_path, api_keys={"apikey123": "user1"}
    )
    thread = _start_server(server)

    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "table": "test.table",
                "columns": ["id", "region"],
                "auth_token": "ApiKey apikey123",
            }
        ).encode("utf-8")
    )
    options = flight.FlightCallOptions(headers=[(b"x-api-key", b"apikey123")])
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
    backend = StubBackend(schema, [batch])

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        """
version: 1
datasets:
  - table: "test.table"
    rules:
      - principals: ["user1"]
        columns: ["id", "region"]
"""
    )
    monkeypatch.setattr(
        "dal_obscura.interfaces.flight.server.get_resident_memory_bytes",
        lambda: 4_242,
    )
    server = _build_server(
        backend=backend, policy_path=policy_path, api_keys={"apikey123": "user1"}
    )
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "table": "test.table",
                "columns": ["id", "region"],
                "auth_token": "ApiKey apikey123",
            }
        ).encode("utf-8")
    )
    context = DummyContext(headers=[(b"authorization", b"ApiKey apikey123")])

    with caplog.at_level("INFO"):
        info = server.get_flight_info(context, descriptor)
        server.do_get(context, info.endpoints[0].ticket)

    records = [record for record in caplog.records if record.message in {"plan_request", "do_get"}]
    assert [record.message for record in records] == ["plan_request", "do_get"]
    assert all(record.resident_memory_bytes == 4_242 for record in records)


def test_do_get_rejects_principal_mismatch(tmp_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    backend = StubBackend(schema, [batch])

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        """
version: 1
datasets:
  - table: "test.table"
    rules:
      - principals: ["user1"]
        columns: ["id", "region"]
"""
    )
    server = _build_server(
        backend=backend,
        policy_path=policy_path,
        api_keys={"apikey123": "user1", "apikey999": "user2"},
    )
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "table": "test.table",
                "columns": ["id", "region"],
                "auth_token": "ApiKey apikey123",
            }
        ).encode("utf-8")
    )
    plan_context = DummyContext(headers=[(b"authorization", b"ApiKey apikey123")])
    info = server.get_flight_info(plan_context, descriptor)

    do_get_context = DummyContext(headers=[(b"authorization", b"ApiKey apikey999")])
    with pytest.raises(flight.FlightUnauthorizedError):
        server.do_get(do_get_context, info.endpoints[0].ticket)


def test_policy_version_is_per_dataset(tmp_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1]), pa.array(["us"])], schema=schema)
    backend = StubBackend(schema, [batch])

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        """
version: 1
datasets:
  - table: "table_a"
    rules:
      - principals: ["user1"]
        columns: ["id", "region"]
  - table: "table_b"
    rules:
      - principals: ["user1"]
        columns: ["id"]
"""
    )
    server = _build_server(
        backend=backend, policy_path=policy_path, api_keys={"apikey123": "user1"}
    )
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "table": "table_a",
                "columns": ["id", "region"],
                "auth_token": "ApiKey apikey123",
            }
        ).encode("utf-8")
    )
    plan_context = DummyContext(headers=[(b"authorization", b"ApiKey apikey123")])
    info = server.get_flight_info(plan_context, descriptor)
    ticket = info.endpoints[0].ticket

    policy_path.write_text(
        """
version: 1
datasets:
  - table: "table_a"
    rules:
      - principals: ["user1"]
        columns: ["id", "region"]
  - table: "table_b"
    rules:
      - principals: ["user1"]
        columns: ["id", "region"]
        row_filter: "region = 'us'"
"""
    )
    do_get_context = DummyContext(headers=[(b"authorization", b"ApiKey apikey123")])
    server.do_get(do_get_context, ticket)

    policy_path.write_text(
        """
version: 1
datasets:
  - table: "table_a"
    rules:
      - principals: ["user1"]
        columns: ["id", "region"]
        row_filter: "region = 'us'"
  - table: "table_b"
    rules:
      - principals: ["user1"]
        columns: ["id", "region"]
"""
    )
    with pytest.raises(flight.FlightUnauthorizedError):
        server.do_get(do_get_context, ticket)
