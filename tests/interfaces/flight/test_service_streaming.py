import json
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, cast

import jwt
import pyarrow as pa
import pyarrow.flight as flight
import pytest

from dal_obscura.application.use_cases.fetch_stream import FetchStreamUseCase
from dal_obscura.application.use_cases.plan_access import PlanAccessUseCase
from dal_obscura.domain.query_planning.models import (
    BackendBinding,
    BackendDescriptor,
    BackendReference,
    BoundBackendTarget,
    DatasetSelector,
    Plan,
    ReadPayload,
    ReadSpec,
)
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
    def describe(self, catalog: str | None, target: str) -> BackendDescriptor:
        selector = DatasetSelector(catalog=catalog, target=target)
        return StubDescriptor(
            dataset_identity=selector,
        )


class StubBackend:
    def __init__(self, schema: pa.Schema, batches: list[pa.RecordBatch]) -> None:
        self._schema = schema
        self._batches = batches

    def bind_descriptor(self, descriptor: BackendDescriptor) -> BoundBackendTarget:
        return BoundBackendTarget(
            dataset_identity=descriptor.dataset_identity,
            backend=BackendReference(backend_id=descriptor.backend_id, generation=1),
            binding=StubBinding(),
        )

    def schema_for(self, bound_target: BoundBackendTarget) -> pa.Schema:
        return self._schema

    def plan_for(
        self, bound_target: BoundBackendTarget, columns: Iterable[str], max_tickets: int
    ) -> Plan:
        payload = json.dumps(
            {
                "catalog": bound_target.dataset_identity.catalog,
                "target": bound_target.dataset_identity.target,
                "columns": list(columns),
            }
        ).encode("utf-8")
        return Plan(schema=self._schema, tasks=[ReadPayload(payload=payload)])

    def read_spec_for(self, backend: BackendReference, read_payload: bytes) -> ReadSpec:
        raw = json.loads(read_payload.decode("utf-8"))
        return ReadSpec(
            dataset=DatasetSelector(catalog=raw.get("catalog"), target=str(raw["target"])),
            columns=list(raw["columns"]),
            schema=self._schema,
        )

    def read_stream_for(self, backend: BackendReference, read_payload: bytes) -> Iterable[Any]:
        return iter(self._batches)


@dataclass(frozen=True)
class StubDescriptor:
    dataset_identity: DatasetSelector
    backend_id: str = field(init=False, default="duckdb_file")


@dataclass(frozen=True)
class StubBinding:
    backend_id: str = field(init=False, default="duckdb_file")


class DummyContext:
    def __init__(self, headers):
        self.headers = headers


def _make_jwt(principal_id: str) -> str:
    return jwt.encode({"sub": principal_id}, JWT_SECRET, algorithm="HS256")


def _authorization_header(principal_id: str) -> tuple[bytes, bytes]:
    value = f"Bearer {_make_jwt(principal_id)}".encode()
    return b"authorization", value


def _build_server(
    backend_registry: StubBackend,
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
        backend_registry=cast(Any, backend_registry),
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=ticket_ttl_seconds,
        max_tickets=max_tickets,
    )
    fetch_stream = FetchStreamUseCase(
        identity=identity,
        authorizer=authorizer,
        backend_registry=cast(Any, backend_registry),
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
    backend = StubBackend(schema, [batch1, batch2])

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
    server = _build_server(backend_registry=backend, policy_path=policy_path)
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
    backend = StubBackend(schema, [batch])

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
    server = _build_server(backend_registry=backend, policy_path=policy_path)
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
    backend = StubBackend(schema, [batch])

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
    server = _build_server(backend_registry=backend, policy_path=policy_path)
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
    backend = StubBackend(schema, [batch])

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
    server = _build_server(backend_registry=backend, policy_path=policy_path)
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
    backend = StubBackend(schema, [batch])

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
    server = _build_server(backend_registry=backend, policy_path=policy_path)
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
    backend = StubBackend(schema, [batch])

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
    server = _build_server(backend_registry=backend, policy_path=policy_path)
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
