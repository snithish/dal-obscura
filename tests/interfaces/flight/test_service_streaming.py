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
from dal_obscura.interfaces.flight.contracts import parse_descriptor
from dal_obscura.interfaces.flight.server import DataAccessFlightService

JWT_SECRET = "test-jwt-secret-32-characters-long"


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
        payload = json.dumps(
            {
                "catalog": request.catalog,
                "target": request.target,
                "columns": list(request.columns),
            }
        ).encode("utf-8")
        return Plan(
            schema=self.schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=self.schema,
                    partition=StubInputPartition(payload=payload),
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


class DummyContext:
    def __init__(self, headers):
        self.headers = headers


def _make_jwt(principal_id: str) -> str:
    return jwt.encode({"sub": principal_id}, JWT_SECRET, algorithm="HS256")


def _authorization_header(principal_id: str) -> tuple[bytes, bytes]:
    value = f"Bearer {_make_jwt(principal_id)}".encode()
    return b"authorization", value


def _build_server(
    table_format: StubTableFormat,
    policy_path,
    jwt_secret: str = JWT_SECRET,
    ticket_secret: str = "secret",
    ticket_ttl_seconds: int = 300,
    max_tickets: int = 1,
) -> DataAccessFlightService:
    identity = DefaultIdentityAdapter(AuthConfig(jwt_secret=jwt_secret))
    authorizer = PolicyFileAuthorizer(policy_path)
    catalog_registry = StubCatalogRegistry(table_format)
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(masking)
    ticket_codec = HmacTicketCodecAdapter(ticket_secret)
    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=authorizer,
        catalog_registry=cast(Any, catalog_registry),
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=ticket_ttl_seconds,
        max_tickets=max_tickets,
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
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch1, batch2),
    )

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
    server = _build_server(table_format=table_format, policy_path=policy_path)
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


def test_flight_streaming_masks_nested_struct_fields_and_filters_rows(tmp_path):
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
            pa.field("region", pa.string()),
            pa.field("user", user_type),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["us", "eu", "us"], type=pa.string()),
            pa.array(
                [
                    {"email": "alpha@example.com", "address": {"zip": 1011, "city": "AMS"}},
                    {"email": "beta@example.com", "address": {"zip": 2022, "city": "BER"}},
                    {"email": "gamma@example.com", "address": {"zip": 3033, "city": "NYC"}},
                ],
                type=user_type,
            ),
        ],
        schema=schema,
    )
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "user"]
            masks:
              "user.address.zip":
                type: "hash"
            row_filter: "region = 'us'"
"""
        )
    )
    server = _build_server(table_format=table_format, policy_path=policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id", "user"],
            }
        ).encode("utf-8")
    )
    options = flight.FlightCallOptions(headers=[_authorization_header("user1")])

    info = client.get_flight_info(descriptor, options=options)
    table = client.do_get(info.endpoints[0].ticket, options=options).read_all()

    user_field = info.schema.field("user")
    address_field = user_field.type.field("address")
    assert address_field.type.field("zip").type == pa.string()
    assert table.num_rows == 2
    assert [row["id"] for row in table.to_pylist()] == [1, 3]
    assert all(len(row["user"]["address"]["zip"]) == 64 for row in table.to_pylist())

    server.shutdown()
    thread.join(timeout=2)


def test_flight_streaming_supports_nested_projection_with_combined_row_filters(tmp_path):
    user_type = pa.struct(
        [
            pa.field("email", pa.string()),
            pa.field(
                "address",
                pa.struct([pa.field("zip", pa.int64())]),
            ),
        ]
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("active", pa.bool_()),
            pa.field("user", user_type),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2, 3, 4], type=pa.int64()),
            pa.array(["us", "us", "eu", "us"], type=pa.string()),
            pa.array([True, False, True, True], type=pa.bool_()),
            pa.array(
                [
                    {"email": "alpha@example.com", "address": {"zip": 1011}},
                    {"email": "beta@example.com", "address": {"zip": 2022}},
                    {"email": "gamma@example.com", "address": {"zip": 3033}},
                    {"email": "delta@example.com", "address": {"zip": 4044}},
                ],
                type=user_type,
            ),
        ],
        schema=schema,
    )
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["group:analyst"]
            columns: ["id", "user.address.zip"]
            masks:
              "user.address.zip":
                type: "hash"
            row_filter: "region = 'us'"
          - principals: ["user1"]
            columns: ["user.email"]
            masks:
              "user.email":
                type: "redact"
                value: "[hidden]"
            row_filter: "active = true"
"""
        )
    )
    server = _build_server(table_format=table_format, policy_path=policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id", "user.address.zip", "user.email"],
            }
        ).encode("utf-8")
    )
    token = jwt.encode({"sub": "user1", "groups": ["analyst"]}, JWT_SECRET, algorithm="HS256")
    options = flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {token}".encode())])

    info = client.get_flight_info(descriptor, options=options)
    table = client.do_get(info.endpoints[0].ticket, options=options).read_all()

    assert info.schema.names == ["id", "user.address.zip", "user.email"]
    assert info.schema.field("user.address.zip").type == pa.string()
    assert info.schema.field("user.email").type == pa.string()
    assert table.num_rows == 2
    assert table.column("id").to_pylist() == [1, 4]
    assert table.column("user.email").to_pylist() == ["[hidden]", "[hidden]"]
    assert all(len(value) == 64 for value in table.column("user.address.zip").to_pylist())

    server.shutdown()
    thread.join(timeout=2)


def test_flight_streaming_masks_list_of_struct_fields(tmp_path):
    preference_type = pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("theme", pa.string()),
        ]
    )
    metadata_type = pa.struct(
        [
            pa.field("preferences", pa.list_(preference_type)),
        ]
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("metadata", metadata_type),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1], type=pa.int64()),
            pa.array(
                [
                    {
                        "preferences": [
                            {"name": "web", "theme": "dark"},
                            {"name": "mobile", "theme": "light"},
                        ]
                    }
                ],
                type=metadata_type,
            ),
        ],
        schema=schema,
    )
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        _catalog_policy(
            """
      "test.table":
        rules:
          - principals: ["user1"]
            columns: ["id", "metadata"]
            masks:
              "metadata.preferences.theme":
                type: "redact"
                value: "[hidden]"
"""
        )
    )
    server = _build_server(table_format=table_format, policy_path=policy_path)
    thread = _start_server(server)
    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "catalog": "analytics",
                "target": "test.table",
                "columns": ["id", "metadata"],
            }
        ).encode("utf-8")
    )
    options = flight.FlightCallOptions(headers=[_authorization_header("user1")])

    info = client.get_flight_info(descriptor, options=options)
    table = client.do_get(info.endpoints[0].ticket, options=options).read_all()

    metadata_field = info.schema.field("metadata")
    preferences_field = metadata_field.type.field("preferences")
    assert preferences_field.type.value_field.type.field("theme").type == pa.string()
    preferences = table.column("metadata").to_pylist()[0]["preferences"]
    assert [item["theme"] for item in preferences] == ["[hidden]", "[hidden]"]

    server.shutdown()
    thread.join(timeout=2)


def test_flight_logs_include_resident_memory(tmp_path, caplog, monkeypatch):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])
    batch = pa.record_batch([pa.array([1, 2]), pa.array(["us", "eu"])], schema=schema)
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

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
    server = _build_server(table_format=table_format, policy_path=policy_path)
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
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

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
    server = _build_server(table_format=table_format, policy_path=policy_path)
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
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

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
    server = _build_server(table_format=table_format, policy_path=policy_path)
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
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

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
    server = _build_server(table_format=table_format, policy_path=policy_path)
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
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="test.table",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )

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
    server = _build_server(table_format=table_format, policy_path=policy_path)
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
