import json
import threading
import time

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from dal_obscura.application.use_cases import FetchStreamUseCase, PlanAccessUseCase
from dal_obscura.infrastructure.adapters import (
    AuthConfig,
    DefaultIdentityAdapter,
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
    HmacTicketCodecAdapter,
    IcebergBackend,
    IcebergConfig,
    PolicyFileAuthorizer,
)
from dal_obscura.interfaces.flight import DataAccessFlightService


def _start_server(server: DataAccessFlightService) -> threading.Thread:
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    deadline = time.time() + 5
    while time.time() < deadline:
        if server.port > 0:
            return thread
        time.sleep(0.05)
    raise RuntimeError("Flight server failed to start")


def _create_iceberg_table(tmp_path):
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType, StructType

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        "test",
        type="sql",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=str(warehouse),
    )
    identifier = "default.users"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="email", field_type=StringType(), required=False),
        NestedField(field_id=3, name="region", field_type=StringType(), required=False),
        NestedField(
            field_id=4,
            name="user",
            field_type=StructType(
                NestedField(field_id=5, name="name", field_type=StringType(), required=False),
                NestedField(field_id=6, name="zip", field_type=StringType(), required=False),
            ),
            required=False,
        ),
    )
    try:
        catalog.create_namespace("default")
    except Exception:
        pass

    table = catalog.create_table(
        identifier=identifier,
        schema=schema,
        properties={"format-version": "2"},
    )
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("email", pa.string(), nullable=True),
            pa.field("region", pa.string(), nullable=True),
            pa.field(
                "user",
                pa.struct(
                    [
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("zip", pa.string(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )
    row_count = 100_000
    ids = list(range(1, row_count + 1))
    emails = [f"user{i}@example.com" for i in ids]
    regions = ["us" if i % 2 == 0 else "eu" for i in ids]
    user_struct = [{"name": f"name{i}", "zip": f"{10000 + (i % 90000)}"} for i in ids]
    data = pa.table(
        {
            "id": ids,
            "email": emails,
            "region": regions,
            "user": user_struct,
        },
        schema=arrow_schema,
    )

    batch_size = 25_000
    for offset in range(0, row_count, batch_size):
        table.append(data.slice(offset, batch_size))

    table.delete("id <= 10000")
    return identifier, warehouse, row_count


def test_flight_plan_and_get_with_iceberg(tmp_path):
    table_id, warehouse, row_count = _create_iceberg_table(tmp_path)
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        """
version: 1
datasets:
  - table: "default.*"
    rules:
      - principals: ["user1"]
        columns: ["id", "email", "region", "user"]
        masks:
          id: { type: "hash" }
          "user.zip": { type: "redact", value: "***" }
        row_filter: "region = 'us'"
"""
    )

    identity = DefaultIdentityAdapter(AuthConfig(api_keys={"apikey123": "user1"}))
    authorizer = PolicyFileAuthorizer(policy_path)
    backend = IcebergBackend(
        IcebergConfig(
            catalog_name="test",
            catalog_options={
                "type": "sql",
                "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
                "warehouse": str(warehouse),
            },
        )
    )
    masking = DefaultMaskingAdapter()
    row_transform = DuckDBRowTransformAdapter(masking)
    ticket_codec = HmacTicketCodecAdapter("secret")
    plan_access = PlanAccessUseCase(
        identity=identity,
        authorizer=authorizer,
        planning_backend=backend,
        masking=masking,
        ticket_codec=ticket_codec,
        ticket_ttl_seconds=300,
        max_tickets=4,
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
    server = DataAccessFlightService(
        location="grpc+tcp://0.0.0.0:0",
        plan_access_use_case=plan_access,
        fetch_stream_use_case=fetch_stream,
    )
    thread = _start_server(server)

    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "table": table_id,
                "columns": ["id", "email", "region", "user"],
                "auth_token": "ApiKey apikey123",
            }
        ).encode("utf-8")
    )
    options = flight.FlightCallOptions(headers=[(b"x-api-key", b"apikey123")])
    info = client.get_flight_info(descriptor, options=options)
    assert info.endpoints, "Expected at least one endpoint"
    assert info.schema.field("id").type == pa.string()

    batches = []
    for endpoint in info.endpoints:
        reader = client.do_get(endpoint.ticket, options=options)
        batches.extend(reader.read_all().to_batches())

    result = pa.Table.from_batches(batches)
    deleted_rows = 10_000
    expected_rows = (row_count - deleted_rows) // 2
    assert result.num_rows == expected_rows
    assert set(result.column("region").to_pylist()) == {"us"}
    assert all(isinstance(value, str) for value in result.column("id").to_pylist())
    user_col = result.column("user").to_pylist()
    assert len(user_col) == result.num_rows
    assert all(item["zip"] == "***" for item in user_col if item is not None)

    server.shutdown()
    thread.join(timeout=2)
