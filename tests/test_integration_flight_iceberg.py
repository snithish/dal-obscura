import json
import threading
import time

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from dal_obscura.auth import AuthConfig
from dal_obscura.backend.iceberg import IcebergBackend, IcebergConfig
from dal_obscura.service import DataAccessFlightService, ServerConfig


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
    from pyiceberg.types import LongType, NestedField, StringType

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
        ]
    )
    data = pa.table(
        {
            "id": [1, 2, 3],
            "email": ["a@example.com", "b@example.com", "c@example.com"],
            "region": ["us", "eu", "us"],
        },
        schema=arrow_schema,
    )
    # pyiceberg tables expose append/overwrite in recent versions.
    if hasattr(table, "append"):
        table.append(data)
    elif hasattr(table, "overwrite"):
        table.overwrite(data)
    else:
        pytest.skip("pyiceberg table append API not available")
    return identifier, warehouse


def test_flight_plan_and_get_with_iceberg(tmp_path):
    table_id, warehouse = _create_iceberg_table(tmp_path)
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        """
version: 1
datasets:
  - table: "default.*"
    rules:
      - principals: ["user1"]
        columns: ["id", "email", "region"]
        masks:
          email: { type: "redact", value: "***" }
        row_filter: "region = 'us'"
"""
    )

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
    server = DataAccessFlightService(
        location="grpc+tcp://0.0.0.0:0",
        backend=backend,
        config=ServerConfig(
            policy_path=str(policy_path),
            ticket_secret="secret",
            ticket_ttl_seconds=300,
            auth=AuthConfig(api_keys={"apikey123": "user1"}),
        ),
    )
    thread = _start_server(server)

    client = flight.FlightClient(f"grpc+tcp://localhost:{server.port}")
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "table": table_id,
                "columns": ["id", "email", "region"],
                "auth_token": "ApiKey apikey123",
            }
        ).encode("utf-8")
    )
    options = flight.FlightCallOptions(headers=[(b"authorization", b"ApiKey apikey123")])
    info = client.get_flight_info(descriptor, options=options)
    assert info.endpoints, "Expected at least one endpoint"

    batches = []
    for endpoint in info.endpoints:
        reader = client.do_get(endpoint.ticket, options=options)
        batches.extend(reader.read_all().to_batches())

    result = pa.Table.from_batches(batches)
    assert result.num_rows == 2
    assert set(result.column("region").to_pylist()) == {"us"}
    assert set(result.column("email").to_pylist()) == {"***"}

    server.shutdown()
    thread.join(timeout=2)
