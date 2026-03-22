import json
import socket
import subprocess
import time
from pathlib import Path

import jwt
import pyarrow as pa
import pyarrow.flight as flight
import pytest
import yaml

pytest.importorskip("pyiceberg")
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    ListType,
    LongType,
    NestedField,
    StringType,
    StructType,
)


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture
def iceberg_setup(tmp_path: Path) -> tuple[str, Path]:
    """Sets up a sqlite pyiceberg catalog with a deeply nested table structure."""
    catalog_name = "e2e_catalog"
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()

    catalog_uri = f"sqlite:///{tmp_path / 'catalog.db'}"

    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=catalog_uri,
        warehouse=str(warehouse),
    )

    identifier = "default.users"

    # Deeply nested schema
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="email", field_type=StringType(), required=False),
        NestedField(
            field_id=3,
            name="metadata",
            field_type=StructType(
                NestedField(
                    field_id=4,
                    name="preferences",
                    field_type=ListType(
                        element_id=5,
                        element_type=StructType(
                            NestedField(
                                field_id=6, name="name", field_type=StringType(), required=False
                            ),
                            NestedField(
                                field_id=7, name="theme", field_type=StringType(), required=False
                            ),
                            NestedField(
                                field_id=8,
                                name="notifications",
                                field_type=StringType(),
                                required=False,
                            ),
                        ),
                        element_required=False,
                    ),
                    required=False,
                )
            ),
            required=False,
        ),
    )

    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier=identifier,
        schema=schema,
        properties={"format-version": "2"},
    )

    # Ingest data
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("email", pa.string(), nullable=True),
            pa.field(
                "metadata",
                pa.struct(
                    [
                        pa.field(
                            "preferences",
                            pa.list_(
                                pa.struct(
                                    [
                                        pa.field("name", pa.string(), nullable=True),
                                        pa.field("theme", pa.string(), nullable=True),
                                        pa.field("notifications", pa.string(), nullable=True),
                                    ]
                                )
                            ),
                            nullable=True,
                        )
                    ]
                ),
                nullable=True,
            ),
        ]
    )

    table.append(
        pa.table(
            {
                "id": [1, 2],
                "email": ["user1@example.com", "user2@example.com"],
                "metadata": [
                    {
                        "preferences": [
                            {"name": "web", "theme": "dark", "notifications": "enabled"},
                            {"name": "mobile", "theme": "light", "notifications": "disabled"},
                        ]
                    },
                    {
                        "preferences": [
                            {"name": "web", "theme": "light", "notifications": "enabled"},
                        ]
                    },
                ],
            },
            schema=arrow_schema,
        )
    )

    return catalog_uri, warehouse


@pytest.fixture
def configs(tmp_path: Path, iceberg_setup: tuple[str, Path]) -> tuple[Path, Path]:
    catalog_uri, warehouse = iceberg_setup

    service_config = {
        "catalogs": {
            "e2e_catalog": {
                "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                "options": {
                    "type": "sql",
                    "uri": catalog_uri,
                    "warehouse": str(warehouse),
                },
            }
        }
    }

    service_path = tmp_path / "service.yaml"
    with open(service_path, "w") as f:
        yaml.safe_dump(service_config, f)

    policy = {
        "version": 1,
        "catalogs": {
            "e2e_catalog": {
                "targets": {
                    "default.users": {
                        "rules": [
                            {
                                "principals": ["e2e_user"],
                                "columns": ["id", "email", "metadata"],
                            }
                        ]
                    }
                }
            }
        },
    }

    policy_path = tmp_path / "policy.json"
    with open(policy_path, "w") as f:
        json.dump(policy, f)

    return service_path, policy_path


def test_e2e_flight_server_with_iceberg(configs: tuple[Path, Path]):
    service_path, policy_path = configs
    port = get_free_port()
    jwt_secret = "e2e-very-secret-key-that-is-long-enough"
    ticket_secret = "e2e-ticket-secret"

    import os
    import sys

    cmd = [
        sys.executable,
        "-c",
        "import sys; from dal_obscura.interfaces.cli.main import main; sys.exit(main())",
        "--location",
        f"grpc://0.0.0.0:{port}",
        "--policy",
        str(policy_path),
        "--service-config",
        str(service_path),
        "--ticket-secret",
        ticket_secret,
        "--jwt-secret",
        jwt_secret,
    ]

    if os.environ.get("DEBUG_SERVER") == "1":
        cmd = [
            sys.executable,
            "-m",
            "debugpy",
            "--listen",
            "0.0.0.0:5678",
            "--wait-for-client",
            "-m",
            "dal_obscura.interfaces.cli.main",
            "--location",
            f"grpc://0.0.0.0:{port}",
            "--policy",
            str(policy_path),
            "--service-config",
            str(service_path),
            "--ticket-secret",
            ticket_secret,
            "--jwt-secret",
            jwt_secret,
        ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Wait for server to be healthy
    time.sleep(20)

    client = None
    try:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Server exited early:\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")

        client = flight.FlightClient(f"grpc+tcp://localhost:{port}")

        # Authenticate with valid JWT built matching our server expectation
        token = jwt.encode({"sub": "e2e_user"}, jwt_secret, algorithm="HS256")
        options = flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {token}".encode())])

        # Query the data
        descriptor = flight.FlightDescriptor.for_command(
            json.dumps(
                {
                    "catalog": "e2e_catalog",
                    "target": "default.users",
                    "columns": ["id", "email", "metadata"],
                }
            ).encode("utf-8")
        )

        info = client.get_flight_info(descriptor, options=options)

        batches = []
        for endpoint in info.endpoints:
            reader = client.do_get(endpoint.ticket, options=options)
            batches.extend(reader.read_all().to_batches())

        table = pa.Table.from_batches(batches) if batches else pa.table({})

        assert table.num_rows == 2
        assert table.schema.field("id").type == pa.int64()
        assert table.schema.field("email").type == pa.large_string()
        assert pa.types.is_struct(table.schema.field("metadata").type)

        data = table.to_pylist()
        # Verify the 3-level nesting survived serialization natively over Flight stream
        assert data[0]["metadata"]["preferences"][0]["name"] == "web"
        assert data[0]["metadata"]["preferences"][0]["theme"] == "dark"

    finally:
        process.terminate()
        process.wait(timeout=5)
