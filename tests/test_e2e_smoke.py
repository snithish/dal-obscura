import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any
from uuid import UUID

import jwt
import pyarrow as pa
import pyarrow.flight as flight
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    ListType,
    LongType,
    NestedField,
    StringType,
    StructType,
)

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.common.flight_contract import encode_plan_command
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.provisioning import ProvisioningService

pytestmark = pytest.mark.heavy


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_flight_server(
    process: subprocess.Popen[str], port: int, *, timeout: float = 30.0
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return False
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(0.2)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.1)
    return False


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
def oidc_jwks_server() -> Iterator[dict[str, str]]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_jwk = json.loads(RSAAlgorithm.to_jwk(private_key.public_key()))
    public_jwk["alg"] = "RS256"
    public_jwk["use"] = "sig"
    public_jwk["kid"] = "e2e"
    private_pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    jwks = {"keys": [public_jwk]}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != "/jwks.json":
                self.send_error(404)
                return
            payload = json.dumps(jwks).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: Any) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield {
            "url": f"http://127.0.0.1:{server.server_port}/jwks.json",
            "private_key": private_pem,
        }
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.fixture
def control_plane_setup(
    tmp_path: Path,
    iceberg_setup: tuple[str, Path],
    oidc_jwks_server: dict[str, str],
) -> dict[str, str]:
    catalog_uri, warehouse = iceberg_setup
    database_url = f"sqlite+pysqlite:///{tmp_path / 'control-plane.db'}"
    engine = create_engine_from_url(database_url)
    migrate_config_store(engine)

    with session_factory(engine)() as session:
        service = ProvisioningService(session)
        tenant = service.create_tenant(slug="default", display_name="Default")
        cell = service.create_cell(name="default", region="local")
        tenant_id = UUID(tenant["id"])
        cell_id = UUID(cell["id"])
        service.assign_tenant(
            cell_id=cell_id,
            tenant_id=tenant_id,
            shard_key="default",
        )
        service.upsert_runtime_settings(
            cell_id=cell_id,
            ttl=900,
            max_tickets=64,
            max_ticket_exchanges=1,
        )
        service.upsert_catalog(
            cell_id=cell_id,
            tenant_id=tenant_id,
            name="e2e_catalog",
            module="dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog",
            options={
                "type": "sql",
                "uri": catalog_uri,
                "warehouse": str(warehouse),
            },
        )
        asset = service.upsert_asset(
            cell_id=cell_id,
            tenant_id=tenant_id,
            catalog="e2e_catalog",
            target="default.users",
            backend="iceberg",
            table_identifier="default.users",
            options={},
        )
        service.replace_asset_schema_fields(
            asset_id=UUID(asset["id"]),
            fields=[
                {"name": "id", "field_id": "iceberg:1", "path": ["id"], "type": "int64"},
                {
                    "name": "email",
                    "field_id": "iceberg:2",
                    "path": ["email"],
                    "type": "large_string",
                },
                {
                    "name": "metadata",
                    "field_id": "iceberg:3",
                    "path": ["metadata"],
                    "type": (
                        "struct<preferences: large_list<element: struct<name: large_string, "
                        "theme: large_string, notifications: large_string>>>"
                    ),
                },
                {
                    "name": "metadata.preferences",
                    "field_id": "iceberg:4",
                    "path": ["metadata", "preferences"],
                    "type": (
                        "large_list<element: struct<name: large_string, theme: large_string, "
                        "notifications: large_string>>"
                    ),
                },
                {
                    "name": "metadata.preferences.$element",
                    "field_id": "iceberg:5",
                    "path": ["metadata", "preferences", "$element"],
                    "type": (
                        "struct<name: large_string, theme: large_string, "
                        "notifications: large_string>"
                    ),
                },
                {
                    "name": "metadata.preferences.$element.name",
                    "field_id": "iceberg:6",
                    "path": ["metadata", "preferences", "$element", "name"],
                    "type": "large_string",
                },
                {
                    "name": "metadata.preferences.$element.theme",
                    "field_id": "iceberg:7",
                    "path": ["metadata", "preferences", "$element", "theme"],
                    "type": "large_string",
                },
                {
                    "name": "metadata.preferences.$element.notifications",
                    "field_id": "iceberg:8",
                    "path": ["metadata", "preferences", "$element", "notifications"],
                    "type": "large_string",
                },
            ],
        )
        service.replace_policy_rules(
            asset_id=UUID(asset["id"]),
            rules=[
                {
                    "ordinal": 10,
                    "principals": ["e2e_user"],
                    "columns": ["id", "email", "metadata"],
                    "effect": "allow",
                    "when": {},
                    "masks": {},
                    "row_filter": None,
                }
            ],
            actor=ControlPlaneActor.for_platform_admin("test:setup"),
        )
        service.replace_asset_owners(
            asset_id=UUID(asset["id"]),
            owners=["user:e2e-owner@example.com"],
            expected_revision=0,
        )
        service.replace_auth_providers(
            cell_id=cell_id,
            providers=[
                {
                    "ordinal": 1,
                    "module": (
                        "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks."
                        "OidcJwksIdentityProvider"
                    ),
                    "args": {
                        "issuer": "https://issuer.example",
                        "algorithms": ["RS256"],
                        "jwks_url": oidc_jwks_server["url"],
                        "attribute_claims": {"tenant_id": "attributes.tenant_id"},
                    },
                    "enabled": True,
                }
            ],
        )
        publication = service.create_publication(cell_id=cell_id)
        service.activate_publication(
            cell_id=cell_id,
            publication_id=UUID(str(publication["publication_id"])),
        )
        session.commit()

    return {
        "database_url": database_url,
        "cell_id": cell["id"],
        "tenant_id": tenant["id"],
        "private_key": oidc_jwks_server["private_key"],
    }


def test_e2e_flight_server_with_iceberg(control_plane_setup: dict[str, str]):
    port = get_free_port()
    ticket_secret = "e2e-ticket-secret"

    env = dict(os.environ)
    env["DAL_OBSCURA_DATABASE_URL"] = control_plane_setup["database_url"]
    env["DAL_OBSCURA_CELL_ID"] = control_plane_setup["cell_id"]
    env["DAL_OBSCURA_LOCATION"] = f"grpc://0.0.0.0:{port}"
    env["DAL_OBSCURA_TICKET_SECRET"] = ticket_secret

    cmd = [
        sys.executable,
        "-c",
        "import sys; from dal_obscura.data_plane.interfaces.cli.main import main; sys.exit(main())",
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
            "dal_obscura.data_plane.interfaces.cli.main",
        ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    if not wait_for_flight_server(process, port):
        if process.poll() is None:
            process.terminate()
        stdout, stderr = process.communicate(timeout=5)
        raise RuntimeError(f"Server did not become ready:\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")

    client = None
    try:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Server exited early:\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")

        client = flight.FlightClient(f"grpc+tcp://localhost:{port}")

        # Authenticate with valid JWT built matching our server expectation
        token = jwt.encode(
            {
                "sub": "e2e_user",
                "attributes": {"tenant_id": control_plane_setup["tenant_id"]},
                "iss": "https://issuer.example",
                "exp": int(time.time()) + 900,
            },
            control_plane_setup["private_key"],
            algorithm="RS256",
            headers={"kid": "e2e"},
        )
        options = flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {token}".encode())])

        # Query the data
        descriptor = flight.FlightDescriptor.for_command(
            encode_plan_command(
                catalog="e2e_catalog",
                target="default.users",
                columns=["id", "email", "metadata"],
            )
        )

        try:
            info = client.get_flight_info(descriptor, options=options)
        except Exception as exc:
            process.terminate()
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"Flight request failed:\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
            ) from exc

        batches = []
        for endpoint in info.endpoints:
            reader = client.do_get(endpoint.ticket, options=options)
            batches.extend(reader.read_all().to_batches())

        table = pa.Table.from_batches(batches) if batches else pa.table({})

        assert table.num_rows == 2
        assert table.schema.field("id").type == pa.int64()
        assert table.schema.field("email").type == pa.string()
        assert pa.types.is_struct(table.schema.field("metadata").type)

        data = table.to_pylist()
        # Verify the 3-level nesting survived serialization natively over Flight stream
        assert data[0]["metadata"]["preferences"][0]["name"] == "web"
        assert data[0]["metadata"]["preferences"][0]["theme"] == "dark"

    finally:
        process.terminate()
        process.wait(timeout=5)
