"""Consumer qualification for the Python and DuckDB read surfaces."""

from __future__ import annotations

import base64
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dal_obscura_iceberg_rest.catalog import DESCRIPTOR as REST_CATALOG_DESCRIPTOR
from dal_obscura_iceberg_rest.catalog import rest_catalog_factory
from dal_obscura_manifest_parquet.catalog import CATALOG_DESCRIPTOR, manifest_factory
from dal_obscura_manifest_parquet.format import FORMAT_DESCRIPTOR, parquet_factory
from pyiceberg.catalog import load_catalog

from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.connectors.python_sdk import DalObscuraClient, DuckDBDalObscuraReader
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from tests.support.arrow import metadata_schema
from tests.support.flight import (
    InMemoryPolicyAuthorizer,
    StubTableFormat,
    build_flight_service,
    make_jwt,
    running_flight_client,
)
from tests.support.iceberg import create_iceberg_table, iceberg_sql_catalog_options
from tests.support.policy import allow_rule

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def require_consumer_lane() -> None:
    if os.getenv("DAL_OBSCURA_RUN_CONSUMER_TESTS") != "1":
        pytest.skip("set DAL_OBSCURA_RUN_CONSUMER_TESTS=1 for the loopback consumer lane")


def test_python_and_duckdb_consumers_receive_identical_nested_governed_data() -> None:
    schema = metadata_schema()
    batch = pa.RecordBatch.from_pylist(
        [
            {
                "id": 1,
                "metadata": {
                    "preferences": [
                        {"name": "web", "theme": "dark"},
                        {"name": "mobile", "theme": "light"},
                    ]
                },
            },
            {
                "id": 2,
                "metadata": {"preferences": [{"name": "desktop", "theme": "dark"}]},
            },
        ],
        schema=schema,
    )
    table_format = StubTableFormat(
        catalog_name="analytics",
        table_name="consumer.nested",
        format="stub_format",
        schema=schema,
        batches=(batch,),
    )
    policy = [
        allow_rule(
            ["id", "metadata"],
            masks={"metadata.preferences.theme": {"type": "redact", "value": "[hidden]"}},
        )
    ]
    server = build_flight_service(table_format=table_format, policy_rules=policy)

    with running_flight_client(server) as flight_client:
        sdk = DalObscuraClient.from_flight_client(
            flight_client,
            auth_token=make_jwt("user1"),
        )
        arrow_table = sdk.read_table(
            catalog="analytics",
            target="consumer.nested",
            columns=["id", "metadata"],
        )
        with DuckDBDalObscuraReader(sdk) as reader:
            duckdb_row_count = (
                reader.relation(
                    catalog="analytics",
                    target="consumer.nested",
                    columns=["id", "metadata"],
                )
                .aggregate("count(*) AS rows")
                .fetchone()
            )

    assert arrow_table.schema == schema
    assert arrow_table.num_rows == 2
    assert arrow_table.column("metadata").to_pylist()[0]["preferences"][0]["theme"] == "[hidden]"
    assert duckdb_row_count == (2,)


def test_python_and_duckdb_consumers_read_real_sql_iceberg_nested_data(tmp_path) -> None:
    """Exercise the consumer contract against the executable Iceberg adapter.

    The regular consumer test intentionally uses a tiny in-memory format for
    fast feedback.  This opt-in lane uses the same nested fixture with a real
    SQL catalog, metadata files and parquet data so a passing result covers
    catalog resolution, file planning, Arrow execution and both consumers.
    """

    schema = metadata_schema()
    table = pa.table(
        {
            "id": [1, 2],
            "metadata": [
                {
                    "preferences": [
                        {"name": "web", "theme": "dark"},
                        {"name": "mobile", "theme": "light"},
                    ]
                },
                {"preferences": [{"name": "desktop", "theme": "dark"}]},
            ],
        },
        schema=schema,
    )
    target = create_iceberg_table(
        tmp_path,
        "analytics",
        "warehouse",
        identifier="default.consumer_nested",
        arrow_schema=schema,
        append_tables=[table],
    )
    registry = CatalogRegistry(
        ServiceConfig(
            catalogs={
                "analytics": CatalogConfig(
                    name="analytics",
                    type="iceberg",
                    options=iceberg_sql_catalog_options(tmp_path, "analytics", "warehouse"),
                )
            }
        )
    )
    policy = [
        allow_rule(
            ["id", "metadata"],
            masks={"metadata.preferences.theme": {"type": "redact", "value": "[hidden]"}},
        )
    ]
    server = build_flight_service(
        catalog_registry=registry,
        authorizer=InMemoryPolicyAuthorizer(catalog="analytics", target=target, rules=policy),
    )

    try:
        with running_flight_client(server) as flight_client:
            sdk = DalObscuraClient.from_flight_client(
                flight_client,
                auth_token=make_jwt("user1"),
            )
            arrow_table = sdk.read_table(
                catalog="analytics",
                target=target,
                columns=["id", "metadata"],
            )
            with DuckDBDalObscuraReader(sdk) as reader:
                duckdb_row_count = (
                    reader.relation(
                        catalog="analytics",
                        target=target,
                        columns=["id", "metadata"],
                    )
                    .aggregate("count(*) AS rows")
                    .fetchone()
                )
    finally:
        registry.close()

    assert arrow_table.schema == schema
    assert arrow_table.num_rows == 2
    assert arrow_table.column("metadata").to_pylist()[0]["preferences"][0]["theme"] == "[hidden]"
    assert duckdb_row_count == (2,)


def test_python_and_duckdb_consumers_read_real_manifest_parquet_nested_data(tmp_path: Path) -> None:
    """Exercise the public manifest/Parquet pair through the governed bridge."""

    root = tmp_path / "dataset"
    root.mkdir()
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("profile", pa.struct([pa.field("email", pa.string())])),
        ]
    )
    table = pa.table(
        {
            "id": [1, 2],
            "profile": [{"email": "a@example.com"}, {"email": "b@example.com"}],
        },
        schema=schema,
    )
    pq.write_table(table.slice(0, 1), root / "part-0.parquet", row_group_size=1)
    pq.write_table(table.slice(1), root / "part-1.parquet", row_group_size=1)
    manifest = root / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "revision": "snapshot-1",
                "tables": {
                    "default.users": {
                        "files": ["part-0.parquet", "part-1.parquet"],
                        "schema_ipc": base64.b64encode(schema.serialize().to_pybytes()).decode(
                            "ascii"
                        ),
                        "field_ids": ["id", "profile"],
                    }
                },
            }
        )
    )
    plugin_registry = PluginRegistry(
        builtins={
            ("catalog", "manifest"): (CATALOG_DESCRIPTOR, manifest_factory),
            ("table_format", "parquet.dataset"): (FORMAT_DESCRIPTOR, parquet_factory),
        }
    )
    plugin_registry.reload()
    registry = CatalogRegistry(
        ServiceConfig(
            catalogs={
                "datasets": CatalogConfig(
                    name="datasets",
                    type="plugin",
                    plugin_id="manifest",
                    revision=1,
                    options={"root": str(root), "manifest_path": "manifest.json"},
                )
            }
        ),
        plugin_registry=plugin_registry,
    )
    target = "default.users"
    policy = [
        allow_rule(
            ["id", "profile"],
            masks={"profile.email": {"type": "redact", "value": "[hidden]"}},
        )
    ]
    server = build_flight_service(
        catalog_registry=registry,
        authorizer=InMemoryPolicyAuthorizer(catalog="datasets", target=target, rules=policy),
        max_tickets=2,
    )

    try:
        with running_flight_client(server) as flight_client:
            sdk = DalObscuraClient.from_flight_client(
                flight_client,
                auth_token=make_jwt("user1"),
            )
            arrow_table = sdk.read_table(
                catalog="datasets",
                target=target,
                columns=["id", "profile"],
            )
            with DuckDBDalObscuraReader(sdk) as reader:
                duckdb_rows = (
                    reader.relation(
                        catalog="datasets",
                        target=target,
                        columns=["id", "profile"],
                    )
                    .order("id")
                    .fetchall()
                )
    finally:
        registry.close()

    assert arrow_table.schema == schema
    assert arrow_table.to_pylist() == [
        {"id": 1, "profile": {"email": "[hidden]"}},
        {"id": 2, "profile": {"email": "[hidden]"}},
    ]
    assert duckdb_rows == [(1, {"email": "[hidden]"}), (2, {"email": "[hidden]"})]


def test_python_and_duckdb_consumers_read_real_rest_iceberg_nested_data(tmp_path: Path) -> None:
    """Exercise REST catalog resolution with a real Iceberg table response."""

    target = create_iceberg_table(
        tmp_path,
        "rest_catalog",
        "warehouse",
        identifier="default.users",
        values=[1, 2],
    )
    sql_catalog = load_catalog(
        "rest_catalog",
        type="sql",
        uri=f"sqlite:///{tmp_path / 'rest_catalog.db'}",
        warehouse=str(tmp_path / "warehouse"),
    )
    metadata_location = str(sql_catalog.load_table(target).metadata_location)
    metadata_path = Path(metadata_location.removeprefix("file://"))
    table_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    requests: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append(self.path)
            path = urlsplit(self.path).path
            if path == "/v1/config":
                payload = {"defaults": {}, "overrides": {}, "endpoints": []}
            elif path == "/v1/namespaces":
                payload = {"namespaces": [["default"]]}
            elif path == "/v1/namespaces/default/tables/users":
                payload = {"metadata-location": metadata_location, "metadata": table_metadata}
            else:
                self.send_error(404)
                return
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    http_server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    http_thread = Thread(target=http_server.serve_forever, daemon=True)
    http_thread.start()
    plugin_registry = PluginRegistry(
        builtins={("catalog", "iceberg.rest"): (REST_CATALOG_DESCRIPTOR, rest_catalog_factory)},
    )
    plugin_registry.reload()
    registry = CatalogRegistry(
        ServiceConfig(
            catalogs={
                "rest": CatalogConfig(
                    name="rest",
                    type="plugin",
                    plugin_id="iceberg.rest",
                    revision=1,
                    options={"uri": f"http://127.0.0.1:{http_server.server_port}"},
                )
            }
        ),
        plugin_registry=plugin_registry,
    )
    resolved = registry.resolve("rest", target)
    assert resolved.get_schema().names == ["id", "email", "region"]
    policy = [allow_rule(["id", "email"], masks={"email": {"type": "email"}})]
    server = build_flight_service(
        catalog_registry=registry,
        authorizer=InMemoryPolicyAuthorizer(catalog="rest", target=target, rules=policy),
        max_tickets=2,
    )

    try:
        with running_flight_client(server) as flight_client:
            sdk = DalObscuraClient.from_flight_client(
                flight_client,
                auth_token=make_jwt("user1"),
            )
            arrow_table = sdk.read_table(catalog="rest", target=target, columns=["id", "email"])
            with DuckDBDalObscuraReader(sdk) as reader:
                duckdb_rows = (
                    reader.relation(catalog="rest", target=target, columns=["id", "email"])
                    .order("id")
                    .fetchall()
                )
    finally:
        registry.close()
        http_server.shutdown()
        http_server.server_close()
        http_thread.join(timeout=2)

    assert arrow_table.num_rows == 2
    assert arrow_table.column("email").to_pylist() == ["u***@example.com", "u***@example.com"]
    assert duckdb_rows == [(1, "u***@example.com"), (2, "u***@example.com")]
    assert "/v1/config" in requests
    assert "/v1/namespaces/default/tables/users" in requests
