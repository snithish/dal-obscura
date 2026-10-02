"""Real, disposable backend fixtures for the shared consumer contract."""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
from dal_obscura_iceberg_rest.catalog import DESCRIPTOR as REST_DESCRIPTOR
from dal_obscura_iceberg_rest.catalog import rest_catalog_factory
from dal_obscura_manifest_parquet.catalog import CATALOG_DESCRIPTOR, manifest_factory
from dal_obscura_manifest_parquet.format import FORMAT_DESCRIPTOR, parquet_factory
from pyiceberg.catalog import load_catalog

from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from dal_obscura.data_plane.infrastructure.adapters.iceberg_format_plugin import IcebergFormatPlugin
from tests.support.arrow import metadata_schema
from tests.support.flight import InMemoryPolicyAuthorizer, StubTableFormat, build_flight_service
from tests.support.iceberg import create_iceberg_table, iceberg_sql_catalog_options
from tests.support.policy import allow_rule


def nested_consumer_table() -> pa.Table:
    return pa.Table.from_pylist(
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
            {"id": 2, "metadata": {"preferences": [{"name": "desktop", "theme": "dark"}]}},
        ],
        schema=metadata_schema(),
    )


def plugin_registry(*registrations) -> PluginRegistry:
    registry = PluginRegistry(builtins=dict(registrations))
    registry.reload()
    return registry


def catalog_registry(options, *, plugin_id=None, plugins=None) -> CatalogRegistry:
    return CatalogRegistry(
        ServiceConfig(
            catalogs={
                "analytics": CatalogConfig(
                    name="analytics",
                    type="plugin" if plugin_id else "iceberg",
                    plugin_id=plugin_id or "iceberg.sql",
                    revision=1,
                    options=options,
                )
            }
        ),
        plugin_registry=plugins,
    )


@contextmanager
def manifest_registry(directory: Path, table: pa.Table) -> Iterator[CatalogRegistry]:
    root = directory / "dataset"
    root.mkdir()
    pq.write_table(table.slice(0, 1), root / "part-0.parquet", row_group_size=1)
    pq.write_table(table.slice(1), root / "part-1.parquet", row_group_size=1)
    manifest = root / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "revision": "snapshot-1",
                "tables": {
                    "default.users": {
                        "namespace": ["default"],
                        "name": "users",
                        "files": ["part-0.parquet", "part-1.parquet"],
                        "schema_ipc": base64.b64encode(
                            table.schema.serialize().to_pybytes()
                        ).decode("ascii"),
                        "field_ids": ["id", "metadata"],
                    }
                },
            }
        )
    )
    plugins = plugin_registry(
        (("catalog", "manifest"), (CATALOG_DESCRIPTOR, manifest_factory)),
        (("table_format", "parquet.dataset"), (FORMAT_DESCRIPTOR, parquet_factory)),
    )
    registry = catalog_registry(
        {"root": str(root), "manifest_path": str(manifest)}, plugin_id="manifest", plugins=plugins
    )
    try:
        yield registry
    finally:
        registry.close()


@contextmanager
def sql_registry(directory: Path, table: pa.Table) -> Iterator[CatalogRegistry]:
    create_iceberg_table(
        directory, "analytics", "warehouse", arrow_schema=table.schema, append_tables=[table]
    )
    registry = catalog_registry(iceberg_sql_catalog_options(directory, "analytics", "warehouse"))
    try:
        yield registry
    finally:
        registry.close()


@contextmanager
def rest_registry(directory: Path, table: pa.Table) -> Iterator[CatalogRegistry]:
    create_iceberg_table(
        directory, "analytics", "warehouse", arrow_schema=table.schema, append_tables=[table]
    )
    catalog = load_catalog(
        "analytics",
        type="sql",
        uri=f"sqlite:///{directory / 'analytics.db'}",
        warehouse=str(directory / "warehouse"),
    )
    metadata_location = str(catalog.load_table("default.users").metadata_location)
    table_metadata = json.loads(Path(metadata_location.removeprefix("file://")).read_text())

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlsplit(self.path).path
            responses = {
                "/v1/config": {"defaults": {}, "overrides": {}, "endpoints": []},
                "/v1/namespaces": {"namespaces": [["default"]]},
                "/v1/namespaces/default/tables/users": {
                    "metadata-location": metadata_location,
                    "metadata": table_metadata,
                },
            }
            if path not in responses:
                self.send_error(404)
                return
            body = json.dumps(responses[path]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=http.serve_forever, daemon=True)
    thread.start()
    try:
        plugins = plugin_registry(
            (("catalog", "iceberg.rest"), (REST_DESCRIPTOR, rest_catalog_factory)),
            (("table_format", "iceberg"), (IcebergFormatPlugin.descriptor, IcebergFormatPlugin)),
        )
        registry = catalog_registry(
            {"uri": f"http://127.0.0.1:{http.server_port}"},
            plugin_id="iceberg.rest",
            plugins=plugins,
        )
        try:
            yield registry
        finally:
            registry.close()
    finally:
        http.shutdown()
        http.server_close()
        thread.join(timeout=2)


@contextmanager
def consumer_server(backend: str, directory: Path):
    table = nested_consumer_table()
    policy = [
        allow_rule(
            ["id", "metadata"],
            masks={
                "metadata.preferences.$element.theme": {"type": "redact", "value": "[hidden]"},
            },
        )
    ]
    if backend == "memory":
        yield build_flight_service(
            table_format=StubTableFormat(
                catalog_name="analytics",
                table_name="default.users",
                format="stub_format",
                schema=table.schema,
                batches=tuple(table.to_batches()),
            ),
            policy_rules=policy,
        )
        return
    factory = {
        "iceberg.sql": sql_registry,
        "manifest.parquet": manifest_registry,
        "iceberg.rest": rest_registry,
    }[backend]
    with factory(directory, table) as registry:
        yield build_flight_service(
            catalog_registry=registry,
            authorizer=InMemoryPolicyAuthorizer(
                catalog="analytics", target="default.users", rules=policy
            ),
            max_tickets=2,
        )
