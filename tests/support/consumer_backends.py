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
from dal_obscura_delta.catalog import CATALOG_DESCRIPTOR as DELTA_CATALOG_DESCRIPTOR
from dal_obscura_delta.catalog import delta_catalog_factory
from dal_obscura_delta.format import FORMAT_DESCRIPTOR as DELTA_FORMAT_DESCRIPTOR
from dal_obscura_delta.format import delta_format_factory
from dal_obscura_iceberg_rest.catalog import DESCRIPTOR as REST_DESCRIPTOR
from dal_obscura_iceberg_rest.catalog import rest_catalog_factory
from dal_obscura_manifest_parquet.catalog import CATALOG_DESCRIPTOR, manifest_factory
from dal_obscura_manifest_parquet.format import FORMAT_DESCRIPTOR, parquet_factory
from deltalake import WriterProperties, write_deltalake
from pyiceberg.catalog import load_catalog

from dal_obscura.sources.builtins import create_builtin_plugin_registry
from dal_obscura.sources.catalogs import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from dal_obscura.sources.iceberg_plugin import IcebergFormatPlugin
from dal_obscura.sources.plugins import PluginRegistry
from dal_obscura.sources.task_codec import SourceTaskCodec
from tests.support.arrow import metadata_schema
from tests.support.delta import install_deletion_vector
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
                    plugin_id=plugin_id or "iceberg.sql",
                    revision=1,
                    options=options,
                )
            }
        ),
        plugin_registry=plugins,
    )


@contextmanager
def manifest_registry(
    directory: Path, table: pa.Table
) -> Iterator[tuple[CatalogRegistry, PluginRegistry]]:
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
        yield registry, plugins
    finally:
        registry.close()


@contextmanager
def delta_registry(directory: Path, table: pa.Table):
    path = directory / "sales"
    # Forty physical rows, only the first two survive the real deletion vector.
    physical = pa.concat_tables([table] + [table.slice(0, 1)] * 38).combine_chunks()
    write_deltalake(
        path,
        physical,
        configuration={"delta.enableDeletionVectors": "true"},
        writer_properties=WriterProperties(max_row_group_size=1),
    )
    install_deletion_vector(path, range(2, 40), storage="u")
    membership = directory / "delta-tables.json"
    membership.write_text(
        json.dumps([{"namespace": ["default"], "name": "users", "path": "sales"}])
    )
    plugins = plugin_registry(
        (("catalog", "delta.directory"), (DELTA_CATALOG_DESCRIPTOR, delta_catalog_factory)),
        (("table_format", "delta"), (DELTA_FORMAT_DESCRIPTOR, delta_format_factory)),
    )
    registry = catalog_registry(
        {"root": str(directory), "tables_path": str(membership)},
        plugin_id="delta.directory",
        plugins=plugins,
    )
    try:
        yield registry, plugins
    finally:
        registry.close()


@contextmanager
def sql_registry(
    directory: Path, table: pa.Table
) -> Iterator[tuple[CatalogRegistry, PluginRegistry]]:
    create_iceberg_table(
        directory, "analytics", "warehouse", arrow_schema=table.schema, append_tables=[table]
    )
    plugins = create_builtin_plugin_registry()
    registry = catalog_registry(
        iceberg_sql_catalog_options(directory, "analytics", "warehouse"), plugins=plugins
    )
    try:
        yield registry, plugins
    finally:
        registry.close()


@contextmanager
def rest_registry(
    directory: Path, table: pa.Table
) -> Iterator[tuple[CatalogRegistry, PluginRegistry]]:
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
            yield registry, plugins
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
        "delta.directory": delta_registry,
    }[backend]
    with factory(directory, table) as (registry, plugins):
        yield build_flight_service(
            catalog_registry=registry,
            authorizer=InMemoryPolicyAuthorizer(
                catalog="analytics", target="default.users", rules=policy
            ),
            max_tickets=2,
            task_codec=SourceTaskCodec(plugins),
        )
