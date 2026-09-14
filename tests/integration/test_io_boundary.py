from __future__ import annotations

from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlsplit

import pytest
from dal_obscura_iceberg_rest.catalog import RestCatalog
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext

from dal_obscura.control_plane.application.catalog_service import validate_catalog_options
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer


def _context() -> ExecutionContext:
    return ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(minutes=1),
        correlation_id="io-boundary",
    )


def _rest_config(**options: object) -> CatalogConfig:
    return CatalogConfig(
        plugin_id="iceberg.rest",
        instance_id="io-boundary",
        revision=1,
        options={"uri": "https://catalog.example/v1", **options},
    )


def test_storage_paths_cannot_escape_a_published_uri_root() -> None:
    enforcer = PathRuleEnforcer([{"root": "s3://warehouse/curated"}])

    enforcer.check("s3://warehouse/curated/orders/part-0.parquet")
    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("s3://warehouse/curated/../secrets/credentials.json")
    with pytest.raises(PermissionError, match="Path is not allowed"):
        enforcer.check("s3://other-bucket/curated/orders/part-0.parquet")


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"uri": "https://user:pass@catalog.example/v1"}, "credentials"),
        ({"uri": "https://catalog.example/v1?access_token=leak"}, "query"),
        ({"uri": "https://other.example/v1"}, "outside"),
    ],
)
def test_catalog_options_fail_closed_at_the_egress_boundary(
    options: dict[str, str], message: str
) -> None:
    with pytest.raises(ValidationFailure, match=message):
        validate_catalog_options(options, egress_allowlist=("catalog.example",))


def test_rest_plugin_rejects_insecure_authenticated_endpoint_before_provider_setup() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        RestCatalog(_rest_config(uri="http://catalog.example/v1", token="resolved"), _context())


def test_rest_plugin_rejects_credential_bearing_auxiliary_uri() -> None:
    with pytest.raises(ValueError, match="warehouse"):
        RestCatalog(
            _rest_config(warehouse="s3://user:password@warehouse.example/root"),
            _context(),
        )


def test_rest_plugin_accepts_local_file_warehouse_uri() -> None:
    catalog = RestCatalog(_rest_config(warehouse="file:///tmp/warehouse"), _context())
    catalog.close()


def test_rest_plugin_rejects_file_uri_authority() -> None:
    with pytest.raises(ValueError, match="file URI must be local"):
        RestCatalog(_rest_config(warehouse="file://localhost/tmp/warehouse"), _context())


def test_rest_plugin_rejects_malformed_catalog_port() -> None:
    with pytest.raises(ValueError, match="invalid port"):
        RestCatalog(_rest_config(uri="https://catalog.example:not-a-port/v1"), _context())


def test_rest_plugin_does_not_follow_redirect_to_new_destination() -> None:
    denied_hits = 0

    class DeniedHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            nonlocal denied_hits
            denied_hits += 1
            self.send_response(200)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    denied_server = ThreadingHTTPServer(("127.0.0.1", 0), DeniedHandler)
    denied_thread = Thread(target=denied_server.serve_forever, daemon=True)
    denied_thread.start()
    source_paths: list[str] = []

    class SourceHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            source_paths.append(urlsplit(self.path).path)
            self.send_response(307)
            self.send_header(
                "Location",
                f"http://127.0.0.1:{denied_server.server_port}/v1/secret",
            )
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    source_server = ThreadingHTTPServer(("127.0.0.1", 0), SourceHandler)
    source_thread = Thread(target=source_server.serve_forever, daemon=True)
    source_thread.start()
    catalog = RestCatalog(
        _rest_config(uri=f"http://127.0.0.1:{source_server.server_port}"),
        _context(),
    )
    try:
        with pytest.raises(ValueError):
            catalog.list_namespaces(_context())
    finally:
        catalog.close()
        source_server.shutdown()
        source_server.server_close()
        source_thread.join(timeout=2)
        denied_server.shutdown()
        denied_server.server_close()
        denied_thread.join(timeout=2)

    assert source_paths == ["/v1/config"]
    assert denied_hits == 0
