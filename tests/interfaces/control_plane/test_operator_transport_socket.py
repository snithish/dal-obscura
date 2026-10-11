"""Real HTTP qualification for operator response bounds and credential redirects."""

import threading
from contextlib import suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TypedDict

import pytest

from dal_obscura.interfaces.cli import operator

pytestmark = pytest.mark.socket


class ResponseState(TypedDict):
    payload: bytes
    requests: list[tuple[str, str | None]]


@pytest.fixture
def control_endpoint(monkeypatch):
    state: ResponseState = {"payload": b'{"plugins":[],"pairs":[],"states":[]}', "requests": []}
    monkeypatch.setenv("DAL_OBSCURA_DEPLOY_ADMIN_TOKEN", "sentinel-credential")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state["requests"].append((self.path, self.headers.get("Authorization")))
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/credential-trap")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            # Deliberately omit Content-Length: the client must bound actual reads.
            self.end_headers()
            with suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(state["payload"])

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield {"control_url": f"http://127.0.0.1:{server.server_port}"}, state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_operator_reads_authenticated_inventory_over_real_http(control_endpoint):
    target, state = control_endpoint
    assert operator.api_request(target, "GET", "/v1/plugins") == {
        "plugins": [],
        "pairs": [],
        "states": [],
    }
    assert state["requests"] == [("/v1/plugins", "Bearer sentinel-credential")]


def test_operator_rejects_real_unbounded_http_body(control_endpoint):
    target, state = control_endpoint
    state["payload"] = b'{"plugins":[]}' + b" " * (8 * 1024 * 1024)
    with pytest.raises(ValueError, match="Control API response exceeds"):
        operator.api_request(target, "GET", "/v1/plugins")


def test_operator_redirect_never_reaches_credential_destination(control_endpoint):
    target, state = control_endpoint
    with pytest.raises(ValueError, match="redirects are not allowed"):
        operator.api_request(target, "GET", "/redirect")
    assert state["requests"] == [("/redirect", "Bearer sentinel-credential")]
