from __future__ import annotations

import logging
import socket
from typing import cast

import pytest

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.data_plane.infrastructure.adapters.runtime_config import DataPlaneRuntimeConfig
from dal_obscura.data_plane.interfaces.cli.main import _start_health_server
from dal_obscura.data_plane.interfaces.flight.server import _health_payload
from dal_obscura.data_plane.interfaces.health import create_health_app, live_runtime_readiness


def test_data_plane_healthz_reports_process_alive(
    http_client,
):
    app = create_health_app(readiness=lambda: {"status": "ready"})
    client = http_client(app)

    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}


def test_data_plane_readyz_returns_503_when_runtime_not_loaded(
    http_client,
):
    app = create_health_app(readiness=lambda: {"status": "not_ready", "reason": "no runtime"})
    client = http_client(app)

    response = client.get("/readyz")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready", "reason": "no runtime"}


def test_data_plane_readyz_redacts_readiness_exceptions(
    http_client,
):
    app = create_health_app(
        readiness=lambda: (_ for _ in ()).throw(
            RuntimeError("postgres://user:secret@db.internal/path")
        )
    )

    response = http_client(app).get("/readyz")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready", "reason": "readiness check failed"}
    assert "secret" not in response.text


def test_flight_health_logs_do_not_include_provider_exception_details(caplog):
    secret = "sentinel-health-provider-secret"

    def readiness():
        raise RuntimeError(f"catalog URI contains {secret}")

    with caplog.at_level(logging.WARNING), pytest.raises(Exception) as error:
        _health_payload(readiness, logging.getLogger("test-flight-health"))

    assert str(error.value) == "Data plane is not ready"
    assert secret not in caplog.text


def test_data_plane_runtime_readiness_requires_active_auth_chain():
    readiness = live_runtime_readiness(_RuntimeStore(auth_chain={"providers": []}))
    checks = cast(dict[str, str], readiness["checks"])

    assert readiness["status"] == "not_ready"
    assert "configuration_revision" not in checks
    assert checks["auth_chain"] == "missing_enabled_provider"


def test_data_plane_runtime_readiness_requires_ticket_settings():
    readiness = live_runtime_readiness(
        _RuntimeStore(auth_chain={"providers": [{"module": "auth.Provider"}]}, ticket={})
    )
    checks = cast(dict[str, str], readiness["checks"])

    assert readiness["status"] == "not_ready"
    assert checks["runtime"] == "missing_ticket_settings"


@pytest.mark.socket
def test_data_plane_health_server_bind_failure_blocks_startup():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        port = occupied.getsockname()[1]
        engine = create_engine_from_url("sqlite+pysqlite:///:memory:")

        with pytest.raises(OSError):
            _start_health_server(
                session_factory(engine),
                DataPlaneRuntimeConfig(
                    database_url="sqlite+pysqlite:///:memory:",
                    location="grpc://127.0.0.1:8815",
                    ticket_secret="ticket-secret",
                    health_port=port,
                ),
            )


class _RuntimeStore:
    def __init__(
        self,
        *,
        auth_chain: dict[str, object],
        ticket: dict[str, object] | None = None,
    ) -> None:
        self._auth_chain = auth_chain
        default_ticket: dict[str, object] = {"ttl_seconds": 300, "max_tickets": 1}
        self._ticket = default_ticket if ticket is None else ticket

    def get_runtime(self) -> _Runtime:
        return _Runtime(auth_chain=self._auth_chain, ticket=self._ticket)


def test_live_runtime_readiness_redacts_store_exceptions():
    class BrokenStore:
        def get_runtime(self) -> object:
            raise RuntimeError("postgres://user:secret@db.internal/path")

    readiness = live_runtime_readiness(BrokenStore())

    assert readiness["reason"] == "live runtime unavailable"
    assert "secret" not in str(readiness)


class _Runtime:
    def __init__(
        self,
        *,
        auth_chain: dict[str, object],
        ticket: dict[str, object],
    ) -> None:
        self.auth_chain = auth_chain
        self.ticket = ticket
