from __future__ import annotations

from typing import Any, cast

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.interfaces.api import create_app


def _client() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return TestClient(create_app(session_factory(engine), admin_token="test-admin"))


def test_control_plane_healthz_does_not_require_auth():
    client = _client()

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_control_plane_readyz_checks_database():
    client = _client()

    response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "checks": {"database": "ok"}}


def test_control_plane_readyz_returns_503_when_database_check_fails():
    client = TestClient(create_app(cast(Any, _broken_session_factory), admin_token="test-admin"))

    response = client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert "database" in response.json()["checks"]


def _broken_session_factory() -> _BrokenSession:
    return _BrokenSession()


class _BrokenSession:
    def __enter__(self) -> _BrokenSession:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: object) -> None:
        del statement
        raise RuntimeError("database unavailable")
