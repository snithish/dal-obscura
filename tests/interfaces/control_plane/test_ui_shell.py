from __future__ import annotations

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.interfaces.api import create_app


def _client() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return TestClient(create_app(session_factory(engine), admin_token="test-admin"))


def test_ui_serves_react_index_without_embedding_admin_token():
    client = _client()

    response = client.get("/ui")

    assert response.status_code == 200
    assert '<div id="root">' in response.text
    assert "dal-obscura control plane" in response.text
    assert "test-admin" not in response.text
    assert "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN" not in response.text
    assert "htmx" not in response.text.lower()
    assert "tenant" not in response.text.lower()
    assert "cell" not in response.text.lower()


def test_ui_client_routes_fall_back_to_react_index():
    client = _client()

    response = client.get("/ui/assets/example")

    assert response.status_code == 200
    assert '<div id="root">' in response.text
    assert "dal-obscura control plane" in response.text
