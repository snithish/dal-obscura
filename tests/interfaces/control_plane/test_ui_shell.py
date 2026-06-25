from __future__ import annotations

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.interfaces.api import create_app


def _client() -> TestClient:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return TestClient(create_app(session_factory(engine), admin_token="test-admin"))


def test_swagger_docs_are_exposed_without_auth() -> None:
    client = _client()

    response = client.get("/docs")

    assert response.status_code == 200
    assert "Swagger UI" in response.text
    assert "test-admin" not in response.text


def test_openapi_schema_describes_control_plane_api() -> None:
    client = _client()

    response = client.get("/openapi.json")

    assert response.status_code == 200
    payload = response.json()
    assert payload["info"]["title"] == "dal-obscura control-plane API"
    assert "/v1/assets" in payload["paths"]
    assert "/v1/ui-auth-config" in payload["paths"]


def test_ui_paths_do_not_mask_missing_api_routes() -> None:
    client = _client()

    response = client.get("/ui/assets/example")

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/json")
    assert '<div id="root">' not in response.text
