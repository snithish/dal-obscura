from __future__ import annotations

import ast
import inspect

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.interfaces import ui
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


def test_ui_static_asset_route_is_reserved_for_react_build_assets():
    client = _client()

    response = client.get("/ui/static/index.js")

    assert response.status_code in {200, 404}
    assert response.status_code != 500


def test_ui_module_does_not_import_application_or_repository_layers():
    forbidden_prefixes = (
        "dal_obscura.control_plane.application",
        "dal_obscura.control_plane.infrastructure",
        "dal_obscura.common.config_store",
    )
    tree = ast.parse(inspect.getsource(ui))
    imported_modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)
        elif isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)

    assert not [module for module in imported_modules if module.startswith(forbidden_prefixes)]


def test_v1_routes_ignore_hx_request_and_return_json():
    client = _client()

    response = client.get(
        "/v1/tenants",
        headers={"authorization": "Bearer test-admin", "HX-Request": "true"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.json() == []
