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


def test_ui_shell_is_served_without_embedding_admin_token():
    client = _client()

    response = client.get("/ui")

    assert response.status_code == 200
    assert "dal-obscura control plane" in response.text
    assert "test-admin" not in response.text
    assert "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN" not in response.text
    assert 'data-api-root="/v1"' in response.text


def test_ui_static_app_targets_v1_api_only_for_state():
    client = _client()

    response = client.get("/ui/static/app.js")

    assert response.status_code == 200
    assert "/v1" in response.text
    assert "/ui/actions" not in response.text
    assert "/ui/forms" not in response.text
    assert "ProvisioningService" not in response.text


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
