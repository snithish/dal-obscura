from __future__ import annotations

import ast
import inspect

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.interfaces import ui
from dal_obscura.control_plane.interfaces.api import create_app

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)


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
    assert "/ui/static/vendor/htmx/2.0.4/htmx.min.js" in response.text
    assert 'hx-get="/v1/tenants"' in response.text


def test_ui_static_app_configures_htmx_auth_and_local_table_interactivity():
    client = _client()

    response = client.get("/ui/static/app.js")

    assert response.status_code == 200
    assert "htmx:configRequest" in response.text
    assert "Authorization" in response.text
    assert "dal-obscura-admin-token" in response.text
    assert "data-table-filter" in response.text
    assert "data-sort-key" in response.text
    assert "data-set-context" in response.text
    assert "hasTenantCellContext" in response.text
    assert "aria-disabled" in response.text
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


def test_ui_shell_splits_global_admin_from_tenant_policy_workspace():
    client = _client()

    shell = client.get("/ui").text

    expected_views = [
        "global-admin",
        "policy-workspace",
        "policy-editor",
        "catalogs",
        "add-catalog",
        "jobs",
    ]
    for view in expected_views:
        assert view in shell
    assert "Fleet Admin" in shell
    assert "Policy Center" in shell
    assert "Catalogs" in shell
    assert "Background Jobs" in shell


def test_ui_javascript_uses_v1_api_only_for_stateful_calls():
    client = _client()

    app_js = client.get("/ui/static/app.js").text

    assert "fetch(" not in app_js
    assert "XMLHttpRequest" not in app_js
    assert "/ui/" not in app_js.replace("/ui/static/app.js", "")


def test_ui_shell_exposes_htmx_calls_to_existing_v1_routes():
    client = _client()

    shell = client.get("/ui").text

    expected_routes = [
        'hx-get="/v1/tenants"',
        'hx-get="/v1/cell-tenant-assignments"',
        'data-list-template="/v1/tenants/{tenant_id}/cells"',
        'data-post-template="/v1/tenants/{tenant_id}/cells"',
        'data-post-template="/v1/tenants/{tenant_id}/cell-assignments"',
        'data-route-template="/v1/cells/{cell_id}/assets"',
        'data-route-template="/v1/cells/{cell_id}/catalogs"',
        'data-route-template="/v1/assets/{asset_id}/policy-rules"',
        'data-put-template="/v1/tenants/{tenant_id}/cells/{cell_id}/catalogs/{catalog_name}"',
    ]
    for route in expected_routes:
        assert route in shell


def test_ui_shell_exposes_tenant_and_cell_create_forms():
    client = _client()

    shell = client.get("/ui").text

    assert 'hx-post="/v1/tenants"' in shell
    assert 'name="slug"' in shell
    assert 'name="display_name"' in shell
    assert 'data-post-template="/v1/tenants/{tenant_id}/cells"' in shell
    assert 'name="name"' in shell
    assert 'name="region"' in shell
    assert 'id="tenants-list"' in shell
    assert 'id="cells-list"' in shell
    assert "setup-stepper" in shell
    assert "data-requires-context" in shell
    assert "lock-icon" in shell
    assert "Select a tenant and cell to unlock this section" in shell
    assert "Select a tenant and cell first" in shell
    assert "Assign existing cell" in shell
    assert "Save catalog" in shell
    assert "Background job runner is not wired yet" in shell


def test_ui_partials_render_tenants_cells_and_assignments_as_html():
    client = _client()
    tenant = client.post(
        "/v1/tenants",
        json={"slug": "default", "display_name": "Default"},
        headers={"authorization": "Bearer test-admin"},
    ).json()
    cell = client.post(
        "/v1/cells",
        json={"name": "default", "region": "local"},
        headers={"authorization": "Bearer test-admin"},
    ).json()
    client.put(
        f"/v1/cells/{cell['id']}/tenants/{tenant['id']}",
        json={"shard_key": "default"},
        headers={"authorization": "Bearer test-admin"},
    )

    tenants = client.get(
        "/v1/tenants",
        headers={"authorization": "Bearer test-admin", "HX-Request": "true"},
    )
    cells = client.get(
        f"/v1/tenants/{tenant['id']}/cells",
        headers={"authorization": "Bearer test-admin", "HX-Request": "true"},
    )
    assignments = client.get(
        "/v1/cell-tenant-assignments",
        headers={"authorization": "Bearer test-admin", "HX-Request": "true"},
    )

    assert "resource-table" in tenants.text
    assert 'data-set-context="tenant_id"' in tenants.text
    assert "Select default" in tenants.text
    assert "default" in tenants.text
    assert "resource-table" in cells.text
    assert 'data-set-context="cell_id"' in cells.text
    assert "Select default" in cells.text
    assert "local" in cells.text
    assert "Tenant cell assignments" in assignments.text
    assert "default" in assignments.text


def test_ui_can_create_tenant_and_cell_from_htmx_forms():
    client = _client()

    tenant_response = client.post(
        "/v1/tenants",
        data={"slug": "acme", "display_name": "Acme Analytics"},
        headers={"authorization": "Bearer test-admin", "HX-Request": "true"},
    )
    cell_response = client.post(
        "/v1/cells",
        data={"name": "local-dev", "region": "local"},
        headers={"authorization": "Bearer test-admin", "HX-Request": "true"},
    )

    assert tenant_response.status_code == 200
    assert "Acme Analytics" in tenant_response.text
    assert "resource-table" in tenant_response.text
    assert cell_response.status_code == 200
    assert "local-dev" in cell_response.text
    assert "resource-table" in cell_response.text


def test_ui_can_save_catalog_from_htmx_form():
    client = _client()
    tenant = client.post(
        "/v1/tenants",
        json={"slug": "default", "display_name": "Default"},
        headers={"authorization": "Bearer test-admin"},
    ).json()
    cell_response = client.post(
        f"/v1/tenants/{tenant['id']}/cells",
        data={"name": "local", "region": "local", "shard_key": "default"},
        headers={"authorization": "Bearer test-admin", "HX-Request": "true"},
    )
    cell_id = cell_response.text.split('data-context-value="')[1].split('"')[0]

    response = client.put(
        f"/v1/tenants/{tenant['id']}/cells/{cell_id}/catalogs/analytics",
        data={
            "module": ICEBERG_CATALOG_MODULE,
            "options": '{"type":"sql","uri":"sqlite:///catalog.db"}',
        },
        headers={"authorization": "Bearer test-admin", "HX-Request": "true"},
    )

    assert response.status_code == 200
    assert "analytics" in response.text
    assert "Refresh tables" in response.text


def test_ui_partials_handle_null_runtime_settings():
    client = _client()

    response = client.get(
        "/v1/cells/00000000-0000-0000-0000-000000000001/runtime-settings",
        headers={"HX-Request": "true", "authorization": "Bearer test-admin"},
    )

    assert response.status_code == 200
    assert "No runtime settings" in response.text


def test_ui_partials_render_catalog_management_flows():
    client = _client()
    tenant = client.post(
        "/v1/tenants",
        json={"slug": "default", "display_name": "Default"},
        headers={"authorization": "Bearer test-admin"},
    ).json()
    cell = client.post(
        "/v1/cells",
        json={"name": "default", "region": "local"},
        headers={"authorization": "Bearer test-admin"},
    ).json()
    client.put(
        f"/v1/tenants/{tenant['id']}/cells/{cell['id']}/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers={"authorization": "Bearer test-admin"},
    )

    response = client.get(
        f"/v1/cells/{cell['id']}/catalogs",
        headers={"authorization": "Bearer test-admin", "HX-Request": "true"},
    )

    assert response.status_code == 200
    assert "analytics" in response.text
    assert "Refresh tables" in response.text
    assert "Remove catalog" in response.text
    assert "Recently discovered" in response.text
    assert "data-table-filter" in response.text
    assert "data-route-template" not in response.text


def test_ui_partials_use_reusable_design_components():
    source = inspect.getsource(ui)

    reusable_components = [
        "viewHeader(",
        "tabs(",
        "statStrip(",
        "card(",
        "badge(",
        "actionBar(",
        "tableSection(",
    ]
    for component in reusable_components:
        assert component in source
