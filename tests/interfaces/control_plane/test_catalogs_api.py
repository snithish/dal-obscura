from __future__ import annotations

from fastapi.testclient import TestClient

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.control_plane.interfaces.api import create_app
from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_MODULE,
    _client,
    _keys_recursive,
)


def test_workspace_catalog_upsert_bootstraps_default_workspace():
    client = _client()

    response = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    catalogs = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
    summary = client.get("/v1/workspace/summary", headers=ADMIN_HEADERS).json()

    assert response.status_code == 200
    assert response.json() == {"id": response.json()["id"], "name": "analytics"}
    assert catalogs == [
        {
            "id": response.json()["id"],
            "name": "analytics",
            "module": ICEBERG_CATALOG_MODULE,
            "plugin_id": "iceberg.sql",
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
            "status": "configured",
            "revision": 0,
            "discovered_table_count": 0,
            "governed_asset_count": 0,
        }
    ]
    assert summary["catalog_count"] == 1
    assert summary["asset_count"] == 0
    assert summary["runtime_configured"] is False
    assert summary["enabled_auth_provider_count"] == 0
    events = client.get("/v1/audit/events", headers=ADMIN_HEADERS).json()
    catalog_events = [event for event in events if event["action"] == "workspace.catalog.update"]
    assert catalog_events[0]["actor"] == "platform:admin"
    assert catalog_events[0]["details"] == {
        "name": "analytics",
        "module": ICEBERG_CATALOG_MODULE,
        "option_keys": ["type", "uri"],
    }


def test_workspace_catalog_upsert_accepts_plugin_defaults_without_backend_fields():
    client = _client()

    response = client.put(
        "/v1/catalogs/analytics",
        json={"module": "iceberg.sql", "options": {"uri": "sqlite:///catalog.db"}},
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 200
    catalog = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()[0]
    assert catalog["module"] == "iceberg.sql"
    assert catalog["plugin_id"] == "iceberg.sql"
    assert catalog["options"] == {"uri": "sqlite:///catalog.db"}


def test_workspace_catalog_upsert_rejects_a_stale_revision():
    client = _client()
    first = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    assert first.status_code == 200
    updated = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog-new.db"},
            "expected_revision": 0,
        },
        headers=ADMIN_HEADERS,
    )
    stale = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog-stale.db"},
            "expected_revision": 0,
        },
        headers=ADMIN_HEADERS,
    )
    assert updated.status_code == 200
    assert stale.status_code == 409
    assert "Catalog revision changed" in stale.json()["detail"]
    assert stale.json()["error"]["current_revision"] == 1


def test_workspace_catalog_update_requires_revision_precondition():
    client = _client()
    created = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    assert created.status_code == 200

    missing = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog-new.db"},
        },
        headers=ADMIN_HEADERS,
    )

    assert missing.status_code == 428
    assert client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()[0]["revision"] == 0


def test_workspace_catalog_rejects_non_iceberg_module():
    client = _client()

    response = client.put(
        "/v1/catalogs/analytics",
        json={"module": "example.CustomCatalog", "options": {}},
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 422


def test_workspace_catalog_rejects_credentials_embedded_in_uri():
    client = _client()

    response = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"uri": "https://catalog-user:catalog-password@catalog.example/api"},
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert "secret reference" in response.json()["detail"]


def test_workspace_catalog_rejects_nested_dynamic_loader_options():
    client = _client()

    response = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"properties": {"py-catalog-impl": "example.CustomCatalog"}},
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert "cannot select an implementation class" in response.json()["detail"]


def test_workspace_catalog_rejects_unbounded_option_shape():
    client = _client()

    deeply_nested: object = "value"
    for _ in range(18):
        deeply_nested = {"nested": deeply_nested}
    response = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"properties": deeply_nested},
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert "too deeply nested" in response.json()["detail"]


def test_workspace_catalog_rejects_oversized_option_list():
    client = _client()
    response = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"properties": ["x"] * 257},
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert "list is too large" in response.json()["detail"]


def test_workspace_catalog_rejects_inline_sensitive_options_but_accepts_secret_refs():
    client = _client()

    rejected = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"password": "inline-password"},
        },
        headers=ADMIN_HEADERS,
    )
    accepted = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"password": {"secret": "CATALOG_PASSWORD", "scope": "catalog:analytics"}},
        },
        headers=ADMIN_HEADERS,
    )

    assert rejected.status_code == 400
    assert "secret reference" in rejected.json()["detail"]
    assert accepted.status_code == 200, accepted.json()


def test_workspace_catalog_enforces_configured_egress_allowlist():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    client = TestClient(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            catalog_egress_allowlist=("catalog.example",),
        )
    )

    rejected = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"uri": "https://other.example/api"},
        },
        headers=ADMIN_HEADERS,
    )
    accepted = client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"uri": "https://catalog.example/api"},
        },
        headers=ADMIN_HEADERS,
    )

    assert rejected.status_code == 400
    assert "egress allowlist" in rejected.json()["detail"]
    assert accepted.status_code == 200, accepted.json()


def test_workspace_catalog_tables_can_be_discovered_without_runtime_ids(monkeypatch):
    client = _client()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "default.users", "options": {}},
        headers=ADMIN_HEADERS,
    )

    def fake_discover_catalog_tables(name, module, options):
        assert name == "analytics"
        assert module == ICEBERG_CATALOG_MODULE
        assert options == {"type": "sql", "uri": "sqlite:///catalog.db"}
        return [
            {"backend": "iceberg", "name": "default.users", "table_identifier": "default.users"},
            {"backend": "iceberg", "name": "prod.orders", "table_identifier": "prod.orders"},
        ]

    monkeypatch.setattr(
        "dal_obscura.control_plane.application.provisioning.discover_catalog_tables",
        fake_discover_catalog_tables,
    )

    response = client.get("/v1/catalogs/analytics/tables", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json() == {
        "catalog": "analytics",
        "tables": [
            {
                "backend": "iceberg",
                "governed": True,
                "name": "default.users",
                "table_identifier": "default.users",
                "target": "default.users",
            },
            {
                "backend": "iceberg",
                "governed": False,
                "name": "prod.orders",
                "table_identifier": "prod.orders",
                "target": "prod.orders",
            },
        ],
    }
    assert "tenant" not in _keys_recursive(response.json())


def test_workspace_catalog_discovery_resolves_secret_references(monkeypatch):
    client = _client()
    monkeypatch.setenv("CATALOG_TOKEN", "sentinel-secret")
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {
                "uri": "https://catalog.example/api",
                "token": {"secret": "CATALOG_TOKEN", "scope": "catalog:analytics"},
            },
        },
        headers=ADMIN_HEADERS,
    )
    received: dict[str, object] = {}

    def fake_discover_catalog_tables(name, module, options):
        received.update(options)
        return []

    monkeypatch.setattr(
        "dal_obscura.control_plane.application.provisioning.discover_catalog_tables",
        fake_discover_catalog_tables,
    )

    response = client.get("/v1/catalogs/analytics/tables", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert received["token"] == "sentinel-secret"


def test_workspace_catalog_discovery_does_not_echo_provider_errors(monkeypatch):
    client = _client()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"uri": "https://catalog.example/api"},
        },
        headers=ADMIN_HEADERS,
    )

    def failing_discovery(name, module, options):
        raise ValueError(
            "failed to connect https://catalog-user:catalog-password@catalog.example/api"
        )

    monkeypatch.setattr(
        "dal_obscura.control_plane.application.provisioning.discover_catalog_tables",
        failing_discovery,
    )

    response = client.get("/v1/catalogs/analytics/tables", headers=ADMIN_HEADERS)

    assert response.status_code == 400
    payload = response.json()
    assert payload["detail"] == "Catalog discovery failed"
    assert payload["error"]["code"] == "validation_error"
    assert payload["error"]["request_id"]
    assert "catalog-password" not in response.text
    assert "cell" not in _keys_recursive(response.json())


def test_workspace_catalog_diagnostics_are_bounded_and_redacted(monkeypatch):
    client = _client()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )

    def fake_discover_catalog_tables(name, module, options):
        assert name == "analytics"
        assert module == ICEBERG_CATALOG_MODULE
        assert options == {"type": "sql", "uri": "sqlite:///catalog.db"}
        return [
            {
                "backend": "iceberg",
                "name": "default.users",
                "table_identifier": "default.users",
            }
        ]

    monkeypatch.setattr(
        "dal_obscura.control_plane.application.provisioning.discover_catalog_tables",
        fake_discover_catalog_tables,
    )

    ready = client.get("/v1/catalogs/analytics/diagnostics", headers=ADMIN_HEADERS)

    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"
    assert ready.json()["catalog"] == "analytics"
    assert ready.json()["table_count"] == 1
    assert ready.json()["sample_tables"] == ["default.users"]
    assert "options" not in _keys_recursive(ready.json())

    def failing_discover_catalog_tables(name, module, options):
        raise RuntimeError("failed to connect with password=super-secret")

    monkeypatch.setattr(
        "dal_obscura.control_plane.application.provisioning.discover_catalog_tables",
        failing_discover_catalog_tables,
    )
    failed = client.get("/v1/catalogs/analytics/diagnostics", headers=ADMIN_HEADERS)

    assert failed.status_code == 200
    assert failed.json()["status"] == "unavailable"
    assert failed.json()["message"] == "Catalog discovery failed"
    assert "super-secret" not in failed.text
