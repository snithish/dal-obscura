from __future__ import annotations

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
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
            "status": "configured",
            "discovered_table_count": 0,
            "governed_asset_count": 0,
        }
    ]
    assert summary["catalog_count"] == 1
    assert summary["asset_count"] == 0
    assert summary["runtime_configured"] is False
    assert summary["enabled_auth_provider_count"] == 0


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
    assert "cell" not in _keys_recursive(response.json())
