from __future__ import annotations

from tests.interfaces.control_plane.workspace_helpers import ADMIN_HEADERS, _client


def test_plugin_descriptors_require_platform_admin() -> None:
    client = _client()

    assert client.get("/v1/plugins").status_code == 401


def test_plugin_descriptors_expose_only_admitted_bounded_capabilities() -> None:
    client = _client()

    response = client.get("/v1/plugins", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    payload = response.json()
    assert {item["plugin_id"] for item in payload["plugins"]} == {"iceberg.sql", "iceberg"}
    assert payload["states"] == [
        {"kind": "catalog", "plugin_id": "iceberg.sql", "status": "enabled"},
        {"kind": "table_format", "plugin_id": "iceberg", "status": "enabled"},
    ]
    assert payload["pairs"] == [
        {
            "catalog_plugin_id": "iceberg.sql",
            "format_plugin_id": "iceberg",
            "capabilities": ["nested_schema", "snapshot_reads", "splittable_scan"],
            "status": "admitted",
        }
    ]
    catalog = next(item for item in payload["plugins"] if item["plugin_id"] == "iceberg.sql")
    assert catalog["status"] == "admitted"
    assert all("$ref" not in str(value) for value in catalog["config_schema"].values())
    assert "password" in {
        field["name"] for field in catalog["config_schema"]["fields"]
    }
