from __future__ import annotations

from tests.support.control.actors import _bearer, _client
from tests.support.control.workspace import ADMIN_HEADERS


def test_plugin_descriptors_require_platform_admin(client_factory) -> None:
    client = _client(client_factory)

    assert client.get("/v1/plugins").status_code == 401
    assert client.get("/v1/plugins", headers=_bearer("owner-token")).status_code == 403


def test_plugin_descriptors_expose_only_admitted_bounded_capabilities(client_factory) -> None:
    client = client_factory()

    response = client.get("/v1/plugins", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    payload = response.json()
    assert {item["plugin_id"] for item in payload["plugins"]} == {
        "iceberg.catalog",
        "iceberg",
        "environment",
        "mounted_file",
    }
    assert payload["states"] == [
        {"kind": "catalog", "plugin_id": "iceberg.catalog", "status": "enabled"},
        {"kind": "secret_backend", "plugin_id": "environment", "status": "enabled"},
        {"kind": "secret_backend", "plugin_id": "mounted_file", "status": "enabled"},
        {"kind": "table_format", "plugin_id": "iceberg", "status": "enabled"},
    ]
    assert payload["pairs"] == [
        {
            "catalog_plugin_id": "iceberg.catalog",
            "format_plugin_id": "iceberg",
            "capabilities": ["nested_schema", "snapshot_reads", "splittable_scan"],
            "handle_versions": [1],
            "status": "admitted",
        }
    ]
    catalog = next(item for item in payload["plugins"] if item["plugin_id"] == "iceberg.catalog")
    assert catalog["status"] == "admitted"
    assert catalog["output_formats"] == ["iceberg"]
    assert all("$ref" not in str(value) for value in catalog["config_schema"].values())
    assert "password" in {field["name"] for field in catalog["config_schema"]["fields"]}


def test_plugin_lifecycle_mutation_is_retired_without_state_or_audit_changes(
    client_factory,
) -> None:
    client = client_factory()
    before = client.get("/v1/plugins", headers=ADMIN_HEADERS).json()
    events = client.get("/v1/audit/events/page", headers=ADMIN_HEADERS).json()

    response = client.patch(
        "/v1/plugins/catalog/iceberg.catalog/lifecycle",
        headers=ADMIN_HEADERS,
        json={"target": "disabled"},
    )
    assert response.status_code == 404
    assert client.get("/v1/plugins", headers=ADMIN_HEADERS).json() == before
    assert client.get("/v1/audit/events/page", headers=ADMIN_HEADERS).json() == events
