from __future__ import annotations

from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_MODULE,
    _client,
    _keys_recursive,
    _provision_draft,
)


def test_workspace_asset_upsert_uses_default_workspace_context():
    client = _client()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )

    response = client.put(
        "/v1/assets/analytics/default.users",
        json={
            "backend": "iceberg",
            "table_identifier": "prod.users",
            "options": {"snapshot": 7},
        },
        headers=ADMIN_HEADERS,
    )
    assets = client.get("/v1/assets", headers=ADMIN_HEADERS).json()
    detail = client.get(f"/v1/assets/{response.json()['id']}", headers=ADMIN_HEADERS).json()

    assert response.status_code == 200
    assert response.json() == {
        "id": response.json()["id"],
        "catalog": "analytics",
        "target": "default.users",
    }
    assert assets == [
        {
            "id": response.json()["id"],
            "name": "default.users",
            "catalog": "analytics",
            "backend": "iceberg",
            "table_identifier": "prod.users",
            "owner_count": 0,
            "owners": [],
            "policy_status": "missing",
            "draft_status": "draft",
        }
    ]
    assert detail["options"] == {"snapshot": 7}
    assert detail["policy_rules"] == []
    assert "tenant" not in _keys_recursive({"assets": assets, "detail": detail})
    assert "cell" not in _keys_recursive({"assets": assets, "detail": detail})


def test_workspace_asset_schema_fields_can_be_replaced_from_asset_detail():
    client = _client()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    asset = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {}},
        headers=ADMIN_HEADERS,
    ).json()

    response = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={
            "fields": [
                {"name": "id", "type": "long", "nullable": False},
                {"name": "email", "type": "string", "nullable": True},
            ]
        },
        headers=ADMIN_HEADERS,
    )
    second_response = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={
            "fields": [
                {"name": "id", "type": "long", "nullable": False},
                {"name": "email", "type": "string", "nullable": False},
            ]
        },
        headers=ADMIN_HEADERS,
    )
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()

    assert response.status_code == 200
    assert response.json() == {
        "asset_id": asset["id"],
        "fields": [
            {"name": "id", "type": "long", "nullable": False},
            {"name": "email", "type": "string", "nullable": True},
        ],
    }
    assert second_response.status_code == 200
    assert detail["schema_fields"] == [
        {"name": "id", "type": "long", "nullable": False},
        {"name": "email", "type": "string", "nullable": False},
    ]


def test_workspace_policy_rules_can_be_replaced_from_asset_detail():
    client = _client()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    asset = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {}},
        headers=ADMIN_HEADERS,
    ).json()

    response = client.put(
        f"/v1/assets/{asset['id']}/policy-rules",
        json={
            "rules": [
                {
                    "ordinal": 1,
                    "effect": "allow",
                    "principals": ["group:data-stewards"],
                    "when": {},
                    "columns": ["id", "email"],
                    "masks": {"email": {"type": "email"}},
                    "row_filter": "region = 'us'",
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()

    assert response.status_code == 200
    assert detail["policy_status"] == "configured"
    assert detail["policy_rules"] == [
        {
            "id": detail["policy_rules"][0]["id"],
            "asset_id": asset["id"],
            "ordinal": 1,
            "effect": "allow",
            "principals": ["group:data-stewards"],
            "when": {},
            "columns": ["id", "email"],
            "masks": {"email": {"type": "email"}},
            "row_filter": "region = 'us'",
        }
    ]


def test_asset_policy_preview_uses_server_policy_semantics():
    client = _client()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    asset = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {}},
        headers=ADMIN_HEADERS,
    ).json()
    client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={
            "fields": [
                {"name": "id", "type": "long", "nullable": False},
                {"name": "email", "type": "string", "nullable": True},
                {"name": "region", "type": "string", "nullable": True},
            ]
        },
        headers=ADMIN_HEADERS,
    )
    client.put(
        f"/v1/assets/{asset['id']}/policy-rules",
        json={
            "rules": [
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["group:data-stewards"],
                    "when": {"tenant": "default"},
                    "columns": ["id", "email"],
                    "masks": {"email": {"type": "email"}},
                    "row_filter": "region = 'us'",
                }
            ]
        },
        headers=ADMIN_HEADERS,
    )

    unauthorized = client.post(
        f"/v1/assets/{asset['id']}/policy-preview",
        json={"principal": "user:alice@example.com"},
    )
    response = client.post(
        f"/v1/assets/{asset['id']}/policy-preview",
        json={
            "principal": "user:alice@example.com",
            "groups": ["data-stewards"],
            "claims": {"tenant": "default"},
        },
        headers=ADMIN_HEADERS,
    )

    assert unauthorized.status_code == 401
    assert response.status_code == 200
    assert response.json() == {
        "decision": "allow",
        "matched_ordinal": 10,
        "reason": "Rule 10 matched.",
        "visible_columns": ["id", "email"],
        "masks": [{"column": "email", "type": "email"}],
        "row_filter": "(region = 'us')",
    }
    assert "tenant" not in _keys_recursive(response.json())
    assert "cell" not in _keys_recursive(response.json())


def test_workspace_asset_owners_can_be_replaced_from_asset_detail():
    client = _client()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    asset = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {}},
        headers=ADMIN_HEADERS,
    ).json()

    response = client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:alice@example.com", "group:data-owners"]},
        headers=ADMIN_HEADERS,
    )
    second_response = client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:alice@example.com"]},
        headers=ADMIN_HEADERS,
    )
    assets = client.get("/v1/assets", headers=ADMIN_HEADERS).json()
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()
    summary = client.get("/v1/workspace/summary", headers=ADMIN_HEADERS).json()

    assert response.status_code == 200
    assert response.json() == {
        "asset_id": asset["id"],
        "owners": ["user:alice@example.com", "group:data-owners"],
    }
    assert second_response.status_code == 200
    assert assets[0]["owner_count"] == 1
    assert assets[0]["owners"] == ["user:alice@example.com"]
    assert detail["owner_count"] == 1
    assert detail["owners"] == ["user:alice@example.com"]
    assert summary["unowned_asset_count"] == 0
    assert summary["runtime_configured"] is False
    assert summary["enabled_auth_provider_count"] == 0


def test_workspace_catalogs_assets_and_asset_detail_hide_runtime_ids():
    client = _client()
    asset = _provision_draft(client)

    summary = client.get("/v1/workspace/summary", headers=ADMIN_HEADERS).json()
    catalogs = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
    assets = client.get("/v1/assets", headers=ADMIN_HEADERS).json()
    asset_detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()

    assert summary == {
        "catalog_count": 1,
        "asset_count": 1,
        "unowned_asset_count": 1,
        "missing_policy_count": 0,
        "draft_change_count": 1,
        "runtime_configured": True,
        "enabled_auth_provider_count": 1,
    }
    assert catalogs == [
        {
            "id": catalogs[0]["id"],
            "name": "analytics",
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
            "status": "configured",
            "discovered_table_count": 0,
            "governed_asset_count": 1,
        }
    ]
    assert assets == [
        {
            "id": asset["id"],
            "name": "default.users",
            "catalog": "analytics",
            "backend": "iceberg",
            "table_identifier": "prod.users",
            "owner_count": 0,
            "owners": [],
            "policy_status": "configured",
            "draft_status": "draft",
        }
    ]
    assert asset_detail == {
        **assets[0],
        "options": {"snapshot": 1},
        "schema_fields": [],
        "policy_rules": [
            {
                "id": asset_detail["policy_rules"][0]["id"],
                "asset_id": asset["id"],
                "ordinal": 10,
                "effect": "allow",
                "principals": ["user1"],
                "when": {"tenant": "default"},
                "columns": ["id", "email"],
                "masks": {"email": {"type": "email"}},
                "row_filter": "region = 'us'",
            }
        ],
    }
    assert "tenant" not in _keys_recursive(summary | {"catalogs": catalogs, "assets": assets})
    assert "cell" not in _keys_recursive(summary | {"catalogs": catalogs, "assets": assets})
