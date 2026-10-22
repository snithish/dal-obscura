from __future__ import annotations

import pytest

from tests.support.control.workspace import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_ID,
    _keys_recursive,
    _provision_asset,
)


@pytest.fixture
def asset_client(client_factory):
    client = client_factory()
    catalog = client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    assert catalog.status_code == 200, catalog.text
    asset = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {}},
        headers=ADMIN_HEADERS,
    )
    assert asset.status_code == 200, asset.text
    return client, asset.json()


def test_schema_admission_preserves_exact_names_and_distinct_paths(asset_client):
    client, asset = asset_client
    names = ["x" * 257, "界" * 1024, "x" * 16384, " spaced.name "]
    paths = [[name] for name in names] + [
        ["a.b"],
        ["a", "b"],
        ["left", "value"],
        ["right", "value"],
    ]
    fields = [
        {"name": path[-1], "field_id": str(index), "path": path}
        for index, path in enumerate(paths, 1)
    ]
    response = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={"expected_revision": 0, "fields": fields},
        headers=ADMIN_HEADERS,
    )
    assert response.status_code == 200, response.text
    expected = [
        {**field, "field_id": f"iceberg:{field['field_id']}", "type": "string", "nullable": True}
        for field in fields
    ]
    assert response.json()["fields"] == expected
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()
    assert detail["schema_fields"] == expected


@pytest.mark.parametrize("nullable", ["false", 0, None])
def test_schema_admission_requires_boolean_nullability_without_coercion(asset_client, nullable):
    client, asset = asset_client
    response = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={
            "expected_revision": 0,
            "fields": [
                {
                    "name": "id",
                    "field_id": "1",
                    "path": ["id"],
                    "type": "int64",
                    "nullable": nullable,
                }
            ],
        },
        headers=ADMIN_HEADERS,
    )
    assert response.status_code == 422
    assert client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()["revision"] == 0


def test_asset_access_reports_effective_capabilities_and_reasons(asset_client):
    client, asset = asset_client
    admin = client.get(f"/v1/assets/{asset['id']}/access", headers=ADMIN_HEADERS)
    assert admin.status_code == 200
    assert admin.json()["principal"] == "platform:admin"
    assert admin.json()["can_revoke_tokens"] is True
    assert all(item["allowed"] for item in admin.json()["capabilities"])
    assert all(
        item["reasons"] == ["Platform administrator"] for item in admin.json()["capabilities"]
    )


def test_workspace_asset_upsert_uses_default_workspace_context(client_factory):
    client = client_factory()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )

    response = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {"snapshot": 7}},
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
            "policy_revision": 0,
        }
    ]
    assert detail["options"] == {"snapshot": 7}
    assert detail["policy_rules"] == []
    assert "tenant" not in _keys_recursive({"assets": assets, "detail": detail})
    assert "cell" not in _keys_recursive({"assets": assets, "detail": detail})


def test_workspace_asset_requires_physical_iceberg_identifier(client_factory):
    client = client_factory()

    response = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "options": {}},
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 422


def test_workspace_asset_schema_fields_can_be_replaced_from_asset_detail(asset_client):
    client, asset = asset_client

    legacy_response = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={"fields": [{"name": "id", "type": "long"}], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    missing_id_response = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={"fields": [{"name": "id", "path": ["id"], "type": "long"}], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    response = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={
            "fields": [
                {"name": "id", "field_id": "id", "path": ["id"], "type": "long", "nullable": False},
                {
                    "name": "email",
                    "field_id": "email",
                    "path": ["email"],
                    "type": "string",
                    "nullable": True,
                },
            ],
            "expected_revision": 0,
        },
        headers=ADMIN_HEADERS,
    )
    second_response = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={
            "fields": [
                {"name": "id", "field_id": "id", "path": ["id"], "type": "long", "nullable": False},
                {
                    "name": "email",
                    "field_id": "email",
                    "path": ["email"],
                    "type": "string",
                    "nullable": False,
                },
            ],
            "expected_revision": 1,
        },
        headers=ADMIN_HEADERS,
    )
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()

    assert legacy_response.status_code == 400
    assert "field path" in legacy_response.json()["error"]["message"].lower()
    assert missing_id_response.status_code == 400
    assert "field id" in missing_id_response.json()["error"]["message"].lower()
    assert response.status_code == 200
    assert response.json() == {
        "asset_id": asset["id"],
        "fields": [
            {
                "name": "id",
                "field_id": "iceberg:id",
                "path": ["id"],
                "type": "long",
                "nullable": False,
            },
            {
                "name": "email",
                "field_id": "iceberg:email",
                "path": ["email"],
                "type": "string",
                "nullable": True,
            },
        ],
    }
    assert second_response.status_code == 200
    assert detail["schema_fields"] == [
        {"name": "id", "field_id": "iceberg:id", "path": ["id"], "type": "long", "nullable": False},
        {
            "name": "email",
            "field_id": "iceberg:email",
            "path": ["email"],
            "type": "string",
            "nullable": False,
        },
    ]


def test_workspace_policy_can_be_replaced_directly_from_asset_detail(asset_client):
    client, asset = asset_client

    response = client.put(
        f"/v1/assets/{asset['id']}/policy",
        json={
            "expected_revision": 0,
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
            ],
        },
        headers=ADMIN_HEADERS,
    )
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()

    assert response.status_code == 200
    assert detail["policy_status"] == "configured"
    assert [
        {
            key: rule[key]
            for key in ("ordinal", "effect", "principals", "when", "columns", "masks", "row_filter")
        }
        for rule in detail["policy_rules"]
    ] == [
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


def test_retired_asset_policy_preview_route_does_not_dispatch(asset_client):
    client, asset = asset_client
    response = client.post(
        f"/v1/assets/{asset['id']}/policy-preview",
        json={"principal": "user:alice@example.com"},
        headers=ADMIN_HEADERS,
    )
    # With the catch-all asset detail route still present, Starlette reports a
    # method mismatch (405) for this retired path. The important contract is
    # that no policy-preview handler executes or leaks resource metadata.
    assert response.status_code == 405
    assert "cell" not in _keys_recursive(response.json())


def test_workspace_asset_owners_can_be_replaced_from_asset_detail(asset_client):
    client, asset = asset_client

    missing = client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:alice@example.com"]},
        headers=ADMIN_HEADERS,
    )
    assert missing.status_code == 428
    assert client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()["revision"] == 0
    response = client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:alice@example.com", "group:data-owners"], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    removed = client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": [], "expected_revision": 1},
        headers=ADMIN_HEADERS,
    )
    assert removed.status_code == 400
    assert "last owner" in removed.json()["error"]["message"]
    second_response = client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:alice@example.com"], "expected_revision": 1},
        headers=ADMIN_HEADERS,
    )
    stale = client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user:stale"], "expected_revision": 1},
        headers=ADMIN_HEADERS,
    )
    assert stale.status_code == 409
    assert "Asset revision changed" in stale.json()["error"]["message"]
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


def test_workspace_catalogs_assets_and_asset_detail_hide_runtime_ids(client_factory):
    client = client_factory()
    asset = _provision_asset(client)

    summary = client.get("/v1/workspace/summary", headers=ADMIN_HEADERS).json()
    catalogs = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
    assets = client.get("/v1/assets", headers=ADMIN_HEADERS).json()
    asset_detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()

    assert summary == {
        "catalog_count": 1,
        "asset_count": 1,
        "unowned_asset_count": 1,
        "missing_policy_count": 0,
        "runtime_configured": True,
        "enabled_auth_provider_count": 1,
    }
    assert catalogs == [
        {
            "id": catalogs[0]["id"],
            "name": "analytics",
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
            "status": "configured",
            "revision": 0,
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
            "policy_revision": 1,
        }
    ]
    assert asset_detail == {
        **assets[0],
        "revision": 0,
        "options": {"snapshot": 1},
        "schema_fields": [],
        "policy_rules": asset_detail["policy_rules"],
    }
    assert [
        {
            key: rule[key]
            for key in ("ordinal", "effect", "principals", "when", "columns", "masks", "row_filter")
        }
        for rule in asset_detail["policy_rules"]
    ] == [
        {
            "ordinal": 10,
            "effect": "allow",
            "principals": ["user1"],
            "when": {"department": "default"},
            "columns": ["id", "email"],
            "masks": {"email": {"type": "email"}},
            "row_filter": "region = 'us'",
        }
    ]
    assert "tenant" not in _keys_recursive(summary | {"catalogs": catalogs, "assets": assets})
    assert "cell" not in _keys_recursive(summary | {"catalogs": catalogs, "assets": assets})


def test_asset_binding_precondition_rejects_stale_update(asset_client) -> None:
    client, asset = asset_client

    missing = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users-v2", "options": {}},
        headers=ADMIN_HEADERS,
    )
    assert missing.status_code == 428
    assert missing.json()["error"]["code"] == "revision_precondition_required"
    unchanged = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()
    assert unchanged["table_identifier"] == "prod.users"
    assert unchanged["revision"] == 0
    first = client.put(
        "/v1/assets/analytics/default.users",
        json={
            "backend": "iceberg",
            "table_identifier": "prod.users-v2",
            "options": {},
            "expected_revision": 0,
        },
        headers=ADMIN_HEADERS,
    )
    stale = client.put(
        "/v1/assets/analytics/default.users",
        json={
            "backend": "iceberg",
            "table_identifier": "prod.users-v3",
            "options": {},
            "expected_revision": 0,
        },
        headers=ADMIN_HEADERS,
    )

    assert first.status_code == 200
    assert stale.status_code == 409
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()
    assert detail["table_identifier"] == "prod.users-v2"
    assert detail["revision"] == 1


def test_asset_binding_precondition_rejects_nonzero_revision_on_create(client_factory) -> None:
    client = client_factory()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )

    response = client.put(
        "/v1/assets/analytics/default.new",
        json={
            "backend": "iceberg",
            "table_identifier": "prod.new",
            "options": {},
            "expected_revision": 7,
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 409
    assert "expected 7, current 0" in response.json()["error"]["message"]
    assert client.get("/v1/assets", headers=ADMIN_HEADERS).json() == []


def test_asset_grant_precondition_rejects_stale_writer(asset_client) -> None:
    client, asset = asset_client
    current = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()

    missing = client.put(
        f"/v1/assets/{asset['id']}/grants",
        json={"grants": [{"principal": "analyst", "capability": "read"}]},
        headers=ADMIN_HEADERS,
    )
    assert missing.status_code == 428
    assert missing.json()["error"]["code"] == "revision_precondition_required"
    assert client.get(f"/v1/assets/{asset['id']}/grants", headers=ADMIN_HEADERS).json() == []
    first = client.put(
        f"/v1/assets/{asset['id']}/grants",
        json={
            "grants": [{"principal": "analyst", "capability": "read"}],
            "expected_revision": current["revision"],
        },
        headers=ADMIN_HEADERS,
    )
    stale = client.put(
        f"/v1/assets/{asset['id']}/grants",
        json={"grants": [], "expected_revision": current["revision"]},
        headers=ADMIN_HEADERS,
    )

    assert first.status_code == 200
    assert stale.status_code == 409


def test_workspace_asset_page_is_bounded_searchable_and_cursor_paginated(client_factory):
    client = client_factory()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    for target in ("default.orders", "default.users", "prod.events"):
        response = client.put(
            f"/v1/assets/analytics/{target}",
            json={"backend": "iceberg", "table_identifier": target, "options": {}},
            headers=ADMIN_HEADERS,
        )
        assert response.status_code == 200, response.json()

    first = client.get("/v1/assets/page?limit=2", headers=ADMIN_HEADERS)
    assert first.status_code == 200, first.json()
    assert [item["name"] for item in first.json()["items"]] == [
        "default.orders",
        "default.users",
    ]
    assert first.json()["next_cursor"]

    second = client.get(
        "/v1/assets/page?limit=2&cursor=" + first.json()["next_cursor"], headers=ADMIN_HEADERS
    )
    assert second.status_code == 200, second.json()
    assert [item["name"] for item in second.json()["items"]] == ["prod.events"]
    assert second.json()["next_cursor"] is None

    searched = client.get("/v1/assets/page?limit=1&search=orders", headers=ADMIN_HEADERS)
    assert searched.status_code == 200, searched.json()
    assert [item["name"] for item in searched.json()["items"]] == ["default.orders"]

    mismatched = client.get(
        "/v1/assets/page?search=users&cursor=" + first.json()["next_cursor"], headers=ADMIN_HEADERS
    )
    assert mismatched.status_code == 400

    invalid = client.get("/v1/assets/page?cursor=garbage", headers=ADMIN_HEADERS)
    assert invalid.status_code == 400


@pytest.mark.parametrize("name", [" spaced.name ", " "], ids=["spaced", "space-only"])
def test_policy_admission_keeps_whitespace_field_identity(client_factory, name):
    from dal_obscura_plugin_api.paths import FieldPath, FieldSegment

    client = client_factory()
    asset = _provision_asset(client)
    saved = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        headers=ADMIN_HEADERS,
        json={
            "expected_revision": 0,
            "fields": [
                {"name": name, "field_id": "1", "path": [name], "type": "string"},
                {"name": "other", "field_id": "2", "path": ["other"], "type": "string"},
            ],
        },
    )
    assert saved.status_code == 200, saved.text
    path = FieldPath((FieldSegment(name),)).to_human()
    policy = client.put(
        f"/v1/assets/{asset['id']}/policy",
        headers=ADMIN_HEADERS,
        json={
            "expected_revision": 1,
            "rules": [
                {
                    "ordinal": 1,
                    "effect": "allow",
                    "principals": ["reader"],
                    "columns": [path],
                    "masks": {path: {"type": "null"}},
                }
            ],
        },
    )
    assert policy.status_code == 200, policy.text
    detail = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()
    assert detail["policy_rules"][0]["columns"] == [path]
    assert list(detail["policy_rules"][0]["masks"]) == [path]
