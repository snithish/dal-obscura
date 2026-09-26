from __future__ import annotations

import json
from pathlib import Path

import pytest

from examples.demo.keycloak.scripts import prepare_demo, provision_demo


@pytest.mark.parametrize(
    ("existing_origin", "expected_port"),
    [
        ("https://governance.localhost", 28821),
        ("http://127.0.0.1:8821", 28821),
        ("http://127.0.0.1:28822", 28822),
        ("http://localhost:28823", 28823),
    ],
)
def test_prepare_demo_uses_high_port_after_legacy_runtime(
    monkeypatch, existing_origin, expected_port
):
    monkeypatch.delenv("DAL_OBSCURA_DEMO_UI_PORT", raising=False)

    assert (
        prepare_demo._demo_ui_port({"DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ORIGIN": existing_origin})
        == expected_port
    )


def test_keycloak_demo_fixture_declares_catalog_backed_iceberg_tables():
    fixture = json.loads(
        Path("examples/demo/keycloak/fixtures/demo_fixture.json").read_text(encoding="utf-8")
    )

    assert fixture["catalogs"] == [{"name": "retail_demo", "plugin_id": "iceberg.sql"}]
    assert {table["backend"] for table in fixture["tables"]} == {"iceberg"}
    assert all("table_path" not in table for table in fixture["tables"])
    assert all(table["rows"] for table in fixture["tables"])
    assert fixture["owners"] == ["group:asset-owners"]
    assert "grants" not in fixture
    assert len(fixture["policies"]) == 3


def test_provision_demo_preserves_nested_live_schema_identities():
    fields = provision_demo._flatten_schema_fields(
        [
            {
                "field_id": 10,
                "name": "customer",
                "path": {"segments": [{"kind": "field", "name": "customer", "field_id": 10}]},
                "type": "struct<email: string>",
                "nullable": False,
                "kind": "struct",
                "children": [
                    {
                        "field_id": 11,
                        "name": "email",
                        "path": {
                            "segments": [
                                {"kind": "field", "name": "customer", "field_id": 10},
                                {"kind": "field", "name": "email", "field_id": 11},
                            ]
                        },
                        "type": "string",
                        "nullable": True,
                        "kind": "scalar",
                    }
                ],
            },
            {
                "field_id": 12,
                "name": "tags",
                "path": {"segments": [{"kind": "field", "name": "tags", "field_id": 12}]},
                "type": "list<string>",
                "nullable": True,
                "kind": "list",
                "children": [
                    {
                        "field_id": 13,
                        "name": "element",
                        "path": {
                            "segments": [
                                {"kind": "field", "name": "tags", "field_id": 12},
                                {"kind": "list_element"},
                            ]
                        },
                        "type": "string",
                        "nullable": False,
                        "kind": "scalar",
                    }
                ],
            },
        ]
    )

    assert [(field["field_id"], field["path"]) for field in fields] == [
        ("10", ["customer"]),
        ("11", ["customer", "email"]),
        ("12", ["tags"]),
        ("13", ["tags", "$element"]),
    ]
    assert fields[0]["nullable"] is False
    assert fields[1]["nullable"] is True


def test_provision_demo_rejects_schema_without_provider_field_ids():
    with pytest.raises(RuntimeError, match="provider identity"):
        provision_demo._flatten_schema_fields(
            [
                {
                    "name": "customer_id",
                    "path": {"segments": [{"kind": "field", "name": "customer_id"}]},
                }
            ]
        )


def test_prepare_demo_refreshes_runtime_config_without_rotating_secrets(tmp_path, monkeypatch):
    runtime_dir = tmp_path / ".runtime"
    realm_file = runtime_dir / "keycloak" / "realm.json"

    monkeypatch.setattr(prepare_demo, "RUNTIME_DIR", runtime_dir)
    monkeypatch.setattr(prepare_demo, "REALM_FILE", realm_file)
    monkeypatch.setattr(prepare_demo, "KEYCLOAK_ENV", runtime_dir / "keycloak.env")
    monkeypatch.setattr(prepare_demo, "POSTGRES_ENV", runtime_dir / "postgres.env")
    monkeypatch.setattr(prepare_demo, "CONTROL_PLANE_ENV", runtime_dir / "control-plane.env")
    monkeypatch.setattr(prepare_demo, "DATA_PLANE_ENV", runtime_dir / "data-plane.env")
    monkeypatch.setattr(prepare_demo, "CLIENT_ENV", runtime_dir / "client.env")
    monkeypatch.setattr(prepare_demo, "SETUP_ENV", runtime_dir / "setup.env")
    monkeypatch.setattr(prepare_demo, "UI_ENV", runtime_dir / "ui.env")
    monkeypatch.setenv("DAL_OBSCURA_DEMO_UI_PORT", "28821")
    runtime_dir.mkdir(parents=True)
    setup_marker = runtime_dir / "setup.done"
    setup_marker.write_text("", encoding="utf-8")

    prepare_demo.main()

    first_admin_token = next(
        line.partition("=")[2]
        for line in (runtime_dir / "control-plane.env").read_text(encoding="utf-8").splitlines()
        if line.startswith("DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN=")
    )
    monkeypatch.setenv("DAL_OBSCURA_DEMO_UI_PORT", "28822")
    prepare_demo.main()

    keycloak_env = (runtime_dir / "keycloak.env").read_text(encoding="utf-8")
    ui_env = (runtime_dir / "ui.env").read_text(encoding="utf-8")
    control_plane_env = (runtime_dir / "control-plane.env").read_text(encoding="utf-8")
    realm = json.loads(realm_file.read_text(encoding="utf-8"))
    ui_client = next(
        client for client in realm["clients"] if client["clientId"] == "dal-obscura-ui"
    )
    cli_client = next(
        client for client in realm["clients"] if client["clientId"] == "dal-obscura-cli"
    )
    ui_groups_mapper = next(
        mapper for mapper in ui_client["protocolMappers"] if mapper["name"] == "groups"
    )
    tenant_mapper = next(
        mapper for mapper in cli_client["protocolMappers"] if mapper["name"] == "tenant-id"
    )
    ui_origin = "http://localhost:28822"

    assert "127.0.0.1:20080:8080" in Path("examples/demo/keycloak/compose.yaml").read_text(
        encoding="utf-8"
    )
    assert "KC_HOSTNAME=http://127.0.0.1:20080" in keycloak_env
    assert (
        "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER=http://127.0.0.1:20080/realms/dal-obscura-demo"
        in control_plane_env
    )
    assert (
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER=http://127.0.0.1:20080/realms/dal-obscura-demo"
        in control_plane_env
    )
    assert "DAL_OBSCURA_API_BASE_URL=http://127.0.0.1:28820" in ui_env
    assert (
        f"DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI={ui_origin}/auth/callback"
        in control_plane_env
    )
    assert f"DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS={ui_origin}" in control_plane_env
    assert (
        f"DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI={ui_origin}"
        in control_plane_env
    )
    assert (
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_TOKEN_ENDPOINT="
        "http://keycloak:8080/realms/dal-obscura-demo/protocol/openid-connect/token"
        in control_plane_env
    )
    assert ui_client["redirectUris"] == [f"{ui_origin}/auth/callback"]
    assert ui_client["webOrigins"] == [ui_origin]
    assert ui_groups_mapper["config"]["claim.name"] == "groups"
    assert ui_groups_mapper["config"]["id.token.claim"] == "true"
    assert tenant_mapper["config"]["claim.name"] == "tenant_id"
    assert tenant_mapper["config"]["claim.value"] == "default"
    assert tenant_mapper["config"]["access.token.claim"] == "true"
    assert setup_marker.exists()
    assert f"DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN={first_admin_token}" in control_plane_env
