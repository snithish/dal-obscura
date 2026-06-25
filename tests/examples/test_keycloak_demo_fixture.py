from __future__ import annotations

import json
from pathlib import Path

from examples.demo.keycloak.scripts import prepare_demo


def test_keycloak_demo_fixture_declares_catalog_backed_iceberg_tables():
    fixture = json.loads(
        Path("examples/demo/keycloak/fixtures/demo_fixture.json").read_text(encoding="utf-8")
    )

    assert {catalog["name"] for catalog in fixture["catalogs"]} == {"retail_demo"}
    assert {catalog["kind"] for catalog in fixture["catalogs"]} == {"iceberg_sql"}
    assert {table["backend"] for table in fixture["tables"]} == {"iceberg"}
    assert all("table_path" not in table for table in fixture["tables"])
    assert all(table["rows"] for table in fixture["tables"])
    assert fixture["owners"] == ["group:asset-owners"]
    assert len(fixture["policies"]) == 3


def test_prepare_demo_writes_separate_ui_runtime_config(tmp_path, monkeypatch):
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

    prepare_demo.main()

    ui_env = (runtime_dir / "ui.env").read_text(encoding="utf-8")
    control_plane_env = (runtime_dir / "control-plane.env").read_text(encoding="utf-8")
    realm = json.loads(realm_file.read_text(encoding="utf-8"))
    ui_client = next(
        client for client in realm["clients"] if client["clientId"] == "dal-obscura-ui"
    )

    assert "DAL_OBSCURA_API_BASE_URL=http://127.0.0.1:8820" in ui_env
    assert (
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI=http://127.0.0.1:8821/auth/callback"
        in control_plane_env
    )
    assert ui_client["redirectUris"] == ["http://127.0.0.1:8821/auth/callback"]
    assert ui_client["webOrigins"] == ["http://127.0.0.1:8821"]
