from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dal_obscura.interfaces.http.app import create_app
from dal_obscura.sources.secrets import EnvSecretProvider
from dal_obscura.storage.database.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from examples.demo.keycloak.scripts import provision_demo, seed_table


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    options = {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": str(tmp_path / "warehouse"),
    }
    monkeypatch.setattr(seed_table, "_catalog_options", lambda: options)
    fixture = json.loads(Path("examples/demo/keycloak/fixtures/demo_fixture.json").read_text())
    for table in fixture["tables"]:
        seed_table._create_iceberg_table(table)
    monkeypatch.setenv("ICEBERG_CATALOG_URI", options["uri"])
    monkeypatch.setenv("LOCAL_ICEBERG_URI", options["uri"])
    monkeypatch.setenv("ICEBERG_WAREHOUSE", options["warehouse"])
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    with TestClient(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            secret_provider=EnvSecretProvider(
                config={"scope_grants": {"catalog:retail_demo": ["LOCAL_ICEBERG_URI"]}}
            ),
        )
    ) as client:

        def request(method, path, body=None):
            response = client.request(
                method, path, json=body, headers={"authorization": "Bearer test-admin"}
            )
            assert response.status_code < 400, response.text
            return response.json()

        monkeypatch.setattr(provision_demo, "_request", request)
        yield fixture, request
    engine.dispose()


def test_initialization_resumes_after_interrupted_api_write(workspace, monkeypatch):
    fixture, request = workspace

    def interrupted(method, path, body=None):
        if method == "PUT" and path.endswith("/owners"):
            raise RuntimeError("injected interruption")
        return request(method, path, body)

    monkeypatch.setattr(provision_demo, "_request", interrupted)
    with pytest.raises(RuntimeError, match="injected interruption"):
        provision_demo._provision_workspace(fixture)
    monkeypatch.setattr(provision_demo, "_request", request)
    provision_demo._provision_workspace(fixture)
    asset = request("GET", "/v1/assets")[0]
    detail = request("GET", f"/v1/assets/{asset['id']}")
    assert detail["policy_revision"] == 1
    assert detail["owners"] == ["http://localhost:20080/realms/dal-obscura-demo|g|asset-owners"]
    assert detail["schema_fields"]
    assert len(detail["policy_rules"]) == 3
    assert (
        request("GET", "/v1/settings/auth-providers")[0]["args"]["issuer"]
        == provision_demo.DEMO_OIDC_ISSUER
    )


def test_reinitialization_preserves_authored_deny_all_and_settings(workspace):
    fixture, request = workspace
    provision_demo._provision_workspace(fixture)
    asset = request("GET", "/v1/assets")[0]
    request("PUT", f"/v1/assets/{asset['id']}/policy", {"expected_revision": 1, "rules": []})
    settings = request("GET", "/v1/settings/runtime")
    request(
        "PUT",
        "/v1/settings/runtime",
        {
            "expected_revision": settings["revision"],
            "ticket_ttl_seconds": 123,
            "max_tickets": 4,
            "max_ticket_exchanges": 1,
        },
    )
    before = request("GET", f"/v1/assets/{asset['id']}")
    provision_demo._provision_workspace(fixture)
    assert request("GET", f"/v1/assets/{asset['id']}") == before
    assert request("GET", "/v1/settings/runtime")["ticket_ttl_seconds"] == 123


def test_demo_owner_keys_are_issuer_scoped():
    assert provision_demo._scoped_demo_owners(["group:asset-owners", "asset-owner"]) == [
        "http://localhost:20080/realms/dal-obscura-demo|g|asset-owners",
        "http://localhost:20080/realms/dal-obscura-demo|u|asset-owner",
    ]
    with pytest.raises(ValueError, match="at least one owner"):
        provision_demo._scoped_demo_owners([])


def test_catalog_credentials_are_references_not_inline(workspace):
    fixture, request = workspace
    provision_demo._provision_workspace(fixture)
    catalog = request("GET", "/v1/catalogs")[0]
    assert catalog["options"]["uri"] == {
        "secret": "LOCAL_ICEBERG_URI",
        "scope": "catalog:retail_demo",
    }
