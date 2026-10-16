from __future__ import annotations

import json
from email.message import Message
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit

import pytest

from dal_obscura.adapters.sql.database.db import session_factory
from dal_obscura.bootstrap.control import build_control_app
from dal_obscura.runtime.secrets import SecretProviderConfig, load_secret_provider
from examples.demo.keycloak.scripts import provision_demo, seed_table


@pytest.fixture
def workspace(tmp_path, monkeypatch, db_engine, http_client):
    options = {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": str(tmp_path / "warehouse"),
    }
    monkeypatch.setenv("ICEBERG_CATALOG_URI", options["uri"])
    monkeypatch.setenv("ICEBERG_WAREHOUSE", options["warehouse"])
    fixture = json.loads(Path("examples/demo/keycloak/fixtures/demo_fixture.json").read_text())
    for table in fixture["tables"]:
        seed_table._create_iceberg_table(table)
    monkeypatch.setenv("ICEBERG_CATALOG_URI", options["uri"])
    monkeypatch.setenv("LOCAL_ICEBERG_URI", options["uri"])
    monkeypatch.setenv("ICEBERG_WAREHOUSE", options["warehouse"])
    client = http_client(
        build_control_app(
            session_factory(db_engine),
            admin_token="test-admin",
            secret_provider=load_secret_provider(
                SecretProviderConfig(
                    config={"scope_grants": {"catalog:retail_demo": ["LOCAL_ICEBERG_URI"]}}
                )
            ),
        )
    )

    def request(method, path, body=None):
        response = client.request(
            method, path, json=body, headers={"authorization": "Bearer test-admin"}
        )
        response.raise_for_status()
        return response.json()

    token_url = "https://identity.example/token"
    monkeypatch.setenv("OIDC_TOKEN_URL", token_url)
    monkeypatch.setenv("OIDC_CLI_CLIENT_SECRET", "fixture-client-secret")
    monkeypatch.setenv("DEMO_ADMIN_PASSWORD", "fixture-password")

    def transport(outgoing, timeout=15):
        if outgoing.full_url == token_url:
            return BytesIO(b'{"access_token":"test-admin"}')
        result = client.request(
            outgoing.get_method(),
            urlsplit(outgoing.full_url).path,
            content=outgoing.data,
            headers=dict(outgoing.header_items()),
        )
        if result.status_code >= 400:
            raise HTTPError(
                outgoing.full_url,
                result.status_code,
                "Request rejected",
                Message(),
                BytesIO(result.content),
            )
        return BytesIO(result.content)

    provision_demo._admin_token.cache_clear()
    monkeypatch.setattr(provision_demo, "urlopen", transport)
    try:
        yield fixture, request, transport
    finally:
        provision_demo._admin_token.cache_clear()


def test_initialization_resumes_after_interrupted_api_write(workspace, monkeypatch):
    fixture, request, transport = workspace

    def interrupted(outgoing, timeout=15):
        if outgoing.get_method() == "PUT" and urlsplit(outgoing.full_url).path.endswith("/owners"):
            raise RuntimeError("injected interruption")
        return transport(outgoing, timeout=timeout)

    monkeypatch.setattr(provision_demo, "urlopen", interrupted)
    with pytest.raises(RuntimeError, match="injected interruption"):
        provision_demo._provision_workspace(fixture)
    monkeypatch.setattr(provision_demo, "urlopen", transport)
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
    fixture, request, _ = workspace
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
    fixture, request, _ = workspace
    provision_demo._provision_workspace(fixture)
    catalog = request("GET", "/v1/catalogs")[0]
    assert catalog["options"]["uri"] == {
        "secret": "LOCAL_ICEBERG_URI",
        "scope": "catalog:retail_demo",
    }
