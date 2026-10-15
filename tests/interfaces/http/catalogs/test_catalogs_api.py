from __future__ import annotations

import json

import pytest
from dal_obscura_plugin_api import DiscoveryPage, TableIdentifier
from fastapi.testclient import TestClient

from dal_obscura.adapters.sql.database.db import (
    session_factory,
)
from dal_obscura.interfaces.http.app import create_app
from dal_obscura.runtime.secrets import load_secret_provider_from_environment
from tests.support.control.workspace import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_ID,
    _keys_recursive,
)
from tests.support.storage.postgres import isolated_postgres_sessions


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=pytest.mark.integration)])
def publication_client(request, client_factory):
    if request.param == "sqlite":
        yield client_factory()
    else:
        with (
            isolated_postgres_sessions() as factory,
            TestClient(
                create_app(
                    factory, admin_token=ADMIN_HEADERS["authorization"].removeprefix("Bearer ")
                )
            ) as client,
        ):
            yield client


def test_catalog_release_publication_commits_every_change_and_audit(publication_client):
    client = publication_client
    drafts = {
        name: {
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"uri": f"sqlite:///{name}.db"},
            "expected_revision": 0,
        }
        for name in ("warehouse-b", "warehouse-a")
    }
    response = client.post("/v1/catalogs/publish", json={"catalogs": drafts}, headers=ADMIN_HEADERS)
    assert response.status_code == 200
    assert [item["name"] for item in response.json()] == ["warehouse-a", "warehouse-b"]
    inventory = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
    assert {item["name"]: item["options"] for item in inventory} == {
        name: draft["options"] for name, draft in drafts.items()
    }
    events = client.get("/v1/audit/events/page", headers=ADMIN_HEADERS).json()["items"]
    assert {
        event["details"]["name"]
        for event in events
        if event["action"] == "workspace.catalog.update"
    } == set(drafts)


@pytest.mark.parametrize(
    ("invalid", "status"),
    [
        pytest.param({"expected_revision": 9}, 409, id="late-revision-conflict"),
        pytest.param({"plugin_id": "unadmitted.catalog"}, 400, id="late-admission-failure"),
        pytest.param(
            {"options": {"uri": "https://user:private@catalog.invalid"}},
            400,
            id="late-secret-validation",
        ),
    ],
)
def test_catalog_release_failure_rolls_back_earlier_changes_and_audit(
    publication_client, invalid, status
):
    client = publication_client
    original = {
        "plugin_id": ICEBERG_CATALOG_ID,
        "options": {"uri": "sqlite:///original.db"},
        "expected_revision": 0,
    }
    assert (
        client.put("/v1/catalogs/z-existing", json=original, headers=ADMIN_HEADERS).status_code
        == 200
    )
    before = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
    audit = client.get("/v1/audit/events/page", headers=ADMIN_HEADERS).json()["items"]
    replacement = {**original, "options": {"uri": "sqlite:///changed.db"}, **invalid}
    response = client.post(
        "/v1/catalogs/publish",
        json={"catalogs": {"a-new": original, "z-existing": replacement}},
        headers=ADMIN_HEADERS,
    )
    assert response.status_code == status
    assert client.get("/v1/catalogs", headers=ADMIN_HEADERS).json() == before
    assert client.get("/v1/audit/events/page", headers=ADMIN_HEADERS).json()["items"] == audit
    assert "private" not in response.text


@pytest.mark.parametrize(
    "drafts",
    [
        {},
        {str(i): {"plugin_id": ICEBERG_CATALOG_ID, "expected_revision": 0} for i in range(101)},
        {"warehouse": {"plugin_id": ICEBERG_CATALOG_ID}},
    ],
)
def test_catalog_release_requires_bounded_explicit_revision_fences(publication_client, drafts):
    client = publication_client
    response = client.post("/v1/catalogs/publish", json={"catalogs": drafts}, headers=ADMIN_HEADERS)
    assert response.status_code == 422
    assert client.get("/v1/catalogs", headers=ADMIN_HEADERS).json() == []


def test_catalog_release_publication_requires_authenticated_admin(publication_client):
    client = publication_client
    response = client.post(
        "/v1/catalogs/publish",
        json={"catalogs": {"warehouse": {"plugin_id": ICEBERG_CATALOG_ID, "expected_revision": 0}}},
    )
    assert response.status_code == 401


@pytest.mark.integration
def test_postgres_catalog_release_is_not_visible_until_complete(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from dal_obscura.control.catalogs import service as catalog_service

    token = ADMIN_HEADERS["authorization"].removeprefix("Bearer ")
    with (
        isolated_postgres_sessions() as factory,
        TestClient(create_app(factory, admin_token=token)) as client,
    ):
        original = {
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"uri": "sqlite:///original.db"},
            "expected_revision": 0,
        }
        for name in ("a-existing", "z-existing"):
            assert (
                client.put(f"/v1/catalogs/{name}", json=original, headers=ADMIN_HEADERS).status_code
                == 200
            )
        before = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
        entered, release = Event(), Event()
        upsert = catalog_service.upsert_workspace_catalog

        def pause_after_first_write(store, **kwargs):
            result = upsert(store, **kwargs)
            if kwargs["name"] == "a-existing":
                entered.set()
                assert release.wait(10), "Publication was not released"
            return result

        monkeypatch.setattr(catalog_service, "upsert_workspace_catalog", pause_after_first_write)
        drafts = {
            name: {**original, "options": {"uri": "sqlite:///changed.db"}}
            for name in ("z-existing", "a-existing")
        }
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                client.post,
                "/v1/catalogs/publish",
                json={"catalogs": drafts},
                headers=ADMIN_HEADERS,
            )
            try:
                assert entered.wait(10), "Publication did not reach its first write"
                assert client.get("/v1/catalogs", headers=ADMIN_HEADERS).json() == before
            finally:
                release.set()
            assert future.result(timeout=10).status_code == 200
        after = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()
        assert [item["revision"] for item in after] == [1, 1]
        assert all(item["options"] == {"uri": "sqlite:///changed.db"} for item in after)


def test_workspace_catalog_upsert_bootstraps_default_workspace(client_factory):
    client = client_factory()

    response = client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
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
            "plugin_id": ICEBERG_CATALOG_ID,
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
    events = client.get("/v1/audit/events/page", headers=ADMIN_HEADERS).json()["items"]
    catalog_events = [event for event in events if event["action"] == "workspace.catalog.update"]
    assert catalog_events[0]["actor"] == "platform:admin"
    assert catalog_events[0]["details"] == {
        "name": "analytics",
        "plugin_id": ICEBERG_CATALOG_ID,
        "option_keys": ["type", "uri"],
    }


def test_workspace_catalog_upsert_accepts_plugin_defaults_without_backend_fields(client_factory):
    client = client_factory()

    response = client.put(
        "/v1/catalogs/analytics",
        json={"plugin_id": "iceberg.catalog", "options": {"uri": "sqlite:///catalog.db"}},
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 200
    catalog = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()[0]
    assert catalog["plugin_id"] == "iceberg.catalog"
    assert catalog["options"] == {"uri": "sqlite:///catalog.db"}


def test_workspace_catalog_rejects_python_module_identity(client_factory):
    client = client_factory()

    response = client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ("dal_obscura.adapters.providers.catalogs.IcebergCatalog"),
            "options": {"uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 422


def test_workspace_catalog_rejects_retired_module_field(client_factory):
    client = client_factory()

    response = client.put(
        "/v1/catalogs/analytics",
        json={"module": ICEBERG_CATALOG_ID, "options": {"uri": "sqlite:///catalog.db"}},
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 422


def test_workspace_catalog_upsert_rejects_a_stale_revision(client_factory):
    client = client_factory()
    first = client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    assert first.status_code == 200
    updated = client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog-new.db"},
            "expected_revision": 0,
        },
        headers=ADMIN_HEADERS,
    )
    stale = client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog-stale.db"},
            "expected_revision": 0,
        },
        headers=ADMIN_HEADERS,
    )
    assert updated.status_code == 200
    assert stale.status_code == 409
    assert "Catalog revision changed" in stale.json()["detail"]
    assert stale.json()["error"]["current_revision"] == 1


def test_workspace_catalog_update_requires_revision_precondition(client_factory):
    client = client_factory()
    created = client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    assert created.status_code == 200

    missing = client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog-new.db"},
        },
        headers=ADMIN_HEADERS,
    )

    assert missing.status_code == 428
    assert client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()[0]["revision"] == 0


def test_workspace_catalog_rejects_non_admitted_plugin_id(client_factory):
    client = client_factory()

    response = client.put(
        "/v1/catalogs/analytics",
        json={"plugin_id": "example.CustomCatalog", "options": {}},
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 422
    assert response.json()["error"]["field_errors"][0]["field"] == "plugin_id"


def test_workspace_catalog_rejects_credentials_embedded_in_uri(client_factory):
    client = client_factory()

    response = client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"uri": "https://catalog-user:catalog-password@catalog.example/api"},
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert "secret reference" in response.json()["detail"]


def test_workspace_catalog_rejects_malformed_uri_with_structured_error(client_factory):
    client = client_factory()

    response = client.put(
        "/v1/catalogs/analytics",
        json={"plugin_id": ICEBERG_CATALOG_ID, "options": {"uri": "https://[2001:db8::1/api"}},
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    payload = response.json()
    assert payload["error"]["code"] == "validation_error"
    assert payload["error"]["message"] == "Catalog option 'options.uri' contains an invalid URI"
    assert payload["error"]["request_id"]


def test_workspace_catalog_rejects_nested_dynamic_loader_options(client_factory):
    client = client_factory()

    response = client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"properties": {"py-catalog-impl": "example.CustomCatalog"}},
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert "cannot select an implementation class" in response.json()["detail"]


def test_workspace_catalog_rejects_unbounded_option_shape(client_factory):
    client = client_factory()

    deeply_nested: object = "value"
    for _ in range(18):
        deeply_nested = {"nested": deeply_nested}
    response = client.put(
        "/v1/catalogs/analytics",
        json={"plugin_id": ICEBERG_CATALOG_ID, "options": {"properties": deeply_nested}},
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert "too deeply nested" in response.json()["detail"]


def test_workspace_catalog_rejects_oversized_option_list(client_factory):
    client = client_factory()
    response = client.put(
        "/v1/catalogs/analytics",
        json={"plugin_id": ICEBERG_CATALOG_ID, "options": {"properties": ["x"] * 257}},
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 400
    assert "list is too large" in response.json()["detail"]


def test_workspace_catalog_rejects_inline_sensitive_options_but_accepts_secret_refs(client_factory):
    client = client_factory()

    rejected = client.put(
        "/v1/catalogs/analytics",
        json={"plugin_id": ICEBERG_CATALOG_ID, "options": {"password": "inline-password"}},
        headers=ADMIN_HEADERS,
    )
    accepted = client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"password": {"secret": "CATALOG_PASSWORD", "scope": "catalog:analytics"}},
        },
        headers=ADMIN_HEADERS,
    )

    assert rejected.status_code == 400
    assert "secret reference" in rejected.json()["detail"]
    assert accepted.status_code == 200, accepted.json()


def test_workspace_catalog_enforces_configured_egress_allowlist(http_client, db_engine):
    engine = db_engine
    client = http_client(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            catalog_egress_allowlist=("catalog.example",),
        )
    )

    rejected = client.put(
        "/v1/catalogs/analytics",
        json={"plugin_id": ICEBERG_CATALOG_ID, "options": {"uri": "https://other.example/api"}},
        headers=ADMIN_HEADERS,
    )
    accepted = client.put(
        "/v1/catalogs/analytics",
        json={"plugin_id": ICEBERG_CATALOG_ID, "options": {"uri": "https://catalog.example/api"}},
        headers=ADMIN_HEADERS,
    )

    assert rejected.status_code == 400
    assert "egress allowlist" in rejected.json()["detail"]
    assert accepted.status_code == 200, accepted.json()


def test_workspace_catalog_tables_can_be_discovered_without_runtime_ids(
    client_factory, monkeypatch
):
    client = client_factory()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "default.users", "options": {}},
        headers=ADMIN_HEADERS,
    )

    def fake_discover_catalog_tables(name, plugin_id, options, **kwargs):
        assert name == "analytics"
        assert plugin_id == ICEBERG_CATALOG_ID
        assert options == {"type": "sql", "uri": "sqlite:///catalog.db"}
        return DiscoveryPage(
            (TableIdentifier(("default",), "users"), TableIdentifier(("prod",), "orders"))
        )

    monkeypatch.setattr(
        "dal_obscura.control.catalogs.service.discover_public_catalog_tables",
        fake_discover_catalog_tables,
    )

    response = client.get("/v1/catalogs/analytics/tables", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json() == {
        "catalog": "analytics",
        "revision": 0,
        "next_cursor": None,
        "tables": [
            {
                "backend": ICEBERG_CATALOG_ID,
                "governed": True,
                "name": "default.users",
                "table_identifier": "default.users",
                "target": "default.users",
            },
            {
                "backend": ICEBERG_CATALOG_ID,
                "governed": False,
                "name": "prod.orders",
                "table_identifier": "prod.orders",
                "target": "prod.orders",
            },
        ],
    }
    assert "tenant" not in _keys_recursive(response.json())


@pytest.mark.parametrize("kind", ["environment", "mounted_file"])
def test_workspace_catalog_discovery_resolves_secret_references(
    client_factory, monkeypatch, tmp_path, kind
):
    config: dict[str, object] = {"scope_grants": {"catalog:analytics": ["CATALOG_TOKEN"]}}
    if kind == "mounted_file":
        directory = tmp_path / "secrets"
        directory.mkdir()
        (directory / "CATALOG_TOKEN").write_text("sentinel-secret")
        config["directory"] = str(directory)
    provider = load_secret_provider_from_environment(
        {
            "DAL_OBSCURA_SECRET_PROVIDER_KIND": kind,
            "DAL_OBSCURA_SECRET_PROVIDER_CONFIG": json.dumps(config),
        },
        require_scope_grants=True,
    )
    client = client_factory(secret_provider=provider)
    monkeypatch.setenv("CATALOG_TOKEN", "sentinel-secret")
    client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {
                "uri": "https://catalog.example/api",
                "token": {"secret": "CATALOG_TOKEN", "scope": "catalog:analytics"},
            },
        },
        headers=ADMIN_HEADERS,
    )
    received: dict[str, object] = {}

    def fake_discover_catalog_tables(name, plugin_id, options, **kwargs):
        received.update(options)
        return DiscoveryPage(())

    monkeypatch.setattr(
        "dal_obscura.control.catalogs.service.discover_public_catalog_tables",
        fake_discover_catalog_tables,
    )

    response = client.get("/v1/catalogs/analytics/tables", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert received["token"] == "sentinel-secret"
    reference = {"secret": "CATALOG_TOKEN", "scope": "catalog:analytics"}
    catalog = client.get("/v1/catalogs", headers=ADMIN_HEADERS)
    assert catalog.status_code == 200
    analytics = next(item for item in catalog.json() if item["name"] == "analytics")
    assert analytics["options"]["token"] == reference
    assert "sentinel-secret" not in catalog.text
    assert "sentinel-secret" not in response.text
    if kind == "mounted_file":
        replacement = directory / "replacement"
        replacement.write_text("rotated-secret")
        replacement.replace(directory / "CATALOG_TOKEN")
        rotated = client.get("/v1/catalogs/analytics/tables", headers=ADMIN_HEADERS)
        assert rotated.status_code == 200
        assert received["token"] == "rotated-secret"
        assert "rotated-secret" not in rotated.text


def test_workspace_catalog_discovery_does_not_echo_provider_errors(client_factory, monkeypatch):
    client = client_factory()
    client.put(
        "/v1/catalogs/analytics",
        json={"plugin_id": ICEBERG_CATALOG_ID, "options": {"uri": "https://catalog.example/api"}},
        headers=ADMIN_HEADERS,
    )

    def failing_discovery(name, plugin_id, options, **kwargs):
        raise ValueError(
            "failed to connect https://catalog-user:catalog-password@catalog.example/api"
        )

    monkeypatch.setattr(
        "dal_obscura.control.catalogs.service.discover_public_catalog_tables",
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


def test_workspace_catalog_diagnostics_are_bounded_and_redacted(client_factory, monkeypatch):
    client = client_factory()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )

    def fake_discover_catalog_tables(name, plugin_id, options, **kwargs):
        assert name == "analytics"
        assert plugin_id == ICEBERG_CATALOG_ID
        assert options == {"type": "sql", "uri": "sqlite:///catalog.db"}
        return DiscoveryPage((TableIdentifier(("default",), "users"),))

    monkeypatch.setattr(
        "dal_obscura.control.catalogs.service.discover_public_catalog_tables",
        fake_discover_catalog_tables,
    )

    ready = client.get("/v1/catalogs/analytics/diagnostics", headers=ADMIN_HEADERS)

    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"
    assert ready.json()["catalog"] == "analytics"
    assert ready.json()["table_count"] == 1
    assert ready.json()["sample_tables"] == ["default.users"]
    assert "options" not in _keys_recursive(ready.json())

    def failing_discover_catalog_tables(name, plugin_id, options, **kwargs):
        raise RuntimeError("failed to connect with password=super-secret")

    monkeypatch.setattr(
        "dal_obscura.control.catalogs.service.discover_public_catalog_tables",
        failing_discover_catalog_tables,
    )
    failed = client.get("/v1/catalogs/analytics/diagnostics", headers=ADMIN_HEADERS)

    assert failed.status_code == 200
    assert failed.json()["status"] == "unavailable"
    assert failed.json()["message"] == "Catalog discovery failed"
    assert "super-secret" not in failed.text
