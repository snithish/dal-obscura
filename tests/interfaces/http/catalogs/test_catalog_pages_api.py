from __future__ import annotations

import pytest
from sqlalchemy import event

from tests.support.control.workspace import ADMIN_HEADERS
from tests.support.sources.discovery import PagedCatalogRegistry


def _configure(client, name="analytics"):
    result = client.put(
        f"/v1/catalogs/{name}",
        headers=ADMIN_HEADERS,
        json={"plugin_id": "fixture.catalog", "options": {}},
    )
    assert result.status_code == 200, result.text


def test_catalog_pages_bound_provider_work_and_mark_only_page_assets(client_factory):
    registry = PagedCatalogRegistry(total=100_000)
    client = client_factory(plugin_registry=registry)
    _configure(client)
    saved = client.put(
        "/v1/assets/analytics/friendly-name",
        headers=ADMIN_HEADERS,
        json={"backend": "iceberg", "table_identifier": "default.table_000002", "options": {}},
    )
    assert saved.status_code == 200, saved.text
    first = client.get("/v1/catalogs/analytics/tables?limit=2", headers=ADMIN_HEADERS)
    assert first.status_code == 200, first.text
    assert [table["name"] for table in first.json()["tables"]] == [
        "default.table_000000",
        "default.table_000001",
    ]
    cursor = first.json()["next_cursor"]
    second = client.get(
        "/v1/catalogs/analytics/tables",
        headers=ADMIN_HEADERS,
        params={"limit": 2, "cursor": cursor},
    )
    assert second.status_code == 200, second.text
    assert [(table["name"], table["governed"]) for table in second.json()["tables"]] == [
        ("default.table_000002", True),
        ("default.table_000003", False),
    ]
    assert second.json()["revision"] == 0
    assert registry.requests == [(None, 2), ("2", 2)]
    assert registry.closed == 2


@pytest.mark.parametrize("change", ["tampered", "catalog", "revision", "caller", "limit"])
def test_catalog_cursor_rejects_scope_changes_before_provider_io(client_factory, change):
    registry = PagedCatalogRegistry()
    client = client_factory(
        plugin_registry=registry,
        oidc_actor_resolver=lambda token: {
            "principal": token,
            "groups": ["admins"],
            "issuer": "https://issuer.example",
        },
        oidc_admin_group="admins",
    )
    _configure(client)
    _configure(client, "other")
    first = client.get("/v1/catalogs/analytics/tables?limit=2", headers=ADMIN_HEADERS)
    cursor = first.json()["next_cursor"]
    headers = ADMIN_HEADERS
    name = "analytics"
    limit = 2
    if change == "tampered":
        cursor = cursor[:30] + ("A" if cursor[30] != "A" else "B") + cursor[31:]
    elif change == "catalog":
        name = "other"
    elif change == "revision":
        updated = client.put(
            "/v1/catalogs/analytics",
            headers=ADMIN_HEADERS,
            json={
                "plugin_id": "fixture.catalog",
                "options": {"region": "next"},
                "expected_revision": 0,
            },
        )
        assert updated.status_code == 200
    elif change == "caller":
        headers = {"Authorization": "Bearer another-admin"}
    else:
        limit = 3
    response = client.get(
        f"/v1/catalogs/{name}/tables",
        headers=headers,
        params={"limit": limit, "cursor": cursor},
    )
    assert response.status_code == 400, response.text
    assert response.json()["detail"] == "Discovery cursor is invalid or expired; restart discovery"
    assert registry.requests == [(None, 2)]


def test_catalog_discovery_releases_database_transaction_before_provider_io(
    client_factory,
    db_engine,
):
    active = 0

    def began(connection):
        nonlocal active
        active += 1

    def ended(connection):
        nonlocal active
        active -= 1

    def check_no_transaction():
        assert active == 0, "provider IO must not retain a database transaction"

    event.listen(db_engine, "begin", began)
    event.listen(db_engine, "commit", ended)
    event.listen(db_engine, "rollback", ended)
    try:
        registry = PagedCatalogRegistry(on_list=check_no_transaction)
        client = client_factory(plugin_registry=registry)
        _configure(client)
        response = client.get("/v1/catalogs/analytics/tables", headers=ADMIN_HEADERS)
        assert response.status_code == 200, response.text
    finally:
        event.remove(db_engine, "begin", began)
        event.remove(db_engine, "commit", ended)
        event.remove(db_engine, "rollback", ended)


def test_catalog_diagnostics_sample_one_page_without_counting_the_inventory(client_factory):
    registry = PagedCatalogRegistry()
    client = client_factory(plugin_registry=registry)
    _configure(client)
    result = client.get("/v1/catalogs/analytics/diagnostics", headers=ADMIN_HEADERS)
    assert result.status_code == 200, result.text
    assert result.json()["status"] == "ready"
    assert result.json()["table_count"] is None
    assert result.json()["sample_tables"] == [f"default.table_{i:06}" for i in range(10)]
    assert registry.requests == [(None, 10)]


def test_catalog_cursor_expires_before_provider_io(client_factory, monkeypatch):
    import time

    from dal_obscura.control.catalogs.discovery import CURSOR_TTL_SECONDS

    registry = PagedCatalogRegistry()
    client = client_factory(plugin_registry=registry)
    _configure(client)
    first = client.get("/v1/catalogs/analytics/tables?limit=2", headers=ADMIN_HEADERS)
    now = time.time()
    monkeypatch.setattr(
        "dal_obscura.control.catalogs.discovery.time.time", lambda: now + CURSOR_TTL_SECONDS + 2
    )
    expired = client.get(
        "/v1/catalogs/analytics/tables",
        headers=ADMIN_HEADERS,
        params={"limit": 2, "cursor": first.json()["next_cursor"]},
    )
    assert expired.status_code == 400
    assert registry.requests == [(None, 2)]


def test_catalog_cursor_can_move_between_replicas_with_the_same_key(client_factory):
    registry = PagedCatalogRegistry(total=3)
    first_client = client_factory(plugin_registry=registry, catalog_cursor_secret=b"x" * 32)
    second_client = client_factory(plugin_registry=registry, catalog_cursor_secret=b"x" * 32)
    _configure(first_client)
    first = first_client.get("/v1/catalogs/analytics/tables?limit=2", headers=ADMIN_HEADERS)
    second = second_client.get(
        "/v1/catalogs/analytics/tables",
        headers=ADMIN_HEADERS,
        params={"limit": 2, "cursor": first.json()["next_cursor"]},
    )
    assert second.status_code == 200, second.text
    assert [table["name"] for table in second.json()["tables"]] == ["default.table_000002"]
    assert second.json()["next_cursor"] is None


def test_catalog_changes_during_provider_io_reject_the_page(client_factory):
    registry = PagedCatalogRegistry(total=1)
    client = client_factory(plugin_registry=registry)
    _configure(client)

    def change_catalog():
        changed = client.put(
            "/v1/catalogs/analytics",
            headers=ADMIN_HEADERS,
            json={
                "plugin_id": "fixture.catalog",
                "options": {"region": "next"},
                "expected_revision": 0,
            },
        )
        assert changed.status_code == 200

    registry.on_list = change_catalog
    response = client.get("/v1/catalogs/analytics/tables", headers=ADMIN_HEADERS)
    assert response.status_code == 409
    assert response.json()["detail"] == "Catalog changed during discovery; restart discovery"
