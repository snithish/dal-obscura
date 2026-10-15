from __future__ import annotations

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from dal_obscura.adapters.providers import governance as schema_service
from tests.interfaces.http.schema_cases import _provision_reviewable_asset
from tests.support.control.workspace import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_ID,
)


def test_schema_discovery_releases_database_transaction_before_provider_io(
    client_factory, db_engine, monkeypatch
):
    from sqlalchemy import event

    client = client_factory()
    asset = _provision_reviewable_asset(client)
    active = set()
    observed = []

    def begin(connection):
        active.add(id(connection))

    def end(connection):
        active.discard(id(connection))

    def discover(**kwargs):
        observed.append(len(active))
        return pa.schema([pa.field("id", pa.int64())])

    event.listen(db_engine, "begin", begin)
    event.listen(db_engine, "commit", end)
    event.listen(db_engine, "rollback", end)
    monkeypatch.setattr(schema_service, "load_source_schema", discover)
    try:
        response = client.get(f"/v1/assets/{asset['id']}/schema", headers=ADMIN_HEADERS)
        assert response.status_code == 200, response.json()
        assert response.json()["fields"][0]["human_path"] == "id"
        assert observed == [0]
        assert active == set()
    finally:
        event.remove(db_engine, "begin", begin)
        event.remove(db_engine, "commit", end)
        event.remove(db_engine, "rollback", end)


@pytest.mark.integration
@pytest.mark.parametrize("change", ["catalog", "asset"])
def test_postgres_schema_discovery_rejects_configuration_changed_during_provider_io(
    change, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from dal_obscura.bootstrap.control import build_control_app
    from tests.support.storage.postgres import isolated_postgres_sessions

    token = ADMIN_HEADERS["authorization"].removeprefix("Bearer ")
    with (
        isolated_postgres_sessions() as factory,
        TestClient(build_control_app(factory, admin_token=token)) as client,
    ):
        asset = _provision_reviewable_asset(client)
        before = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()
        catalog = client.get("/v1/catalogs", headers=ADMIN_HEADERS).json()[0]
        entered, release = Event(), Event()
        observed = []

        def discover(**kwargs):
            observed.append((kwargs["asset"]["table_identifier"], kwargs["catalog"]["revision"]))
            entered.set()
            assert release.wait(10), "Schema provider was not released"
            return pa.schema([pa.field("id", pa.int64())])

        monkeypatch.setattr(schema_service, "load_source_schema", discover)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                client.get, f"/v1/assets/{asset['id']}/schema", headers=ADMIN_HEADERS
            )
            try:
                assert entered.wait(10), "Schema discovery did not reach provider IO"
                if change == "catalog":
                    updated = client.put(
                        "/v1/catalogs/analytics",
                        json={
                            "plugin_id": ICEBERG_CATALOG_ID,
                            "options": {"type": "sql", "uri": "sqlite:///changed.db"},
                            "expected_revision": catalog["revision"],
                        },
                        headers=ADMIN_HEADERS,
                    )
                    expected_revision = catalog["revision"] + 1
                else:
                    updated = client.put(
                        "/v1/assets/analytics/default.users",
                        json={
                            "backend": "iceberg",
                            "table_identifier": "prod.changed",
                            "options": {},
                            "expected_revision": before["revision"],
                        },
                        headers=ADMIN_HEADERS,
                    )
                    expected_revision = before["revision"] + 1
                assert updated.status_code == 200, updated.json()
            finally:
                release.set()
            response = future.result(timeout=10)
        assert observed == [("prod.users", catalog["revision"])]
        assert response.status_code == 409, response.json()
        assert response.json()["error"]["current_revision"] == expected_revision
        assert "fields" not in response.json()


@pytest.mark.integration
def test_postgres_schema_discovery_rechecks_read_capability_after_provider_io(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from dal_obscura.bootstrap.control import build_control_app
    from tests.support.storage.postgres import isolated_postgres_sessions

    token = ADMIN_HEADERS["authorization"].removeprefix("Bearer ")
    with (
        isolated_postgres_sessions() as factory,
        TestClient(
            build_control_app(
                factory,
                admin_token=token,
                oidc_actor_resolver=lambda token: {"principal": "user1", "groups": []},
            )
        ) as client,
    ):
        asset = _provision_reviewable_asset(client)
        before = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()
        entered, release = Event(), Event()

        def discover(**kwargs):
            entered.set()
            assert release.wait(10), "Schema provider was not released"
            return pa.schema([pa.field("id", pa.int64())])

        monkeypatch.setattr(schema_service, "load_source_schema", discover)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                client.get,
                f"/v1/assets/{asset['id']}/schema",
                headers={"authorization": "Bearer reader-token"},
            )
            try:
                assert entered.wait(10), "Schema discovery did not reach provider IO"
                updated = client.put(
                    f"/v1/assets/{asset['id']}/owners",
                    json={"owners": ["replacement-owner"], "expected_revision": before["revision"]},
                    headers=ADMIN_HEADERS,
                )
                assert updated.status_code == 200, updated.json()
            finally:
                release.set()
            response = future.result(timeout=10)
        assert response.status_code == 403, response.json()
        assert "fields" not in response.json()


@pytest.fixture
def schema_queries(db_engine):
    from sqlalchemy import event

    queries = []

    def record(connection, cursor, statement, parameters, context, many):
        queries.append(statement.lower())

    event.listen(db_engine, "before_cursor_execute", record)
    try:
        yield queries
    finally:
        event.remove(db_engine, "before_cursor_execute", record)


@pytest.mark.parametrize("operation", ["schema", "evaluation"])
def test_schema_work_does_not_load_admitted_fields_and_reads_policy_only_when_needed(
    operation, client_factory, monkeypatch, schema_queries
):
    client = client_factory()
    asset = _provision_reviewable_asset(client)
    monkeypatch.setattr(
        schema_service, "load_source_schema", lambda **kwargs: pa.schema([("id", pa.int64())])
    )
    schema_queries.clear()
    if operation == "schema":
        response = client.get(f"/v1/assets/{asset['id']}/schema", headers=ADMIN_HEADERS)
        assert response.status_code == 200, response.json()
        assert response.json()["fields"][0]["human_path"] == "id"
    else:
        response = client.post(
            f"/v1/assets/{asset['id']}/policy-evaluate",
            json={"principal": "user1", "rows": [{"id": 7}]},
            headers=ADMIN_HEADERS,
        )
        assert response.status_code == 200, response.json()
        assert response.json()["rows"] == [{"id": 7}]
    assert not any("asset_schema_fields" in query for query in schema_queries)
    assert sum("policy_rules" in query for query in schema_queries) == (
        0 if operation == "schema" else 1
    )
