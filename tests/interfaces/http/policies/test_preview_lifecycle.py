from __future__ import annotations

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from dal_obscura.adapters.duckdb.transform import DuckDBRowTransformAdapter
from dal_obscura.adapters.providers import governance as schema_service
from tests.interfaces.http.schema_cases import _provision_reviewable_asset
from tests.support.control.workspace import (
    ADMIN_HEADERS,
)


def test_policy_evaluation_releases_transactions_before_provider_and_duckdb(
    client_factory, db_engine, monkeypatch
):
    from sqlalchemy import event

    client = client_factory()
    asset = _provision_reviewable_asset(client)
    active, observed = set(), []

    def begin(connection):
        active.add(id(connection))

    def end(connection):
        active.discard(id(connection))

    def discover(**kwargs):
        observed.append(("provider", len(active)))
        return pa.schema([pa.field("id", pa.int64())])

    adapter = DuckDBRowTransformAdapter
    transform = adapter.apply_filters_and_masks_stream

    def execute(*args, **kwargs):
        observed.append(("duckdb", len(active)))
        return transform(*args, **kwargs)

    event.listen(db_engine, "begin", begin)
    event.listen(db_engine, "commit", end)
    event.listen(db_engine, "rollback", end)
    monkeypatch.setattr(schema_service, "load_source_schema", discover)
    monkeypatch.setattr(adapter, "apply_filters_and_masks_stream", execute)
    try:
        response = client.post(
            f"/v1/assets/{asset['id']}/policy-evaluate",
            json={"principal": "user1", "rows": [{"id": 7}]},
            headers=ADMIN_HEADERS,
        )
        assert response.status_code == 200, response.json()
        assert response.json()["rows"] == [{"id": 7}]
        assert observed == [("provider", 0), ("duckdb", 0)]
        assert active == set()
    finally:
        event.remove(db_engine, "begin", begin)
        event.remove(db_engine, "commit", end)
        event.remove(db_engine, "rollback", end)


@pytest.mark.integration
def test_postgres_policy_evaluation_rejects_policy_changed_during_provider_io(monkeypatch):
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
        entered, release = Event(), Event()

        def discover(**kwargs):
            entered.set()
            assert release.wait(10), "Evaluation provider was not released"
            return pa.schema([pa.field("id", pa.int64())])

        monkeypatch.setattr(schema_service, "load_source_schema", discover)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                client.post,
                f"/v1/assets/{asset['id']}/policy-evaluate",
                json={"principal": "user1", "rows": [{"id": 7}]},
                headers=ADMIN_HEADERS,
            )
            try:
                assert entered.wait(10), "Evaluation did not reach provider IO"
                updated = client.put(
                    f"/v1/assets/{asset['id']}/policy",
                    json={
                        "expected_revision": before["policy_revision"],
                        "rules": [
                            {
                                "ordinal": 10,
                                "effect": "allow",
                                "principals": ["user1"],
                                "columns": ["id"],
                                "masks": {"id": {"type": "null"}},
                            }
                        ],
                    },
                    headers=ADMIN_HEADERS,
                )
                assert updated.status_code == 200, updated.json()
            finally:
                release.set()
            response = future.result(timeout=10)
        assert response.status_code == 409, response.json()
        assert response.json()["error"]["current_revision"] == before["policy_revision"] + 1
        assert "rows" not in response.json()


@pytest.mark.integration
def test_postgres_policy_evaluation_rejects_mapping_changed_during_provider_io(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from dal_obscura.bootstrap.control import build_control_app
    from tests.support.control.workspace import DEFAULT_AUTH_MODULE
    from tests.support.storage.postgres import isolated_postgres_sessions

    token = ADMIN_HEADERS["authorization"].removeprefix("Bearer ")
    with (
        isolated_postgres_sessions() as factory,
        TestClient(build_control_app(factory, admin_token=token)) as client,
    ):
        asset = _provision_reviewable_asset(client)
        provider = {
            "ordinal": 1,
            "module": DEFAULT_AUTH_MODULE,
            "args": {"issuer": "https://issuer.example", "subject_claim": "sub"},
        }
        saved = client.put(
            "/v1/settings/auth-providers",
            headers=ADMIN_HEADERS,
            json={"expected_revision": 0, "providers": [provider]},
        )
        assert saved.status_code == 200, saved.json()
        revision = client.get("/v1/settings/auth-providers/revision", headers=ADMIN_HEADERS).json()[
            "revision"
        ]
        entered, release = Event(), Event()

        def discover(**kwargs):
            entered.set()
            assert release.wait(10), "Evaluation provider was not released"
            return pa.schema([pa.field("id", pa.int64())])

        monkeypatch.setattr(schema_service, "load_source_schema", discover)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                client.post,
                f"/v1/assets/{asset['id']}/policy-evaluate",
                json={
                    "principal": "ignored",
                    "provider_ordinal": 1,
                    "claims": {"sub": "user1", "other": "user1"},
                    "rows": [{"id": 7}],
                },
                headers=ADMIN_HEADERS,
            )
            try:
                assert entered.wait(10), "Evaluation did not reach provider IO"
                updated = client.put(
                    "/v1/settings/auth-providers",
                    headers=ADMIN_HEADERS,
                    json={
                        "expected_revision": revision,
                        "providers": [
                            {**provider, "args": {**provider["args"], "subject_claim": "other"}}
                        ],
                    },
                )
                assert updated.status_code == 200, updated.json()
            finally:
                release.set()
            response = future.result(timeout=10)
        assert response.status_code == 409, response.json()
        assert response.json()["error"]["current_revision"] == revision + 1
        assert "rows" not in response.json()


@pytest.mark.integration
def test_postgres_policy_evaluation_rechecks_read_capability_after_duckdb(monkeypatch):
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
        revision = client.get(f"/v1/assets/{asset['id']}", headers=ADMIN_HEADERS).json()["revision"]
        entered, release = Event(), Event()
        monkeypatch.setattr(
            schema_service,
            "load_source_schema",
            lambda **kwargs: pa.schema([pa.field("id", pa.int64())]),
        )
        adapter = DuckDBRowTransformAdapter
        transform = adapter.apply_filters_and_masks_stream

        def execute(*args, **kwargs):
            entered.set()
            assert release.wait(10), "Evaluation transform was not released"
            return transform(*args, **kwargs)

        monkeypatch.setattr(adapter, "apply_filters_and_masks_stream", execute)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                client.post,
                f"/v1/assets/{asset['id']}/policy-evaluate",
                json={"principal": "user1", "rows": [{"id": 7}]},
                headers={"authorization": "Bearer reader-token"},
            )
            try:
                assert entered.wait(10), "Evaluation did not reach DuckDB"
                updated = client.put(
                    f"/v1/assets/{asset['id']}/owners",
                    headers=ADMIN_HEADERS,
                    json={"owners": ["replacement-owner"], "expected_revision": revision},
                )
                assert updated.status_code == 200, updated.json()
            finally:
                release.set()
            response = future.result(timeout=10)
        assert response.status_code == 403, response.json()
        assert "rows" not in response.json()
