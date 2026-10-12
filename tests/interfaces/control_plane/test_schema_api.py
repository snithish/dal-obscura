from __future__ import annotations

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, StructType

from dal_obscura.control import schema_service
from dal_obscura.interfaces.http.routes.schemas import PolicyEvaluationResponse
from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_ID,
)
from tests.support.schema_catalog import EvaluationCatalog


@pytest.mark.parametrize(
    ("payload", "field"),
    [
        ({"principal": "analyst", "columns": ["email"]}, "columns"),
        ({"principal": "   "}, "principal"),
        ({"principal": "analyst", "groups": [" "]}, "groups.0"),
    ],
)
def test_policy_evaluation_rejects_ignored_or_blank_inputs(client_factory, payload, field):
    client = client_factory()
    response = client.post(
        "/v1/assets/00000000-0000-4000-8000-000000000001/policy-evaluate",
        json=payload,
        headers=ADMIN_HEADERS,
    )
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "validation_error"
    assert error["field_errors"][0]["field"] == field
    assert "input" not in error["field_errors"][0]


class _FakeTable:
    def schema(self) -> Schema:
        return Schema(
            NestedField(
                field_id=1,
                name="profile",
                field_type=StructType(
                    NestedField(field_id=2, name="email", field_type=StringType()),
                ),
            ),
            NestedField(field_id=3, name="id", field_type=LongType()),
        )


class _FakeCatalog:
    def load_table(self, identifier: str) -> _FakeTable:
        assert identifier == "prod.users"
        return _FakeTable()


class _ChangedEvaluationTable:
    def schema(self) -> Schema:
        return Schema(
            NestedField(field_id=1, name="id", field_type=LongType()),
            NestedField(field_id=2, name="email", field_type=StringType()),
            NestedField(field_id=4, name="region", field_type=StringType()),
        )


class _ChangedEvaluationCatalog:
    def load_table(self, identifier: str) -> _ChangedEvaluationTable:
        assert identifier == "prod.users"
        return _ChangedEvaluationTable()


def _provision_reviewable_asset(client: TestClient) -> dict[str, object]:
    client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
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
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["user1"], "expected_revision": 0},
        headers=ADMIN_HEADERS,
    )
    saved = client.put(
        f"/v1/assets/{asset['id']}/policy",
        json={
            "expected_revision": 0,
            "rules": [
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["user1"],
                    "columns": ["id"],
                    "masks": {},
                    "row_filter": None,
                }
            ],
        },
        headers=ADMIN_HEADERS,
    )
    assert saved.status_code == 200, saved.json()
    return asset


def test_asset_schema_route_reads_authoritative_iceberg_schema(client_factory, monkeypatch) -> None:
    client = client_factory()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers=ADMIN_HEADERS,
    )
    asset = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {}},
        headers=ADMIN_HEADERS,
    ).json()
    monkeypatch.setattr(
        schema_service, "load_source_schema", lambda **kwargs: _FakeTable().schema().as_arrow()
    )

    response = client.get(f"/v1/assets/{asset['id']}/schema", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json()["fields"][0]["children"][0]["human_path"] == "profile.email"
    assert response.json()["fields"][1]["path"]["segments"][0]["field_id"] == 3


def test_asset_schema_route_returns_validation_error_for_response_amplification(
    client_factory, monkeypatch
) -> None:
    client = client_factory()
    asset = _provision_reviewable_asset(client)
    schema = pa.schema(
        [
            pa.field(
                "x" * 100_000,
                pa.struct([pa.field(f"child_{index}", pa.string()) for index in range(100)]),
            )
        ]
    )
    monkeypatch.setattr(schema_service, "load_source_schema", lambda **kwargs: schema)

    response = client.get(f"/v1/assets/{asset['id']}/schema", headers=ADMIN_HEADERS)

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "validation_error"
    assert response.json()["error"]["message"] == (
        "Schema response exceeds the 8388608-byte budget"
    )
    assert "fields" not in response.json()


def test_policy_evaluation_response_rejects_internal_schema_field_name() -> None:
    payload = {
        "status": "completed",
        "decision": "allow",
        "allowed_columns": [],
        "masks": [],
        "row_filter": None,
        "input_rows": 0,
        "output_rows": 0,
        "schema_text": "id: int64",
        "policy_revision": 0,
        "rows": [],
        "evidence": {},
    }

    with pytest.raises(ValidationError) as exc_info:
        PolicyEvaluationResponse.model_validate(payload)

    assert any(error["loc"] == ("schema",) for error in exc_info.value.errors())


def test_policy_evaluation_returns_duckdb_transformed_synthetic_rows(
    client_factory, monkeypatch
) -> None:
    client = client_factory()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "plugin_id": ICEBERG_CATALOG_ID,
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
        f"/v1/assets/{asset['id']}/policy",
        json={
            "expected_revision": 0,
            "rules": [
                {
                    "ordinal": 1,
                    "effect": "allow",
                    "principals": ["other-reader"],
                    "when": {},
                    "columns": ["id", "email", "region"],
                    "masks": {"email": {"type": "redact", "value": "[wrong]"}},
                    "row_filter": "region = 'us'",
                },
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["analyst"],
                    "when": {},
                    "columns": ["id", "email", "region"],
                    "masks": {"email": {"type": "redact", "value": "[right]"}},
                    "row_filter": "region = 'us'",
                },
            ],
        },
        headers=ADMIN_HEADERS,
    )
    monkeypatch.setattr(
        schema_service,
        "load_source_schema",
        lambda **kwargs: EvaluationCatalog().load_table("prod.users").schema().as_arrow(),
    )

    response = client.post(
        f"/v1/assets/{asset['id']}/policy-evaluate",
        json={
            "principal": "analyst",
            "rows": [
                {"id": 1, "email": "alice@example.com", "region": "us"},
                {"id": 2, "email": "bob@example.com", "region": "eu"},
            ],
        },
        headers=ADMIN_HEADERS,
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["decision"] == "allow"
    assert isinstance(payload["schema"], str)
    assert "schema_text" not in payload
    assert payload["input_rows"] == 2
    assert payload["output_rows"] == 1
    assert payload["rows"][0]["email"] == "[right]"
    assert payload["evidence"]["evaluator_version"] == "duckdb-synthetic-v1"
    assert len(payload["evidence"]["fixture_fingerprint"]) == 64

    empty = client.post(
        f"/v1/assets/{asset['id']}/policy-evaluate",
        json={"principal": "analyst", "rows": []},
        headers=ADMIN_HEADERS,
    )
    assert empty.status_code == 200
    assert empty.json()["input_rows"] == 0
    assert empty.json()["output_rows"] == 0
    assert (
        empty.json()["evidence"]["fixture_fingerprint"]
        != payload["evidence"]["fixture_fingerprint"]
    )

    invalid = client.post(
        f"/v1/assets/{asset['id']}/policy-evaluate",
        json={"principal": "analyst", "rows": [{"id": "not-a-long"}]},
        headers=ADMIN_HEADERS,
    )
    assert invalid.status_code == 400
    invalid_payload = invalid.json()
    assert invalid_payload["detail"] == "Synthetic evaluation rows are invalid"
    assert invalid_payload["error"]["code"] == "validation_error"
    assert invalid_payload["error"]["request_id"]


def test_asset_schema_route_preserves_bounded_metadata_beyond_native_execution_depth(
    client_factory, monkeypatch
):
    from tests.support.nested_schema import nested_struct_schema

    client = client_factory()
    asset = _provision_reviewable_asset(client)
    monkeypatch.setattr(
        schema_service, "load_source_schema", lambda **kwargs: nested_struct_schema(63)
    )
    response = client.get(f"/v1/assets/{asset['id']}/schema", headers=ADMIN_HEADERS)
    assert response.status_code == 200
    assert response.json()["fields"][0]["name"] == "root"
    assert response.json()["fields"][0]["children"][0]["name"] == "level_62"


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

    from dal_obscura.interfaces.http.app import create_app
    from tests.support.postgres import isolated_postgres_sessions

    token = ADMIN_HEADERS["authorization"].removeprefix("Bearer ")
    with (
        isolated_postgres_sessions() as factory,
        TestClient(create_app(factory, admin_token=token)) as client,
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

    from dal_obscura.interfaces.http.app import create_app
    from tests.support.postgres import isolated_postgres_sessions

    token = ADMIN_HEADERS["authorization"].removeprefix("Bearer ")
    with (
        isolated_postgres_sessions() as factory,
        TestClient(
            create_app(
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


def test_policy_evaluation_releases_transactions_before_provider_and_duckdb(
    client_factory, db_engine, monkeypatch
):
    from sqlalchemy import event

    from dal_obscura.control import evaluation_service

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

    adapter = evaluation_service.DuckDBRowTransformAdapter
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


def test_policy_evaluation_rejects_oversized_rows_before_provider_io(client_factory, monkeypatch):
    client = client_factory(max_request_bytes=4 * 1024 * 1024)
    asset = _provision_reviewable_asset(client)
    monkeypatch.setattr(
        schema_service,
        "load_source_schema",
        lambda **kwargs: pytest.fail("Invalid rows reached provider IO"),
    )
    response = client.post(
        f"/v1/assets/{asset['id']}/policy-evaluate",
        json={"principal": "user1", "rows": [{"payload": "x" * (2 * 1024 * 1024)}]},
        headers=ADMIN_HEADERS,
    )
    assert response.status_code == 400, response.json()
    assert (
        response.json()["error"]["message"]
        == "Synthetic evaluation accepts at most 2097152 encoded bytes"
    )


@pytest.mark.integration
def test_postgres_policy_evaluation_rejects_policy_changed_during_provider_io(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from dal_obscura.interfaces.http.app import create_app
    from tests.support.postgres import isolated_postgres_sessions

    token = ADMIN_HEADERS["authorization"].removeprefix("Bearer ")
    with (
        isolated_postgres_sessions() as factory,
        TestClient(create_app(factory, admin_token=token)) as client,
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

    from dal_obscura.interfaces.http.app import create_app
    from tests.interfaces.control_plane.workspace_helpers import DEFAULT_AUTH_MODULE
    from tests.support.postgres import isolated_postgres_sessions

    token = ADMIN_HEADERS["authorization"].removeprefix("Bearer ")
    with (
        isolated_postgres_sessions() as factory,
        TestClient(create_app(factory, admin_token=token)) as client,
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

    from dal_obscura.control import evaluation_service
    from dal_obscura.interfaces.http.app import create_app
    from tests.support.postgres import isolated_postgres_sessions

    token = ADMIN_HEADERS["authorization"].removeprefix("Bearer ")
    with (
        isolated_postgres_sessions() as factory,
        TestClient(
            create_app(
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
        adapter = evaluation_service.DuckDBRowTransformAdapter
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


def test_evaluation_capture_detaches_synthetic_inputs_before_work(client_factory):
    from uuid import UUID

    from dal_obscura.control import evaluation_service
    from dal_obscura.control.access import ControlPlaneActor

    client = client_factory()
    asset = _provision_reviewable_asset(client)
    rows: list[dict[str, object]] = [{"id": 7}]
    groups = ["initial-group"]
    claims: dict[str, object] = {"department": "Finance"}
    with client.app.state.test_session_factory() as store:
        captured = evaluation_service.capture_asset_evaluation(
            store,
            UUID(str(asset["id"])),
            ControlPlaneActor.for_platform_admin("admin"),
            principal="user1",
            groups=groups,
            claims=claims,
            rows=rows,
        )
        store.commit()
    rows[0]["id"] = 999
    groups.append("later-group")
    claims["department"] = "Other"
    result = evaluation_service.evaluate_asset_policy(
        captured, pa.schema([pa.field("id", pa.int64())])
    )
    payload = PolicyEvaluationResponse.model_validate(result)
    assert payload.rows == [{"id": 7}]
    assert payload.evidence["identity"] == {
        "principal": "user1",
        "groups": ["initial-group"],
        "attributes": {"department": "Finance"},
    }


def test_policy_evaluation_rejects_nonfinite_json_before_provider_io(client_factory, monkeypatch):
    client = client_factory()
    asset = _provision_reviewable_asset(client)
    entered = []

    def discover(**kwargs):
        entered.append(True)
        raise ValueError("Invalid JSON reached provider IO")

    monkeypatch.setattr(schema_service, "load_source_schema", discover)
    response = client.post(
        f"/v1/assets/{asset['id']}/policy-evaluate",
        content='{"principal":"user1","rows":[{"id":NaN}]}',
        headers={**ADMIN_HEADERS, "content-type": "application/json"},
    )
    assert response.status_code == 400, response.json()
    assert response.json()["error"]["message"] == "Synthetic evaluation rows are invalid"

    assert entered == []


def test_policy_evaluation_rejects_nonfinite_identity_before_provider_io(
    client_factory, monkeypatch
):
    client = client_factory()
    asset = _provision_reviewable_asset(client)
    entered = []

    def discover(**kwargs):
        entered.append(True)
        raise ValueError("Invalid identity reached provider IO")

    monkeypatch.setattr(schema_service, "load_source_schema", discover)
    response = client.post(
        f"/v1/assets/{asset['id']}/policy-evaluate",
        content='{"principal":"user1","claims":{"department":NaN},"rows":[{"id":7}]}',
        headers={**ADMIN_HEADERS, "content-type": "application/json"},
    )
    assert response.status_code == 400, response.json()
    assert response.json()["error"]["message"] == "Synthetic identity claims are invalid"
    assert entered == []


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
