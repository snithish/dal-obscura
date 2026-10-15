from __future__ import annotations

import pyarrow as pa
import pytest
from pydantic import ValidationError

from dal_obscura.adapters.duckdb.compiler import DuckDBProjectionCompiler
from dal_obscura.adapters.duckdb.transform import DuckDBRowTransformAdapter
from dal_obscura.adapters.providers import governance as schema_service
from dal_obscura.interfaces.http.routes.policy_schemas import PolicyEvaluationResponse
from tests.interfaces.http.schema_cases import _provision_reviewable_asset
from tests.support.control.workspace import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_ID,
)
from tests.support.sources.schema_catalog import EvaluationCatalog


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


def test_evaluation_capture_detaches_synthetic_inputs_before_work(client_factory):
    from uuid import UUID

    from dal_obscura.adapters.providers.governance import SyntheticPreviewIdentityMapper
    from dal_obscura.adapters.sql.unit_of_work import SqlGovernanceUnitOfWorkFactory
    from dal_obscura.application.governance.policies import evaluation as evaluation_service
    from dal_obscura.kernel.identity.actor import ControlPlaneActor

    client = client_factory()
    asset = _provision_reviewable_asset(client)
    rows: list[dict[str, object]] = [{"id": 7}]
    groups = ["initial-group"]
    claims: dict[str, object] = {"department": "Finance"}
    with SqlGovernanceUnitOfWorkFactory(client.app.state.test_session_factory)() as store:
        captured = evaluation_service.capture_asset_evaluation(
            store,
            UUID(str(asset["id"])),
            ControlPlaneActor.for_platform_admin("admin"),
            principal="user1",
            groups=groups,
            claims=claims,
            rows=rows,
            identity_mapper=SyntheticPreviewIdentityMapper(),
        )
        store.commit()
    rows[0]["id"] = 999
    groups.append("later-group")
    claims["department"] = "Other"
    result = evaluation_service.evaluate_asset_policy(
        captured,
        pa.schema([pa.field("id", pa.int64())]),
        compiler=DuckDBProjectionCompiler(),
        transform=DuckDBRowTransformAdapter(),
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
