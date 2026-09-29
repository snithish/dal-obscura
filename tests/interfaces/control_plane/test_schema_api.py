from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, StructType

from dal_obscura.control_plane.application import schema_service
from dal_obscura.control_plane.interfaces.routes.schemas import PolicyEvaluationResponse
from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_ID,
    _client,
)


@pytest.mark.parametrize(
    ("payload", "field"),
    [
        ({"principal": "analyst", "columns": ["email"]}, "columns"),
        ({"principal": "   "}, "principal"),
        ({"principal": "analyst", "groups": [" "]}, "groups.0"),
    ],
)
def test_policy_evaluation_rejects_ignored_or_blank_inputs(payload, field):
    client = _client()
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


class _EvaluationTable:
    def schema(self) -> Schema:
        return Schema(
            NestedField(field_id=1, name="id", field_type=LongType()),
            NestedField(field_id=2, name="email", field_type=StringType()),
            NestedField(field_id=3, name="region", field_type=StringType()),
        )


class _EvaluationCatalog:
    def load_table(self, identifier: str) -> _EvaluationTable:
        assert identifier == "prod.users"
        return _EvaluationTable()


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


def test_asset_schema_route_reads_authoritative_iceberg_schema(monkeypatch) -> None:
    client = _client()
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
    monkeypatch.setattr(schema_service, "load_catalog", lambda *args, **kwargs: _FakeCatalog())

    response = client.get(f"/v1/assets/{asset['id']}/schema", headers=ADMIN_HEADERS)

    assert response.status_code == 200
    assert response.json()["fields"][0]["children"][0]["human_path"] == "profile.email"
    assert response.json()["fields"][1]["path"]["segments"][0]["field_id"] == 3


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


def test_policy_evaluation_returns_duckdb_transformed_synthetic_rows(monkeypatch) -> None:
    client = _client()
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
        "load_catalog",
        lambda *args, **kwargs: _EvaluationCatalog(),
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
