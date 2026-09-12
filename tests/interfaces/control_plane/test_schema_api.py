from __future__ import annotations

from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, StructType

from dal_obscura.control_plane.application import schema_service
from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_MODULE,
    _client,
)


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


def test_asset_schema_route_reads_authoritative_iceberg_schema(monkeypatch) -> None:
    client = _client()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
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


def test_policy_evaluation_returns_duckdb_transformed_synthetic_rows(monkeypatch) -> None:
    client = _client()
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
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
        f"/v1/assets/{asset['id']}/policy-rules",
        json={
            "rules": [
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["analyst"],
                    "when": {},
                    "columns": ["id", "email", "region"],
                    "masks": {"email": {"type": "email"}},
                    "row_filter": "region = 'us'",
                }
            ]
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
    assert payload["input_rows"] == 2
    assert payload["output_rows"] == 1
    assert payload["rows"][0]["email"] == "a***@example.com"
    assert payload["evidence"]["evaluator_version"] == "duckdb-synthetic-v1"
