from __future__ import annotations

from fastapi.testclient import TestClient
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, StructType

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.control_plane.application import schema_service
from dal_obscura.control_plane.interfaces.api import create_app
from tests.interfaces.control_plane.workspace_helpers import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_MODULE,
    _client,
    _provision_draft,
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


def test_production_publication_requires_current_server_review(monkeypatch) -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    client = TestClient(
        create_app(
            session_factory(engine),
            admin_token="review-secret-for-test",
            require_review=True,
            bootstrap_enabled=True,
        )
    )
    client.put(
        "/v1/catalogs/analytics",
        json={
            "module": ICEBERG_CATALOG_MODULE,
            "options": {"type": "sql", "uri": "sqlite:///catalog.db"},
        },
        headers={"authorization": "Bearer review-secret-for-test"},
    )
    client.put(
        "/v1/settings/runtime",
        json={
            "ticket_ttl_seconds": 900,
            "max_tickets": 64,
            "max_ticket_exchanges": 2,
        },
        headers={"authorization": "Bearer review-secret-for-test"},
    )
    client.put(
        "/v1/settings/auth-providers",
        json={
            "providers": [
                {
                    "ordinal": 1,
                    "module": (
                        "dal_obscura.data_plane.infrastructure.adapters."
                        "identity_oidc_jwks.OidcJwksIdentityProvider"
                    ),
                    "args": {"issuer": "https://issuer.example"},
                    "enabled": True,
                }
            ]
        },
        headers={"authorization": "Bearer review-secret-for-test"},
    )
    asset = client.put(
        "/v1/assets/analytics/default.users",
        json={"backend": "iceberg", "table_identifier": "prod.users", "options": {}},
        headers={"authorization": "Bearer review-secret-for-test"},
    ).json()
    client.put(
        f"/v1/assets/{asset['id']}/policy-rules",
        json={
            "rules": [
                {
                    "ordinal": 10,
                    "effect": "allow",
                    "principals": ["analyst"],
                    "columns": ["id"],
                    "masks": {},
                    "row_filter": None,
                }
            ]
        },
        headers={"authorization": "Bearer review-secret-for-test"},
    )
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["analyst"]},
        headers={"authorization": "Bearer review-secret-for-test"},
    )
    monkeypatch.setattr(
        schema_service,
        "load_catalog",
        lambda *args, **kwargs: _EvaluationCatalog(),
    )
    missing_draft_review = client.post(
        f"/v1/assets/{asset['id']}/policy-review",
        json={"principal": "analyst", "groups": [], "claims": {}},
        headers={"authorization": "Bearer review-secret-for-test"},
    )
    assert missing_draft_review.status_code == 400
    assert missing_draft_review.json() == {
        "detail": "Save an explicit policy draft before requesting server review."
    }
    draft = client.get(
        f"/v1/assets/{asset['id']}/draft",
        headers={"authorization": "Bearer review-secret-for-test"},
    ).json()
    saved_draft = client.put(
        f"/v1/assets/{asset['id']}/draft",
        json={"expected_revision": draft["revision"], "rules": draft["rules"]},
        headers={"authorization": "Bearer review-secret-for-test"},
    )
    assert saved_draft.status_code == 200, saved_draft.json()
    monkeypatch.setattr(
        schema_service,
        "load_catalog",
        lambda *args, **kwargs: _EvaluationCatalog(),
    )

    missing = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        headers={"authorization": "Bearer review-secret-for-test"},
    )
    reviewed = client.post(
        f"/v1/assets/{asset['id']}/policy-review",
        json={"principal": "analyst", "groups": [], "claims": {}},
        headers={"authorization": "Bearer review-secret-for-test"},
    )
    token = reviewed.json()["review_token"]
    published = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        json={"review_token": token},
        headers={"authorization": "Bearer review-secret-for-test"},
    )
    tampered = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        json={"review_token": token[:-1] + ("a" if token[-1] != "a" else "b")},
        headers={"authorization": "Bearer review-secret-for-test"},
    )

    assert missing.status_code == 400
    assert "server review" in missing.json()["detail"]
    assert reviewed.status_code == 200
    assert published.status_code == 200
    assert tampered.status_code == 400


def test_production_publication_rejects_schema_drift_after_review(monkeypatch) -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    client = TestClient(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            require_review=True,
            bootstrap_enabled=True,
        )
    )
    asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["platform:admin"]},
        headers=ADMIN_HEADERS,
    )
    draft = client.get(f"/v1/assets/{asset['id']}/draft", headers=ADMIN_HEADERS).json()
    saved_draft = client.put(
        f"/v1/assets/{asset['id']}/draft",
        json={"expected_revision": draft["revision"], "rules": draft["rules"]},
        headers=ADMIN_HEADERS,
    )
    assert saved_draft.status_code == 200, saved_draft.json()
    admitted = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={"fields": [{"name": "id", "field_id": "iceberg:1", "path": ["id"]}]},
        headers=ADMIN_HEADERS,
    )
    assert admitted.status_code == 200, admitted.json()
    monkeypatch.setattr(
        schema_service,
        "load_catalog",
        lambda *args, **kwargs: _EvaluationCatalog(),
    )
    reviewed = client.post(
        f"/v1/assets/{asset['id']}/policy-review",
        json={"principal": "user1", "groups": [], "claims": {"tenant": "default"}},
        headers=ADMIN_HEADERS,
    )
    assert reviewed.status_code == 200, reviewed.json()

    changed_admitted = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={"fields": [{"name": "id", "field_id": "iceberg:99", "path": ["id"]}]},
        headers=ADMIN_HEADERS,
    )
    assert changed_admitted.status_code == 200, changed_admitted.json()

    published = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        json={"review_token": reviewed.json()["review_token"]},
        headers=ADMIN_HEADERS,
    )

    assert published.status_code == 400
    assert published.json() == {
        "detail": "Admitted schema fields changed after review; review again."
    }

    reset_admitted = client.put(
        f"/v1/assets/{asset['id']}/schema-fields",
        json={"fields": [{"name": "id", "field_id": "iceberg:1", "path": ["id"]}]},
        headers=ADMIN_HEADERS,
    )
    assert reset_admitted.status_code == 200, reset_admitted.json()
    re_reviewed = client.post(
        f"/v1/assets/{asset['id']}/policy-review",
        json={"principal": "user1", "groups": [], "claims": {"tenant": "default"}},
        headers=ADMIN_HEADERS,
    )
    assert re_reviewed.status_code == 200, re_reviewed.json()
    monkeypatch.setattr(
        schema_service,
        "load_catalog",
        lambda *args, **kwargs: _ChangedEvaluationCatalog(),
    )
    live_schema_drift = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        json={"review_token": re_reviewed.json()["review_token"]},
        headers=ADMIN_HEADERS,
    )
    assert live_schema_drift.status_code == 400
    assert live_schema_drift.json() == {
        "detail": "Iceberg schema changed after review; review again."
    }


def test_explicit_empty_draft_is_reviewable_and_publishable_as_deny_all(monkeypatch) -> None:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    client = TestClient(
        create_app(
            session_factory(engine),
            admin_token="test-admin",
            require_review=True,
            bootstrap_enabled=True,
        )
    )
    asset = _provision_draft(client)
    client.put(
        f"/v1/assets/{asset['id']}/owners",
        json={"owners": ["platform:admin"]},
        headers=ADMIN_HEADERS,
    )
    saved = client.put(
        f"/v1/assets/{asset['id']}/draft",
        json={"expected_revision": 0, "rules": []},
        headers=ADMIN_HEADERS,
    )
    assert saved.status_code == 200, saved.json()
    monkeypatch.setattr(
        schema_service,
        "load_catalog",
        lambda *args, **kwargs: _EvaluationCatalog(),
    )

    reviewed = client.post(
        f"/v1/assets/{asset['id']}/policy-review",
        json={"principal": "analyst", "groups": [], "claims": {}},
        headers=ADMIN_HEADERS,
    )
    assert reviewed.status_code == 200, reviewed.json()
    assert reviewed.json()["decision"] == "deny"
    assert reviewed.json()["review_token"]

    published = client.post(
        f"/v1/assets/{asset['id']}/policy-versions",
        json={"review_token": reviewed.json()["review_token"]},
        headers=ADMIN_HEADERS,
    )
    assert published.status_code == 200, published.json()
