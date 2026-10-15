from __future__ import annotations

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, StructType

from dal_obscura.adapters.providers import governance as schema_service
from tests.interfaces.http.schema_cases import _provision_reviewable_asset
from tests.support.control.workspace import (
    ADMIN_HEADERS,
    ICEBERG_CATALOG_ID,
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


def test_asset_schema_route_preserves_bounded_metadata_beyond_native_execution_depth(
    client_factory, monkeypatch
):
    from dal_obscura_plugin_conformance.schemas import nested_struct_schema

    client = client_factory()
    asset = _provision_reviewable_asset(client)
    monkeypatch.setattr(
        schema_service, "load_source_schema", lambda **kwargs: nested_struct_schema(63)
    )
    response = client.get(f"/v1/assets/{asset['id']}/schema", headers=ADMIN_HEADERS)
    assert response.status_code == 200
    assert response.json()["fields"][0]["name"] == "root"
    assert response.json()["fields"][0]["children"][0]["name"] == "level_62"
