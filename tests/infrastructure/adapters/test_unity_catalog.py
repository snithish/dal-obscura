from __future__ import annotations

import httpx

from dal_obscura.data_plane.infrastructure.adapters.unity_catalog import (
    UnityCatalog,
    UnityCatalogClient,
)


def test_unity_catalog_client_normalizes_databricks_base_url_and_get_table():
    seen_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append(request)
        return httpx.Response(
            200,
            json={
                "full_name": "main.default.users",
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
                "storage_location": "s3://warehouse/users",
            },
        )

    client = UnityCatalogClient(
        base_url="https://example.cloud.databricks.com",
        token="secret",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    table = client.get_table("main.default.users")

    assert table["storage_location"] == "s3://warehouse/users"
    assert seen_requests[0].url.path == "/api/2.1/unity-catalog/tables/main.default.users"
    assert seen_requests[0].headers["authorization"] == "Bearer secret"


def test_unity_catalog_resolves_delta_table():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "full_name": "main.default.users",
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
                "storage_location": "/warehouse/users",
            },
        )

    catalog = UnityCatalog(
        name="uc",
        options={
            "base_url": "https://uc.example",
            "token": "secret",
            "uc_catalog": "main",
            "storage_options": {"AWS_REGION": "us-east-1"},
        },
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    descriptor = catalog.describe_table("default.users")

    assert descriptor.provider_id == "delta"
    assert descriptor.catalog_name == "uc"
    assert descriptor.requested_target == "default.users"
    assert descriptor.location == "/warehouse/users"


def test_unity_catalog_rejects_views_before_returning_descriptor():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "full_name": "main.default.users_view",
                "table_type": "VIEW",
                "data_source_format": "DELTA",
                "storage_location": "/warehouse/users",
            },
        )

    catalog = UnityCatalog(
        name="uc",
        options={"base_url": "https://uc.example", "uc_catalog": "main"},
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    try:
        catalog.describe_table("default.users_view")
    except ValueError as exc:
        assert str(exc) == "Unity Catalog target 'default.users_view' is not a readable table"
    else:
        raise AssertionError("expected view rejection")


def test_unity_catalog_lists_tables_from_configured_schemas():
    seen_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append(request)
        return httpx.Response(
            200,
            json={
                "tables": [
                    {
                        "full_name": "main.default.users",
                        "data_source_format": "DELTA",
                        "storage_location": "/warehouse/users",
                    },
                    {
                        "full_name": "main.default.events",
                        "data_source_format": "PARQUET",
                        "storage_location": "/warehouse/events",
                    },
                ]
            },
        )

    catalog = UnityCatalog(
        name="uc",
        options={"base_url": "https://uc.example", "uc_catalog": "main", "schemas": ["default"]},
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    tables = catalog.list_tables()

    assert seen_requests[0].url.path == "/api/2.1/unity-catalog/tables"
    assert seen_requests[0].url.params["catalog_name"] == "main"
    assert seen_requests[0].url.params["schema_name"] == "default"
    assert [(table.name, table.provider_id, table.table_identifier) for table in tables] == [
        ("default.events", "parquet", "/warehouse/events"),
        ("default.users", "delta", "/warehouse/users"),
    ]
