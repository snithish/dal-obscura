from __future__ import annotations

import httpx

from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogTargetConfig,
)
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
        targets={},
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
        targets={},
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    try:
        catalog.describe_table("default.users_view")
    except ValueError as exc:
        assert str(exc) == "Unity Catalog target 'default.users_view' is not a readable table"
    else:
        raise AssertionError("expected view rejection")


def test_unity_catalog_static_target_override_can_select_file_backend():
    catalog = UnityCatalog(
        name="uc",
        options={"base_url": "https://uc.example"},
        targets={
            "events": CatalogTargetConfig(
                backend="json",
                table="/warehouse/events.json",
            )
        },
        http_client=httpx.Client(
            transport=httpx.MockTransport(lambda request: httpx.Response(500))
        ),
    )

    descriptor = catalog.describe_table("events")

    assert descriptor.provider_id == "json"
    assert descriptor.requested_target == "events"
    assert descriptor.location == "/warehouse/events.json"
