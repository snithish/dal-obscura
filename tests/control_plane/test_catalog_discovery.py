from __future__ import annotations

import httpx

from dal_obscura.control_plane.infrastructure.catalog_discovery import (
    UNITY_CATALOG_MODULE,
    discover_catalog_tables,
    discover_iceberg_tables,
)


class FakeIcebergCatalog:
    def list_namespaces(self, namespace=()):
        if namespace == ():
            return [("default",), ("prod",)]
        return []

    def list_tables(self, namespace):
        if namespace == ("default",):
            return [("default", "users")]
        if namespace == ("prod",):
            return [("prod", "orders")]
        return []


def test_iceberg_discovery_lists_tables_across_namespaces():
    tables = discover_iceberg_tables(
        "analytics",
        {"type": "sql", "uri": "sqlite:///catalog.db"},
        load_catalog_fn=lambda name, **options: FakeIcebergCatalog(),
    )

    assert tables == [
        {"backend": "iceberg", "name": "default.users", "table_identifier": "default.users"},
        {"backend": "iceberg", "name": "prod.orders", "table_identifier": "prod.orders"},
    ]


def test_catalog_discovery_lists_unity_catalog_tables(monkeypatch):
    real_client = httpx.Client

    def client_factory(**kwargs):
        del kwargs

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/2.1/unity-catalog/tables"
            return httpx.Response(
                200,
                json={
                    "tables": [
                        {
                            "full_name": "main.default.users",
                            "data_source_format": "DELTA",
                        },
                        {
                            "full_name": "main.default.events",
                            "data_source_format": "JSON",
                        },
                    ]
                },
            )

        return real_client(transport=httpx.MockTransport(handler))

    monkeypatch.setattr(
        "dal_obscura.data_plane.infrastructure.adapters.unity_catalog.httpx.Client",
        client_factory,
    )

    tables = discover_catalog_tables(
        "uc",
        UNITY_CATALOG_MODULE,
        {"base_url": "https://uc.example", "uc_catalog": "main", "schemas": ["default"]},
    )

    assert tables == [
        {"backend": "json", "name": "default.events", "table_identifier": "default.events"},
        {"backend": "delta", "name": "default.users", "table_identifier": "default.users"},
    ]
