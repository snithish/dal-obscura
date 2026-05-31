from __future__ import annotations

from dal_obscura.control_plane.infrastructure.catalog_discovery import discover_iceberg_tables


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
