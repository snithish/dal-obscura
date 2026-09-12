from __future__ import annotations

from dal_obscura.data_plane.infrastructure.adapters.builtin_plugins import (
    create_builtin_plugin_registry,
)
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import IcebergCatalog
from dal_obscura.data_plane.infrastructure.table_formats.iceberg import IcebergTableFormat


def test_builtin_registry_admits_qualified_iceberg_pair_and_factories() -> None:
    registry = create_builtin_plugin_registry()

    descriptors = registry.admitted()

    assert set(descriptors) == {
        ("catalog", "iceberg.sql"),
        ("table_format", "iceberg"),
    }
    assert registry.load("catalog", "iceberg.sql") is IcebergCatalog
    assert registry.load("table_format", "iceberg") is IcebergTableFormat

