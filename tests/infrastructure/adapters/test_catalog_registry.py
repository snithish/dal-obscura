from __future__ import annotations

from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogTargetConfig,
    StaticCatalog,
)


def test_static_catalog_resolves_delta_target_to_delta_table_format():
    catalog = StaticCatalog(
        name="analytics",
        options={},
        targets={
            "users": CatalogTargetConfig(
                backend="delta",
                table="/warehouse/users",
                options={"storage_options": {"AWS_REGION": "us-east-1"}},
            )
        },
    )

    table = catalog.get_table("users")

    assert table.format == "delta"
    assert table.catalog_name == "analytics"
    assert table.table_name == "users"


def test_static_catalog_rejects_unknown_backend_with_target_context():
    catalog = StaticCatalog(
        name="analytics",
        options={},
        targets={"users": CatalogTargetConfig(backend="unknown", table="/warehouse/users")},
    )

    try:
        catalog.get_table("users")
    except ValueError as exc:
        assert str(exc) == (
            "Unsupported backend 'unknown' for target 'users' in catalog 'analytics'"
        )
    else:
        raise AssertionError("expected unsupported backend error")


def test_catalog_config_backend_routes_parquet_without_importing_iceberg():
    catalog = StaticCatalog(
        name="files",
        options={},
        targets={
            "events": CatalogTargetConfig(
                backend="parquet",
                table="/warehouse/events",
            )
        },
    )

    table = catalog.get_table("events")

    assert table.format == "parquet"
    assert table.catalog_name == "files"
    assert table.table_name == "events"


def test_catalog_config_keeps_target_options_isolated_from_catalog_options():
    config = CatalogConfig(
        name="files",
        module="dal_obscura.data_plane.infrastructure.adapters.catalog_registry.StaticCatalog",
        options={"storage_options": {"catalog": "value"}},
        targets={
            "events": CatalogTargetConfig(
                backend="json",
                table="/warehouse/events",
                options={"storage_options": {"target": "value"}},
            )
        },
    )
    catalog = StaticCatalog(
        name=config.name,
        options=dict(config.options),
        targets=dict(config.targets),
    )

    table = catalog.get_table("events")

    assert table.format == "json"
