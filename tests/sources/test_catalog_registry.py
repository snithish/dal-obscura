from __future__ import annotations

from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pytest
from dal_obscura_iceberg import catalog as iceberg_catalog
from dal_obscura_iceberg.catalog import IcebergCatalog
from dal_obscura_iceberg.format import IcebergFormatPlugin
from dal_obscura_plugin_api import ExecutionContext

from dal_obscura.adapters.providers import catalogs as registry_module
from dal_obscura.adapters.providers.adapters import PublicPluginCatalogAdapter
from dal_obscura.adapters.providers.catalogs import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from dal_obscura.plugins.bootstrap import (
    create_plugin_registry,
)


@pytest.mark.parametrize("cleanup_error", [RuntimeError, KeyboardInterrupt])
def test_catalog_registry_close_attempts_all_catalogs_when_one_fails(
    monkeypatch, cleanup_error
) -> None:
    closed: list[str] = []

    class FakeCatalog:
        def __init__(self, name: str) -> None:
            self.name = name

        def close(self) -> None:
            closed.append(self.name)
            if self.name == "first":
                raise cleanup_error("first close failed")

    monkeypatch.setattr(
        registry_module,
        "_build_catalog",
        lambda config, *, plugin_registry=None, context=None: FakeCatalog(config.name),
    )
    registry = CatalogRegistry(
        ServiceConfig(
            catalogs={
                "first": CatalogConfig(name="first", options={}),
                "second": CatalogConfig(name="second", options={}),
            }
        ),
        plugin_registry=create_plugin_registry(),
    )

    with pytest.raises(cleanup_error, match="first close failed"):
        registry.close()

    assert closed == ["first", "second"]


def test_catalog_config_requires_logical_name():
    try:
        CatalogConfig(
            name=" ",
            options={},
        )
    except ValueError as exc:
        assert str(exc) == "Catalog configuration requires a non-empty logical name"
    else:
        raise AssertionError("expected blank catalog name rejection")

    with pytest.raises(ValueError, match="revision cannot be negative"):
        CatalogConfig(name="analytics", revision=-1)


def test_dynamic_catalog_registry_rejects_legacy_direct_paths_config():
    legacy_config: dict[str, Any] = {
        "catalogs": {},
        "paths": ("/warehouse/users.parquet",),
    }
    try:
        ServiceConfig(**legacy_config)
    except TypeError as exc:
        assert "paths" in str(exc)
    else:
        raise AssertionError("expected standalone paths to be rejected")


def test_catalog_config_rejects_module_config():
    try:
        cast(Any, CatalogConfig)(
            name="legacy",
            module="tests.infrastructure.adapters.test_catalog_registry.LegacyCatalog",
            options={},
        )
    except TypeError as exc:
        assert "module" in str(exc)
    else:
        raise AssertionError("expected Python module catalog config to fail")


def test_catalog_registry_reads_iceberg_through_admitted_plugin_factory(tmp_path):
    from datetime import UTC, datetime, timedelta

    import pyarrow as pa
    from dal_obscura_plugin_conformance.iceberg_fixtures import (
        create_iceberg_table,
        iceberg_sql_catalog_options,
    )

    from dal_obscura.kernel.plans.request import PlanRequest

    create_iceberg_table(tmp_path, "routing", "warehouse", append_batches=[[0, 1]])
    registry = CatalogRegistry(
        ServiceConfig(
            catalogs={
                "analytics": CatalogConfig(
                    name="analytics",
                    options={
                        **iceberg_sql_catalog_options(tmp_path, "routing", "warehouse"),
                        "provider_catalog_name": "routing",
                    },
                )
            }
        ),
        plugin_registry=create_plugin_registry(),
    )
    try:
        source = registry.resolve("analytics", "default.users")
        schema = source.get_schema()
        projected = pa.schema([schema.field("id")], metadata=schema.metadata)
        plan = source.plan(PlanRequest(target="default.users", columns=["id"]), max_tickets=2)
        rows = []
        for task in plan.tasks:
            output_schema, batches = task.table_format.execute(
                task.partition,
                context=ExecutionContext(datetime.now(UTC) + timedelta(minutes=1), "fixture-read"),
            )
            assert output_schema.equals(projected, check_metadata=True)
            rows.extend(pa.Table.from_batches(batches, schema=output_schema).to_pylist())
        assert sorted(rows, key=lambda row: row["id"]) == [{"id": 0}, {"id": 1}]
    finally:
        registry.close()


def test_catalog_registry_close_releases_adapters_and_rejects_reuse(monkeypatch):
    closed: list[str] = []

    class ClosableCatalog:
        def resolve_table(self, target: str):
            del target
            return object()

        def list_tables(self):
            return []

        def close(self):
            closed.append("catalog")

    monkeypatch.setattr(
        registry_module,
        "_build_catalog",
        lambda *_args, **_kwargs: ClosableCatalog(),
    )
    registry = CatalogRegistry(
        ServiceConfig(catalogs={"analytics": CatalogConfig(name="analytics", options={})}),
        plugin_registry=create_plugin_registry(),
    )

    registry.close()
    registry.close()
    assert closed == ["catalog"]
    with pytest.raises(ValueError, match="Catalog registry is closed"):
        registry.list_tables("analytics")


def test_catalog_registry_construction_closes_partially_built_generation(monkeypatch):
    closed: list[str] = []
    calls = 0

    class ClosableCatalog:
        def resolve_table(self, target: str):
            del target
            return object()

        def list_tables(self):
            return []

        def close(self):
            closed.append("catalog")

    def build(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ValueError("factory failed")
        return ClosableCatalog()

    monkeypatch.setattr(registry_module, "_build_catalog", build)
    with pytest.raises(ValueError, match="factory failed"):
        CatalogRegistry(
            ServiceConfig(
                catalogs={
                    "analytics": CatalogConfig(name="analytics", options={}),
                    "replacement": CatalogConfig(name="replacement", options={}),
                }
            ),
            plugin_registry=create_plugin_registry(),
        )
    assert closed == ["catalog"]


def test_catalog_registry_construction_preserves_build_failure_when_cleanup_fails(monkeypatch):
    calls = 0

    class FailingCloseCatalog:
        def close(self):
            raise RuntimeError("cleanup failed")

    def build(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ValueError("factory failed")
        return FailingCloseCatalog()

    monkeypatch.setattr(registry_module, "_build_catalog", build)
    with pytest.raises(ValueError, match="factory failed"):
        CatalogRegistry(
            ServiceConfig(
                catalogs={
                    "analytics": CatalogConfig(name="analytics", options={}),
                    "replacement": CatalogConfig(name="replacement", options={}),
                }
            ),
            plugin_registry=create_plugin_registry(),
        )


def test_catalog_registry_rejects_provider_returned_metadata_outside_storage_roots(monkeypatch):
    class UnsafeTable:
        metadata = SimpleNamespace(current_snapshot_id=None)
        metadata_location = "s3://other-bucket/metadata.json"

        class io:
            properties: ClassVar[dict[str, str]] = {"warehouse": "s3://analytics-demo/warehouse"}

    class Catalog:
        def load_table(self, identifier: str) -> UnsafeTable:
            del identifier
            return UnsafeTable()

    monkeypatch.setattr(iceberg_catalog, "_load_iceberg_catalog", lambda *args: Catalog())
    with pytest.raises(PermissionError, match="Path is not allowed"):
        _source_catalog(
            name="analytics",
            options={},
            path_enforcer=registry_module.PathRuleEnforcer(
                [{"root": "s3://analytics-demo/warehouse"}]
            ),
        ).resolve_table("default.users")


def _source_catalog(*, name, options, path_enforcer=None):
    return PublicPluginCatalogAdapter(
        name,
        options,
        "iceberg.catalog",
        IcebergCatalog,
        lambda plugin_id: IcebergFormatPlugin,
        path_enforcer,
    )
