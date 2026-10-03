from __future__ import annotations

from typing import Any, ClassVar, cast

import pytest
from dal_obscura_plugin_api import TableIdentifier

from dal_obscura.sources import catalogs as registry_module
from dal_obscura.sources import sql_catalog
from dal_obscura.sources.builtins import (
    create_builtin_plugin_registry,
)
from dal_obscura.sources.catalogs import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from dal_obscura.sources.iceberg_plugin import IcebergFormatPlugin
from dal_obscura.sources.plugin_runtime import PublicPluginCatalogAdapter
from dal_obscura.sources.sql_catalog import SqlCatalog


def test_catalog_registry_close_attempts_all_catalogs_when_one_fails(monkeypatch) -> None:
    closed: list[str] = []

    class FakeCatalog:
        def __init__(self, name: str) -> None:
            self.name = name

        def close(self) -> None:
            closed.append(self.name)
            if self.name == "first":
                raise RuntimeError("first close failed")

    monkeypatch.setattr(
        registry_module,
        "_build_catalog",
        lambda config, *, plugin_registry=None: FakeCatalog(config.name),
    )
    registry = CatalogRegistry(
        ServiceConfig(
            catalogs={
                "first": CatalogConfig(name="first", options={}),
                "second": CatalogConfig(name="second", options={}),
            }
        )
    )

    with pytest.raises(RuntimeError, match="first close failed"):
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


def test_iceberg_catalog_uses_provider_catalog_name_from_options(monkeypatch):
    loaded: dict[str, object] = {}

    class FakePyIcebergCatalog:
        def load_table(self, table_identifier: str) -> object:
            loaded["table_identifier"] = table_identifier
            return FakePyIcebergTable()

    def fake_load_catalog(catalog_name: str, options: dict[str, Any]) -> object:
        loaded["catalog_name"] = catalog_name
        loaded["options"] = options
        return FakePyIcebergCatalog()

    monkeypatch.setattr(sql_catalog, "_load_iceberg_catalog", fake_load_catalog)

    catalog = _source_catalog(
        name="analytics",
        options={"provider_catalog_name": "prod_glue", "type": "glue"},
    )

    descriptor = catalog.resolve_table("default.users").handle

    assert loaded == {
        "catalog_name": "prod_glue",
        "options": {"type": "glue"},
        "table_identifier": ("default", "users"),
    }
    assert descriptor.catalog_instance_id == "analytics"
    assert descriptor.identifier == TableIdentifier(namespace=("default",), name="users")


def test_iceberg_catalog_rejects_unsupported_catalog_option() -> None:
    with pytest.raises(ValueError, match="Unsupported catalog option: catalog_name"):
        _source_catalog(name="analytics", options={"catalog_name": "old-provider-name"})


def test_iceberg_registry_rejects_root_only_table_listing(monkeypatch):
    class RootOnlyCatalog:
        def list_namespaces(self, namespace):
            del namespace
            return [()]

        def list_tables(self):
            return [("users",)]

    monkeypatch.setattr(
        sql_catalog,
        "_load_iceberg_catalog",
        lambda catalog_name, options: RootOnlyCatalog(),
    )
    catalog = _source_catalog(name="analytics", options={})

    with pytest.raises(TypeError):
        catalog.list_tables()


def test_iceberg_registry_does_not_hide_root_table_provider_errors(monkeypatch):
    class FailingCatalog:
        def list_namespaces(self, namespace):
            del namespace
            return [()]

        def list_tables(self, namespace):
            del namespace
            raise RuntimeError("catalog unavailable")

    monkeypatch.setattr(
        sql_catalog,
        "_load_iceberg_catalog",
        lambda catalog_name, options: FailingCatalog(),
    )
    catalog = _source_catalog(name="analytics", options={})

    with pytest.raises(RuntimeError, match="catalog unavailable"):
        catalog.list_tables()


@pytest.mark.parametrize("bad_namespace", [("prod", 7), "prod..staging", ""])
def test_iceberg_registry_rejects_malformed_provider_namespaces(monkeypatch, bad_namespace):
    class MalformedCatalog:
        def list_namespaces(self, namespace):
            del namespace
            return [bad_namespace]

        def list_tables(self, namespace):
            del namespace
            return []

    monkeypatch.setattr(
        sql_catalog,
        "_load_iceberg_catalog",
        lambda catalog_name, options: MalformedCatalog(),
    )
    catalog = _source_catalog(name="analytics", options={})

    with pytest.raises(ValueError, match="invalid namespace"):
        catalog.list_tables()


@pytest.mark.parametrize("bad_identifier", [("prod", 7), "prod..orders", "orders\n"])
def test_iceberg_registry_rejects_malformed_provider_table_identifiers(monkeypatch, bad_identifier):
    class MalformedCatalog:
        def list_namespaces(self, namespace):
            del namespace
            return [()]

        def list_tables(self, namespace):
            del namespace
            return [bad_identifier]

    monkeypatch.setattr(
        sql_catalog,
        "_load_iceberg_catalog",
        lambda catalog_name, options: MalformedCatalog(),
    )
    catalog = _source_catalog(name="analytics", options={})

    with pytest.raises(ValueError, match="invalid table identifier"):
        catalog.list_tables()


def test_iceberg_registry_bounds_unbounded_namespace_providers(monkeypatch):
    class EndlessCatalog:
        def list_namespaces(self, namespace):
            del namespace
            return (("namespace", str(index)) for index in range(100_000))

        def list_tables(self, namespace):
            del namespace
            return []

    monkeypatch.setattr(
        sql_catalog,
        "_load_iceberg_catalog",
        lambda catalog_name, options: EndlessCatalog(),
    )
    catalog = _source_catalog(name="analytics", options={})

    with pytest.raises(ValueError, match=r"namespace limit"):
        catalog.list_tables()


def test_iceberg_registry_bounds_unbounded_table_providers(monkeypatch):
    class EndlessCatalog:
        def list_namespaces(self, namespace):
            del namespace
            return [()]

        def list_tables(self, namespace):
            del namespace
            return (("users", str(index)) for index in range(100_000))

    monkeypatch.setattr(
        sql_catalog,
        "_load_iceberg_catalog",
        lambda catalog_name, options: EndlessCatalog(),
    )
    catalog = _source_catalog(name="analytics", options={})

    with pytest.raises(ValueError, match="table limit"):
        catalog.list_tables()


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


class FakePyIcebergTable:
    metadata_location = "s3://warehouse/default/users/metadata.json"

    class io:
        properties: ClassVar[dict[str, str]] = {"warehouse": "s3://warehouse"}


def test_catalog_registry_constructs_iceberg_through_admitted_plugin_factory(tmp_path):
    config = ServiceConfig(
        catalogs={
            "analytics": CatalogConfig(
                name="analytics",
                options={"uri": f"sqlite:///{tmp_path}/warehouse.db"},
            )
        }
    )

    registry = CatalogRegistry(config, plugin_registry=create_builtin_plugin_registry())

    assert isinstance(cast(Any, registry._catalogs["analytics"])._catalog, SqlCatalog)


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
        ServiceConfig(catalogs={"analytics": CatalogConfig(name="analytics", options={})})
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
            )
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
            )
        )


def test_catalog_registry_rejects_provider_returned_metadata_outside_storage_roots(monkeypatch):
    class UnsafeTable:
        metadata_location = "s3://other-bucket/metadata.json"

        class io:
            properties: ClassVar[dict[str, str]] = {"warehouse": "s3://analytics-demo/warehouse"}

    class Catalog:
        def load_table(self, identifier: str) -> UnsafeTable:
            del identifier
            return UnsafeTable()

    monkeypatch.setattr(sql_catalog, "_load_iceberg_catalog", lambda *args: Catalog())
    with pytest.raises(PermissionError, match="Path is not allowed"):
        _source_catalog(
            name="analytics",
            options={},
            path_enforcer=registry_module.PathRuleEnforcer(
                [{"root": "s3://analytics-demo/warehouse"}]
            ),
        ).resolve_table("default.users")


def test_catalog_handle_never_copies_provider_credentials_or_redirect_properties(monkeypatch):
    class Table:
        metadata_location = "s3://analytics-demo/warehouse/metadata.json"

        class io:
            properties: ClassVar[dict[str, str]] = {
                "warehouse": "/outside/warehouse",
                "s3.secret-access-key": "private-value",
            }

    class Catalog:
        def load_table(self, identifier):
            return Table()

    monkeypatch.setattr(sql_catalog, "_load_iceberg_catalog", lambda *args: Catalog())
    source = _source_catalog(name="analytics", options={}).resolve_table("default.users")
    assert dict(source.handle.metadata) == {"metadata_location": Table.metadata_location}


def _source_catalog(*, name, options, path_enforcer=None):
    return PublicPluginCatalogAdapter(
        name,
        options,
        "iceberg.sql",
        SqlCatalog,
        lambda plugin_id: IcebergFormatPlugin,
        path_enforcer,
    )
