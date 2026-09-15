from __future__ import annotations

from collections.abc import Iterable
from typing import Any, ClassVar, cast

import pyarrow as pa
import pytest

from dal_obscura.common.catalog.ports import (
    CatalogTableDescriptor,
    CatalogTableListing,
    TableFormat,
)
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.data_plane.infrastructure.adapters import catalog_registry as registry_module
from dal_obscura.data_plane.infrastructure.adapters.builtin_plugins import (
    create_builtin_plugin_registry,
)
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogRegistry,
    IcebergCatalog,
    ServiceConfig,
    _resolve_iceberg_descriptor,
)


def test_catalog_registry_rejects_removed_file_catalog_type(tmp_path):
    config = ServiceConfig(
        catalogs={
            "local": CatalogConfig(
                name="local",
                type=cast(Any, "files"),
                options={"format": "parquet", "location": str(tmp_path / "users.parquet")},
            )
        }
    )
    try:
        CatalogRegistry(config)
    except ValueError as exc:
        assert str(exc) == "Unsupported catalog type: files"
    else:
        raise AssertionError("expected removed catalog type rejection")


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
                "first": CatalogConfig(name="first", type="iceberg", options={}),
                "second": CatalogConfig(name="second", type="iceberg", options={}),
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
            type="iceberg",
            options={},
        )
    except ValueError as exc:
        assert str(exc) == "Catalog configuration requires a non-empty logical name"
    else:
        raise AssertionError("expected blank catalog name rejection")

    with pytest.raises(ValueError, match="revision cannot be negative"):
        CatalogConfig(name="analytics", type="iceberg", revision=-1)


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

    monkeypatch.setattr(registry_module, "_load_iceberg_catalog", fake_load_catalog)

    catalog = IcebergCatalog(
        name="analytics",
        options={"provider_catalog_name": "prod_glue", "type": "glue"},
    )

    descriptor = catalog.describe_table("default.users")

    assert loaded == {
        "catalog_name": "prod_glue",
        "options": {"type": "glue"},
        "table_identifier": "default.users",
    }
    assert descriptor.catalog_name == "analytics"
    assert descriptor.table_identifier == "default.users"


def test_iceberg_registry_supports_root_only_table_listing(monkeypatch):
    class RootOnlyCatalog:
        def list_namespaces(self):
            return [()]

        def list_tables(self):
            return [("users",)]

    monkeypatch.setattr(
        registry_module,
        "_load_iceberg_catalog",
        lambda catalog_name, options: RootOnlyCatalog(),
    )
    catalog = IcebergCatalog(name="analytics", options={})

    assert [item.table_identifier for item in catalog.list_tables()] == ["users"]


def test_iceberg_registry_does_not_hide_root_table_provider_errors(monkeypatch):
    class FailingCatalog:
        def list_namespaces(self):
            return [()]

        def list_tables(self, namespace):
            del namespace
            raise RuntimeError("catalog unavailable")

    monkeypatch.setattr(
        registry_module,
        "_load_iceberg_catalog",
        lambda catalog_name, options: FailingCatalog(),
    )
    catalog = IcebergCatalog(name="analytics", options={})

    with pytest.raises(RuntimeError, match="catalog unavailable"):
        catalog.list_tables()


@pytest.mark.parametrize("bad_namespace", [("prod", 7), "prod..staging", ""])
def test_iceberg_registry_rejects_malformed_provider_namespaces(monkeypatch, bad_namespace):
    class MalformedCatalog:
        def list_namespaces(self):
            return [bad_namespace]

        def list_tables(self):
            return []

    monkeypatch.setattr(
        registry_module,
        "_load_iceberg_catalog",
        lambda catalog_name, options: MalformedCatalog(),
    )
    catalog = IcebergCatalog(name="analytics", options={})

    with pytest.raises(ValueError, match="invalid namespace"):
        catalog.list_tables()


@pytest.mark.parametrize("bad_identifier", [("prod", 7), "prod..orders", "orders\n"])
def test_iceberg_registry_rejects_malformed_provider_table_identifiers(monkeypatch, bad_identifier):
    class MalformedCatalog:
        def list_namespaces(self):
            return [()]

        def list_tables(self):
            return [bad_identifier]

    monkeypatch.setattr(
        registry_module,
        "_load_iceberg_catalog",
        lambda catalog_name, options: MalformedCatalog(),
    )
    catalog = IcebergCatalog(name="analytics", options={})

    with pytest.raises(ValueError, match="invalid table identifier"):
        catalog.list_tables()


def test_iceberg_registry_bounds_unbounded_namespace_providers(monkeypatch):
    class EndlessCatalog:
        def list_namespaces(self):
            return (("namespace", str(index)) for index in range(100_000))

        def list_tables(self):
            return []

    monkeypatch.setattr(
        registry_module,
        "_load_iceberg_catalog",
        lambda catalog_name, options: EndlessCatalog(),
    )
    catalog = IcebergCatalog(name="analytics", options={})

    with pytest.raises(ValueError, match="namespace limit"):
        catalog.list_tables()


def test_iceberg_registry_bounds_unbounded_table_providers(monkeypatch):
    class EndlessCatalog:
        def list_namespaces(self):
            return [()]

        def list_tables(self):
            return (("users", str(index)) for index in range(100_000))

    monkeypatch.setattr(
        registry_module,
        "_load_iceberg_catalog",
        lambda catalog_name, options: EndlessCatalog(),
    )
    catalog = IcebergCatalog(name="analytics", options={})

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


class FakeCatalog:
    def __init__(self, name: str, options: dict[str, Any], path_enforcer=None):
        del path_enforcer
        self._name = name
        self._options = dict(options)

    @property
    def name(self) -> str:
        return self._name

    def describe_table(self, target: str) -> CatalogTableDescriptor:
        return CatalogTableDescriptor(
            catalog_name=self.name,
            requested_target=target,
            provider_id=str(self._options.get("provider_id", "postgres")),
            table_identifier=target,
            options={key: value for key, value in self._options.items() if key != "tables"},
        )

    def list_tables(self) -> list[CatalogTableListing]:
        return [
            CatalogTableListing(
                name=str(name),
                provider_id=str(self._options.get("provider_id", "postgres")),
                table_identifier=str(name),
            )
            for name in self._options.get("tables", [])
        ]


class FakePostgresTableFormat(TableFormat):
    def get_schema(self) -> pa.Schema:
        return pa.schema([pa.field("id", pa.int64())])

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        del max_tickets
        return Plan(
            schema=self.get_schema(),
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=self.get_schema(),
                    partition=InputPartition(),
                )
            ],
            full_row_filter=request.row_filter,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        del partition
        return self.get_schema(), iter(())


class LegacyCatalog:
    def __init__(self, name, options, path_enforcer=None):
        del options, path_enforcer
        self._name = name

    @property
    def name(self):
        return self._name

    def get_table(self, target):
        return FakePostgresTableFormat(
            catalog_name=self.name,
            table_name=target,
            format="legacy",
        )


class FakePyIcebergTable:
    metadata_location = "s3://warehouse/default/users/metadata.json"

    class io:
        properties: ClassVar[dict[str, str]] = {"warehouse": "s3://warehouse"}


def test_catalog_registry_constructs_iceberg_through_admitted_plugin_factory():
    config = ServiceConfig(
        catalogs={
            "analytics": CatalogConfig(
                name="analytics",
                type="iceberg",
                options={"uri": "sqlite:///warehouse.db"},
            )
        }
    )

    registry = CatalogRegistry(config, plugin_registry=create_builtin_plugin_registry())

    assert isinstance(registry._catalogs["analytics"], IcebergCatalog)


def test_catalog_registry_reload_failure_keeps_previous_generation(monkeypatch):
    initial = ServiceConfig(
        catalogs={
            "analytics": CatalogConfig(
                name="analytics",
                type="iceberg",
                options={"uri": "sqlite:///warehouse.db"},
            )
        }
    )
    registry = CatalogRegistry(initial)
    replacement = ServiceConfig(
        catalogs={
            "replacement": CatalogConfig(
                name="replacement",
                type="iceberg",
                options={"uri": "sqlite:///replacement.db"},
            )
        }
    )
    monkeypatch.setattr(
        registry_module,
        "_build_catalog",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("factory failed")),
    )

    with pytest.raises(ValueError, match="factory failed"):
        registry.reload(replacement)

    assert registry.current_config == initial
    assert set(registry._catalogs) == {"analytics"}


def test_catalog_registry_close_releases_adapters_and_rejects_reuse(monkeypatch):
    closed: list[str] = []

    class ClosableCatalog:
        def resolve_table(self, target: str):
            del target
            return FakePostgresTableFormat(catalog_name="analytics", table_name="users", format="x")

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
        ServiceConfig(
            catalogs={"analytics": CatalogConfig(name="analytics", type="iceberg", options={})}
        )
    )

    registry.close()
    registry.close()
    assert closed == ["catalog"]
    with pytest.raises(ValueError, match="Catalog registry is closed"):
        registry.list_tables("analytics")


def test_catalog_registry_reload_closes_partially_built_generation(monkeypatch):
    closed: list[str] = []
    calls = 0

    class ClosableCatalog:
        def resolve_table(self, target: str):
            del target
            return FakePostgresTableFormat(catalog_name="analytics", table_name="users", format="x")

        def list_tables(self):
            return []

        def close(self):
            closed.append("catalog")

    def build(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise ValueError("factory failed")
        return ClosableCatalog()

    monkeypatch.setattr(registry_module, "_build_catalog", build)
    registry = CatalogRegistry(
        ServiceConfig(
            catalogs={"analytics": CatalogConfig(name="analytics", type="iceberg", options={})}
        )
    )
    with pytest.raises(ValueError, match="factory failed"):
        registry.reload(
            ServiceConfig(
                catalogs={
                    "analytics": CatalogConfig(name="analytics", type="iceberg", options={}),
                    "replacement": CatalogConfig(name="replacement", type="iceberg", options={}),
                }
            )
        )
    assert closed == ["catalog"]


def test_catalog_registry_reload_preserves_build_failure_when_cleanup_fails(monkeypatch):
    calls = 0

    class FailingCloseCatalog:
        def close(self):
            raise RuntimeError("cleanup failed")

    def build(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise ValueError("factory failed")
        return FailingCloseCatalog()

    monkeypatch.setattr(registry_module, "_build_catalog", build)
    registry = CatalogRegistry(
        ServiceConfig(
            catalogs={"analytics": CatalogConfig(name="analytics", type="iceberg", options={})}
        )
    )

    with pytest.raises(ValueError, match="factory failed"):
        registry.reload(
            ServiceConfig(
                catalogs={
                    "analytics": CatalogConfig(name="analytics", type="iceberg", options={}),
                    "replacement": CatalogConfig(name="replacement", type="iceberg", options={}),
                }
            )
        )


def test_catalog_registry_reload_closes_retired_generation(monkeypatch):
    closed: list[str] = []

    class ClosableCatalog:
        def resolve_table(self, target: str):
            del target
            return FakePostgresTableFormat(catalog_name="analytics", table_name="users", format="x")

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
        ServiceConfig(
            catalogs={"analytics": CatalogConfig(name="analytics", type="iceberg", options={})}
        )
    )
    registry.reload(
        ServiceConfig(
            catalogs={"replacement": CatalogConfig(name="replacement", type="iceberg", options={})}
        )
    )
    assert closed == ["catalog"]


def test_catalog_registry_rejects_provider_returned_metadata_outside_storage_roots():
    class UnsafeTable:
        metadata_location = "s3://other-bucket/metadata.json"

        class io:
            properties: ClassVar[dict[str, str]] = {"warehouse": "s3://analytics-demo/warehouse"}

    class Catalog:
        def load_table(self, identifier: str) -> UnsafeTable:
            del identifier
            return UnsafeTable()

    with pytest.raises(PermissionError, match="Path is not allowed"):
        _resolve_iceberg_descriptor(
            Catalog(),
            "analytics",
            "default.users",
            "default.users",
            path_enforcer=registry_module.PathRuleEnforcer(
                [{"root": "s3://analytics-demo/warehouse"}]
            ),
        )


def test_catalog_registry_rejects_provider_returned_local_storage_path_outside_roots():
    class SafeMetadataTable:
        metadata_location = "s3://analytics-demo/warehouse/metadata.json"

        class io:
            properties: ClassVar[dict[str, str]] = {"warehouse": "/outside/warehouse"}

    class Catalog:
        def load_table(self, identifier: str) -> SafeMetadataTable:
            del identifier
            return SafeMetadataTable()

    with pytest.raises(PermissionError, match="Path is not allowed"):
        _resolve_iceberg_descriptor(
            Catalog(),
            "analytics",
            "default.users",
            "default.users",
            path_enforcer=registry_module.PathRuleEnforcer(
                [{"root": "s3://analytics-demo/warehouse"}]
            ),
        )
