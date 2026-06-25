from __future__ import annotations

from collections.abc import Iterable
from typing import Any, ClassVar, cast

import pyarrow as pa

from dal_obscura.common.catalog.ports import (
    CatalogTableDescriptor,
    CatalogTableListing,
    TableFormat,
)
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.data_plane.infrastructure.adapters import catalog_registry as registry_module
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogRegistry,
    IcebergCatalog,
    ServiceConfig,
)


def test_catalog_registry_resolves_executable_reader_without_provider_registry(tmp_path):
    config = ServiceConfig(
        catalogs={
            "local": CatalogConfig(
                name="local",
                type="files",
                options={"format": "parquet", "location": str(tmp_path / "users.parquet")},
            )
        }
    )
    registry = CatalogRegistry(config)

    table = registry.resolve("local", "users")

    assert table.catalog_name == "local"
    assert table.table_name == "users"
    assert table.format == "parquet"


def test_catalog_registry_uses_named_builtin_catalogs():
    registry = CatalogRegistry(
        ServiceConfig(
            catalogs={
                "analytics": CatalogConfig(
                    name="analytics",
                    type="files",
                    options={"format": "parquet", "location": "/tmp/users.parquet"},
                ),
                "warehouse": CatalogConfig(
                    name="warehouse",
                    type="files",
                    options={"format": "csv", "location": "/tmp/events.csv"},
                ),
            }
        )
    )

    analytics_table = registry.describe("analytics", "default.users")
    warehouse_table = registry.describe("warehouse", "default.events")

    assert analytics_table.format == "parquet"
    assert analytics_table.catalog_name == "analytics"
    assert analytics_table.table_name == "default.users"
    assert warehouse_table.format == "csv"
    assert warehouse_table.catalog_name == "warehouse"
    assert warehouse_table.table_name == "default.events"


def test_catalog_registry_lists_tables_from_named_catalog():
    registry = CatalogRegistry(
        ServiceConfig(
            catalogs={
                "analytics": CatalogConfig(
                    name="analytics",
                    type="files",
                    options={
                        "format": "parquet",
                        "location": "/tmp/users.parquet",
                        "tables": ["default.users", "default.events"],
                    },
                )
            }
        ),
    )

    listings = registry.list_tables("analytics")

    assert [table.name for table in listings] == ["default.users", "default.events"]
    assert {table.provider_id for table in listings} == {"parquet"}


def test_catalog_config_requires_logical_name():
    try:
        CatalogConfig(
            name=" ",
            type="files",
            options={},
        )
    except ValueError as exc:
        assert str(exc) == "Catalog configuration requires a non-empty logical name"
    else:
        raise AssertionError("expected blank catalog name rejection")


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
