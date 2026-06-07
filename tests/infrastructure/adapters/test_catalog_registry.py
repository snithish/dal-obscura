from __future__ import annotations

from collections.abc import Iterable

import pyarrow as pa

from dal_obscura.common.catalog.ports import TableFormat
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.data_plane.infrastructure.adapters.catalog_registry import (
    CatalogConfig,
    CatalogTargetConfig,
    DynamicCatalogRegistry,
    ServiceConfig,
    StaticCatalog,
)
from dal_obscura.data_plane.infrastructure.table_providers.registry import (
    TableProviderFactory,
    TableProviderRegistry,
)


def test_static_catalog_resolves_delta_target_to_descriptor():
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

    descriptor = catalog.describe_table("users")

    assert descriptor.provider_id == "delta"
    assert descriptor.catalog_name == "analytics"
    assert descriptor.requested_target == "users"
    assert descriptor.location == "/warehouse/users"


def test_static_catalog_rejects_unknown_backend_with_target_context():
    catalog = StaticCatalog(
        name="analytics",
        options={},
        targets={"users": CatalogTargetConfig(backend="unknown", table="/warehouse/users")},
    )

    try:
        TableProviderRegistry().create(catalog.describe_table("users"))
    except ValueError as exc:
        assert str(exc) == (
            "Unsupported provider 'unknown' for target 'users' in catalog 'analytics'"
        )
    else:
        raise AssertionError("expected unsupported backend error")


def test_catalog_config_backend_describes_parquet_without_importing_iceberg():
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

    descriptor = catalog.describe_table("events")

    assert descriptor.provider_id == "parquet"
    assert descriptor.catalog_name == "files"
    assert descriptor.requested_target == "events"


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

    descriptor = catalog.describe_table("events")

    assert descriptor.provider_id == "json"
    assert descriptor.storage_options == {"target": "value"}


def test_dynamic_catalog_registry_uses_descriptor_and_provider_registry():
    registry = DynamicCatalogRegistry(
        ServiceConfig(
            catalogs={
                "static": CatalogConfig(
                    name="static",
                    module=(
                        "dal_obscura.data_plane.infrastructure.adapters.catalog_registry."
                        "StaticCatalog"
                    ),
                    options={},
                    targets={
                        "users": CatalogTargetConfig(
                            backend="postgres",
                            table="public.users",
                            options={"dsn": "postgresql://example/db"},
                        )
                    },
                )
            }
        ),
        table_provider_registry=TableProviderRegistry(extra_factories=[FakePostgresFactory()]),
    )

    table = registry.describe("static", "users")

    assert table.format == "postgres"


def test_dynamic_catalog_registry_rejects_plugins_without_describe_table():
    registry = DynamicCatalogRegistry(
        ServiceConfig(
            catalogs={
                "legacy": CatalogConfig(
                    name="legacy",
                    module=("tests.infrastructure.adapters.test_catalog_registry.LegacyCatalog"),
                    options={},
                    targets={},
                )
            }
        )
    )

    try:
        registry.describe("legacy", "users")
    except AttributeError as exc:
        assert "describe_table" in str(exc)
    else:
        raise AssertionError("expected catalog without describe_table to fail")


class FakePostgresFactory(TableProviderFactory):
    provider_id = "postgres"

    def create(self, descriptor, *, path_enforcer=None):
        assert descriptor.location is None
        assert path_enforcer is None
        return FakePostgresTableFormat(
            catalog_name=descriptor.catalog_name,
            table_name=descriptor.requested_target,
            format="postgres",
        )


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
    def __init__(self, name, options, targets, path_enforcer=None):
        del options, targets, path_enforcer
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
