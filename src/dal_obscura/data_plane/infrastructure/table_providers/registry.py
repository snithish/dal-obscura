from __future__ import annotations

import importlib
from typing import Protocol, cast

from dal_obscura.common.catalog.ports import CatalogTableDescriptor, TableFormat
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.table_formats.delta import DeltaTableFormat
from dal_obscura.data_plane.infrastructure.table_formats.files import (
    ArrowDatasetTableFormat,
    AvroTableFormat,
    TextTableFormat,
)
from dal_obscura.data_plane.infrastructure.table_formats.iceberg import IcebergTableFormat


class TableProviderFactory(Protocol):
    provider_id: str

    def create(
        self,
        descriptor: CatalogTableDescriptor,
        *,
        path_enforcer: PathRuleEnforcer | None = None,
    ) -> TableFormat: ...


class TableProviderRegistry:
    """Maps catalog table descriptors to executable TableFormat implementations."""

    def __init__(
        self,
        *,
        extra_factories: list[TableProviderFactory] | None = None,
        factory_modules: list[str] | None = None,
    ) -> None:
        factories: list[TableProviderFactory] = [
            IcebergProviderFactory(),
            DeltaProviderFactory(),
            ArrowDatasetProviderFactory("parquet"),
            ArrowDatasetProviderFactory("csv"),
            ArrowDatasetProviderFactory("json"),
            ArrowDatasetProviderFactory("orc"),
            AvroProviderFactory(),
            TextProviderFactory(),
        ]
        factories.extend(extra_factories or [])
        factories.extend(_load_factory(path) for path in factory_modules or [])
        self._factories = {_provider_id(factory): factory for factory in factories}

    def create(
        self,
        descriptor: CatalogTableDescriptor,
        *,
        path_enforcer: PathRuleEnforcer | None = None,
    ) -> TableFormat:
        provider_id = descriptor.provider_id.strip().lower()
        factory = self._factories.get(provider_id)
        if factory is None:
            raise ValueError(
                f"Unsupported provider {provider_id!r} for target "
                f"{descriptor.requested_target!r} in catalog {descriptor.catalog_name!r}"
            )
        return factory.create(descriptor, path_enforcer=path_enforcer)


class IcebergProviderFactory:
    provider_id = "iceberg"

    def create(
        self,
        descriptor: CatalogTableDescriptor,
        *,
        path_enforcer: PathRuleEnforcer | None = None,
    ) -> TableFormat:
        metadata_location = descriptor.metadata_location
        if metadata_location is None:
            raise ValueError(
                f"Iceberg target {descriptor.requested_target!r} in catalog "
                f"{descriptor.catalog_name!r} requires metadata_location"
            )
        return IcebergTableFormat(
            catalog_name=descriptor.catalog_name,
            table_name=descriptor.requested_target,
            metadata_location=metadata_location,
            io_options=dict(descriptor.storage_options),
            path_enforcer=path_enforcer,
        )


class DeltaProviderFactory:
    provider_id = "delta"

    def create(
        self,
        descriptor: CatalogTableDescriptor,
        *,
        path_enforcer: PathRuleEnforcer | None = None,
    ) -> TableFormat:
        table_uri = descriptor.location or descriptor.table_identifier
        if table_uri is None:
            raise ValueError(
                f"Delta target {descriptor.requested_target!r} in catalog "
                f"{descriptor.catalog_name!r} requires location"
            )
        return DeltaTableFormat(
            catalog_name=descriptor.catalog_name,
            table_name=descriptor.requested_target,
            table_uri=table_uri,
            storage_options=_string_mapping(descriptor.storage_options),
            path_enforcer=path_enforcer,
        )


class ArrowDatasetProviderFactory:
    def __init__(self, provider_id: str) -> None:
        self.provider_id = provider_id

    def create(
        self,
        descriptor: CatalogTableDescriptor,
        *,
        path_enforcer: PathRuleEnforcer | None = None,
    ) -> TableFormat:
        uri = descriptor.location or descriptor.table_identifier
        if uri is None:
            raise ValueError(
                f"{self.provider_id} target {descriptor.requested_target!r} in catalog "
                f"{descriptor.catalog_name!r} requires location"
            )
        return ArrowDatasetTableFormat(
            catalog_name=descriptor.catalog_name,
            table_name=descriptor.requested_target,
            uri=uri,
            format=self.provider_id,
            options=dict(descriptor.options),
            path_enforcer=path_enforcer,
        )


class AvroProviderFactory:
    provider_id = "avro"

    def create(
        self,
        descriptor: CatalogTableDescriptor,
        *,
        path_enforcer: PathRuleEnforcer | None = None,
    ) -> TableFormat:
        uri = descriptor.location or descriptor.table_identifier
        if uri is None:
            raise ValueError(
                f"Avro target {descriptor.requested_target!r} in catalog "
                f"{descriptor.catalog_name!r} requires location"
            )
        return AvroTableFormat(
            catalog_name=descriptor.catalog_name,
            table_name=descriptor.requested_target,
            uri=uri,
            options=dict(descriptor.options),
            path_enforcer=path_enforcer,
        )


class TextProviderFactory:
    provider_id = "text"

    def create(
        self,
        descriptor: CatalogTableDescriptor,
        *,
        path_enforcer: PathRuleEnforcer | None = None,
    ) -> TableFormat:
        uri = descriptor.location or descriptor.table_identifier
        if uri is None:
            raise ValueError(
                f"Text target {descriptor.requested_target!r} in catalog "
                f"{descriptor.catalog_name!r} requires location"
            )
        return TextTableFormat(
            catalog_name=descriptor.catalog_name,
            table_name=descriptor.requested_target,
            uri=uri,
            column_name=str(descriptor.options.get("column_name") or "value"),
            path_enforcer=path_enforcer,
        )


def _provider_id(factory: TableProviderFactory) -> str:
    return factory.provider_id.strip().lower()


def _load_factory(module_path: str) -> TableProviderFactory:
    module_name, class_name = module_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    factory_cls = getattr(module, class_name)
    return cast(TableProviderFactory, factory_cls())


def _string_mapping(value: dict[str, object]) -> dict[str, str]:
    return {str(key): str(item) for key, item in value.items() if item is not None}
