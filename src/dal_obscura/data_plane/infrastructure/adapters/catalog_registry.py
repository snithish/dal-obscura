from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from dal_obscura.common.catalog.ports import (
    CatalogPlugin,
    CatalogTableDescriptor,
    CatalogTableListing,
    TableFormat,
)
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.table_formats.iceberg import IcebergTableFormat

CatalogType = Literal["iceberg"]


@dataclass(frozen=True)
class CatalogConfig:
    """One named built-in catalog configured by the control plane."""

    name: str
    type: CatalogType
    options: dict[str, Any] = field(default_factory=dict)
    path_enforcer: PathRuleEnforcer | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("Catalog configuration requires a non-empty logical name")


@dataclass(frozen=True)
class ServiceConfig:
    """Published catalog configuration installed in a data-plane registry."""

    catalogs: dict[str, CatalogConfig]


class CatalogRegistry:
    """Resolves configured catalog targets into executable table formats."""

    def __init__(self, config: ServiceConfig) -> None:
        self._config = config
        self._catalogs = {
            name: _build_catalog(catalog_config) for name, catalog_config in config.catalogs.items()
        }

    @property
    def current_config(self) -> ServiceConfig:
        return self._config

    def reload(self, config: ServiceConfig) -> None:
        self._config = config
        self._catalogs = {
            name: _build_catalog(catalog_config) for name, catalog_config in config.catalogs.items()
        }

    def resolve(
        self,
        catalog: str | None,
        target: str,
        *,
        tenant_id: str = "default",
    ) -> TableFormat:
        del tenant_id
        if catalog is None:
            raise ValueError("Catalog name is required to resolve a target")
        implementation = self._catalogs.get(catalog)
        if implementation is None:
            raise ValueError(f"Unknown catalog: {catalog}")
        return implementation.resolve_table(target)

    def describe(
        self,
        catalog: str | None,
        target: str,
        *,
        tenant_id: str = "default",
    ) -> TableFormat:
        return self.resolve(catalog, target, tenant_id=tenant_id)

    def describe_catalog(self, catalog_name: str, target: str) -> TableFormat:
        return self.resolve(catalog_name, target)

    def list_tables(self, catalog_name: str) -> list[CatalogTableListing]:
        implementation = self._catalogs.get(catalog_name)
        if implementation is None:
            raise ValueError(f"Unknown catalog: {catalog_name}")
        return implementation.list_tables()


DynamicCatalogRegistry = CatalogRegistry


class IcebergCatalog(CatalogPlugin):
    """Catalog resolver for SQL-style Iceberg catalogs."""

    def __init__(
        self,
        name: str,
        options: dict[str, Any],
        path_enforcer: PathRuleEnforcer | None = None,
    ):
        self._name = name
        self.options = dict(options)
        self._path_enforcer = path_enforcer
        self._catalog: Any | None = None

    @property
    def name(self) -> str:
        return self._name

    def resolve_table(self, target: str) -> TableFormat:
        descriptor = self.describe_table(target)
        metadata_location = descriptor.metadata_location
        if metadata_location is None:
            raise ValueError(
                f"Iceberg target {target!r} in catalog {self.name!r} requires metadata_location"
            )
        return IcebergTableFormat(
            catalog_name=self.name,
            table_name=target,
            metadata_location=metadata_location,
            io_options=dict(descriptor.storage_options),
            path_enforcer=self._path_enforcer,
        )

    def describe_table(self, target: str) -> CatalogTableDescriptor:
        """Compatibility helper for catalog-discovery tests."""
        if self._catalog is None:
            self._catalog = _load_iceberg_catalog(
                _provider_catalog_name(self.name, self.options),
                _catalog_options(self.options),
            )
        return _resolve_iceberg_descriptor(
            self._catalog,
            self.name,
            target,
            target,
        )

    def list_tables(self) -> list[CatalogTableListing]:
        if self._catalog is None:
            self._catalog = _load_iceberg_catalog(
                _provider_catalog_name(self.name, self.options),
                _catalog_options(self.options),
            )
        table_names = sorted(
            {
                _identifier_to_name(identifier)
                for namespace in _walk_namespaces(self._catalog)
                for identifier in _list_tables(self._catalog, namespace)
            }
        )
        return [
            CatalogTableListing(
                name=table_name,
                provider_id="iceberg",
                table_identifier=table_name,
            )
            for table_name in table_names
        ]


def _build_catalog(config: CatalogConfig) -> CatalogPlugin:
    if config.type == "iceberg":
        return IcebergCatalog(config.name, config.options, config.path_enforcer)
    raise ValueError(f"Unsupported catalog type: {config.type}")


def _resolve_iceberg_descriptor(
    catalog: Any,
    catalog_name: str,
    requested_target: str,
    table_identifier: str,
) -> CatalogTableDescriptor:
    """Contacts the Iceberg catalog to resolve the actual metadata location for a table."""
    try:
        pyiceberg_table = catalog.load_table(table_identifier)
    except Exception as e:
        raise ValueError(
            f"Failed to load table {table_identifier!r} from catalog {catalog_name!r}: {e}"
        ) from e

    return CatalogTableDescriptor(
        catalog_name=catalog_name,
        requested_target=requested_target,
        provider_id="iceberg",
        table_identifier=table_identifier,
        metadata_location=pyiceberg_table.metadata_location,
        storage_options=dict(pyiceberg_table.io.properties),
    )


def _load_iceberg_catalog(catalog_name: str, catalog_options: dict[str, Any]) -> Any:
    """Loads and caches the underlying PyIceberg catalog implementation."""
    from pyiceberg.catalog import load_catalog

    try:
        return load_catalog(catalog_name, **catalog_options)
    except Exception as exc:
        raise ValueError(f"Failed to load catalog {catalog_name!r}: {exc}") from exc


def _provider_catalog_name(logical_name: str, options: dict[str, Any]) -> str:
    return str(options.get("provider_catalog_name") or options.get("catalog_name") or logical_name)


def _catalog_options(options: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(options)
    cleaned.pop("provider_catalog_name", None)
    cleaned.pop("catalog_name", None)
    return cleaned


def _walk_namespaces(catalog: Any) -> list[tuple[str, ...]]:
    namespaces: list[tuple[str, ...]] = []
    pending: list[tuple[str, ...]] = [()]
    seen: set[tuple[str, ...]] = set()
    while pending:
        namespace = pending.pop(0)
        if namespace in seen:
            continue
        seen.add(namespace)
        namespaces.append(namespace)
        for child in _list_namespaces(catalog, namespace):
            pending.append(_namespace_tuple(child))
    return namespaces


def _list_namespaces(catalog: Any, namespace: tuple[str, ...]) -> list[object]:
    try:
        if namespace:
            return list(catalog.list_namespaces(namespace))
        return list(catalog.list_namespaces())
    except TypeError:
        return list(catalog.list_namespaces(namespace))


def _list_tables(catalog: Any, namespace: tuple[str, ...]) -> list[object]:
    try:
        return list(catalog.list_tables(namespace))
    except Exception:
        if namespace:
            raise
        return []


def _namespace_tuple(namespace: object) -> tuple[str, ...]:
    if isinstance(namespace, str):
        return tuple(part for part in namespace.split(".") if part)
    if isinstance(namespace, tuple):
        return tuple(str(part) for part in namespace)
    if isinstance(namespace, list):
        return tuple(str(part) for part in namespace)
    return (str(namespace),)


def _identifier_to_name(identifier: object) -> str:
    if isinstance(identifier, str):
        return identifier
    if isinstance(identifier, tuple):
        return ".".join(str(part) for part in identifier)
    if isinstance(identifier, list):
        return ".".join(str(part) for part in identifier)
    return str(identifier)
