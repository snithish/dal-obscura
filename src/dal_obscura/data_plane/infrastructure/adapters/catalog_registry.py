from __future__ import annotations

import importlib
import threading
from dataclasses import dataclass, field
from typing import Any, cast

from dal_obscura.common.catalog.ports import CatalogPlugin, CatalogTableDescriptor, TableFormat
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.table_providers.registry import TableProviderRegistry


@dataclass(frozen=True)
class CatalogTargetConfig:
    """Per-target override inside a catalog definition."""

    backend: str | None = None
    table: str | None = None
    format: str | None = None
    paths: tuple[str, ...] = ()
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CatalogConfig:
    """One named catalog plus any target-specific overrides."""

    name: str
    module: str
    options: dict[str, Any]
    targets: dict[str, CatalogTargetConfig]
    path_enforcer: PathRuleEnforcer | None = None


@dataclass(frozen=True)
class ServiceConfig:
    """Published catalog configuration installed in a data-plane registry."""

    catalogs: dict[str, CatalogConfig]
    paths: tuple[object, ...] = ()


class DynamicCatalogRegistry:
    """Stores catalog implementations and resolves dataset requests into tables."""

    def __init__(
        self,
        config: ServiceConfig,
        *,
        table_provider_registry: TableProviderRegistry | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._catalog_implementations: dict[str, CatalogPlugin] = {}
        self._table_provider_registry = table_provider_registry or TableProviderRegistry()
        self._current_config = config
        self.reload(config)

    @property
    def current_config(self) -> ServiceConfig:
        """Latest service config currently installed in the registry."""
        with self._lock:
            return self._current_config

    def reload(self, config: ServiceConfig) -> None:
        """Publishes the latest catalog configuration and instantiates catalogs."""
        new_catalogs: dict[str, CatalogPlugin] = {}
        for catalog_config in config.catalogs.values():
            new_catalogs[catalog_config.name] = _load_and_instantiate_catalog(catalog_config)

        with self._lock:
            self._catalog_implementations = new_catalogs
            self._current_config = config

    def describe_catalog(self, catalog_name: str, target: str) -> TableFormat:
        """Resolves a target within a named catalog."""
        with self._lock:
            implementation = self._catalog_implementations.get(catalog_name)
            if implementation is None:
                raise ValueError(f"Unknown catalog: {catalog_name}")
        descriptor = implementation.describe_table(target)
        path_enforcer = _path_enforcer(implementation)
        return self._table_provider_registry.create(descriptor, path_enforcer=path_enforcer)

    def describe(
        self,
        catalog: str | None,
        target: str,
        *,
        tenant_id: str = "default",
    ) -> TableFormat:
        """Describes a target by asking the requested catalog implementation."""
        del tenant_id
        if catalog is None:
            raise ValueError("Catalog name is required to resolve a target")
        return self.describe_catalog(catalog, target)


class IcebergCatalog:
    """Catalog resolver for SQL-style Iceberg catalogs."""

    def __init__(
        self,
        name: str,
        options: dict[str, Any],
        targets: dict[str, CatalogTargetConfig],
        path_enforcer: PathRuleEnforcer | None = None,
    ):
        self._name = name
        self.options = options
        self.targets = targets
        self._path_enforcer = path_enforcer
        self._catalog: Any | None = None

    @property
    def name(self) -> str:
        return self._name

    def describe_table(self, target: str) -> CatalogTableDescriptor:
        """Resolves an Iceberg target to provider-neutral metadata."""
        target_config = self.targets.get(target)
        if target_config is not None:
            backend = _backend_id(target_config)
            if backend != "iceberg":
                return _descriptor_from_target_config(self.name, target, target_config)
        if self._catalog is None:
            self._catalog = _load_iceberg_catalog(self.name, self.options)
        table_identifier = (target_config.table if target_config else None) or target
        return _resolve_iceberg_descriptor(
            self._catalog,
            self.name,
            target,
            table_identifier,
        )


class StaticCatalog:
    """Catalog resolver for catalogs that require explicit target mappings."""

    def __init__(
        self,
        name: str,
        options: dict[str, Any],
        targets: dict[str, CatalogTargetConfig],
        path_enforcer: PathRuleEnforcer | None = None,
    ):
        self._name = name
        self.options = options
        self.targets = targets
        self._path_enforcer = path_enforcer
        self._catalog: Any | None = None

    @property
    def name(self) -> str:
        return self._name

    def describe_table(self, target: str) -> CatalogTableDescriptor:
        """Resolves an explicitly configured target to provider-neutral metadata."""
        target_config = self.targets.get(target)
        if target_config is None:
            raise ValueError(f"Unknown target {target!r} for catalog {self.name!r}")
        backend = _backend_id(target_config)
        if backend != "iceberg":
            return _descriptor_from_target_config(self.name, target, target_config)
        if self._catalog is None:
            self._catalog = _load_iceberg_catalog(self.name, self.options)
        table_identifier = target_config.table or target
        return _resolve_iceberg_descriptor(
            self._catalog,
            self.name,
            target,
            table_identifier,
        )


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


def _backend_id(target_config: CatalogTargetConfig) -> str:
    return (target_config.backend or target_config.format or "iceberg").strip().lower()


def _descriptor_from_target_config(
    catalog_name: str,
    requested_target: str,
    target_config: CatalogTargetConfig,
) -> CatalogTableDescriptor:
    backend_id = _backend_id(target_config)
    table_uri = target_config.table or requested_target
    options = dict(target_config.options)
    storage_options: dict[str, object] = {
        str(key): str(value)
        for key, value in _mapping(options.get("storage_options")).items()
        if value is not None
    }
    options.pop("storage_options", None)
    return CatalogTableDescriptor(
        catalog_name=catalog_name,
        requested_target=requested_target,
        provider_id=backend_id,
        table_identifier=table_uri,
        location=table_uri if backend_id in _PATH_PROVIDERS else None,
        options=options,
        storage_options=storage_options,
    )


def _load_and_instantiate_catalog(catalog_config: CatalogConfig) -> CatalogPlugin:
    """Dynamically loads and instantiates a catalog from the `module` configuration."""
    module_path = catalog_config.module
    try:
        if "." not in module_path:
            raise ValueError(f"Module path {module_path!r} must be a fully qualified class name")
        module_name, class_name = module_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise ValueError(f"Failed to load module {module_path!r}: {exc}") from exc

    catalog_cls = getattr(module, class_name, None)
    if catalog_cls is None:
        raise ValueError(f"Module {module_name!r} does not define a class named {class_name!r}")

    try:
        return catalog_cls(
            name=catalog_config.name,
            options=dict(catalog_config.options),
            targets=dict(catalog_config.targets),
            path_enforcer=catalog_config.path_enforcer,
        )
    except Exception as exc:
        raise ValueError(f"Failed to instantiate catalog {catalog_config.name!r}: {exc}") from exc


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value).copy()
    return {}


_PATH_PROVIDERS = frozenset({"delta", "parquet", "csv", "json", "orc", "avro", "text"})


def _path_enforcer(implementation: CatalogPlugin) -> PathRuleEnforcer | None:
    value = getattr(implementation, "_path_enforcer", None)
    if isinstance(value, PathRuleEnforcer):
        return value
    return None
