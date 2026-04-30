from __future__ import annotations

import importlib
import threading
from dataclasses import dataclass, field
from typing import Any

from dal_obscura.common.catalog.ports import CatalogPlugin, TableFormat
from dal_obscura.data_plane.infrastructure.table_formats.iceberg import IcebergTableFormat


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


@dataclass(frozen=True)
class ServiceConfig:
    """Published catalog configuration installed in a data-plane registry."""

    catalogs: dict[str, CatalogConfig]
    paths: tuple[object, ...] = ()


class DynamicCatalogRegistry:
    """Stores catalog implementations and resolves dataset requests into tables."""

    def __init__(self, config: ServiceConfig) -> None:
        self._lock = threading.RLock()
        self._catalog_implementations: dict[str, CatalogPlugin] = {}
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
        return implementation.get_table(target)

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

    def __init__(self, name: str, options: dict[str, Any], targets: dict[str, CatalogTargetConfig]):
        self._name = name
        self.options = options
        self.targets = targets
        self._catalog = _load_iceberg_catalog(name, options)

    @property
    def name(self) -> str:
        return self._name

    def get_table(
        self,
        target: str,
    ) -> TableFormat:
        """Resolves an Iceberg target, optionally applying exact target overrides."""
        target_config = self.targets.get(target)
        if target_config is not None:
            _ensure_iceberg_backend(self.name, target, target_config)
        table_identifier = (target_config.table if target_config else None) or target
        return _resolve_iceberg_metadata(self._catalog, self.name, target, table_identifier)


class StaticCatalog:
    """Catalog resolver for catalogs that require explicit target mappings."""

    def __init__(self, name: str, options: dict[str, Any], targets: dict[str, CatalogTargetConfig]):
        self._name = name
        self.options = options
        self.targets = targets
        self._catalog: Any | None = None

    @property
    def name(self) -> str:
        return self._name

    def get_table(
        self,
        target: str,
    ) -> TableFormat:
        """Resolves an explicitly configured target by loading it from the catalog."""
        target_config = self.targets.get(target)
        if target_config is None:
            raise ValueError(f"Unknown target {target!r} for catalog {self.name!r}")
        _ensure_iceberg_backend(self.name, target, target_config)
        if self._catalog is None:
            self._catalog = _load_iceberg_catalog(self.name, self.options)
        table_identifier = target_config.table or target
        return _resolve_iceberg_metadata(self._catalog, self.name, target, table_identifier)


def _resolve_iceberg_metadata(
    catalog: Any,
    catalog_name: str,
    requested_target: str,
    table_identifier: str,
) -> TableFormat:
    """Contacts the Iceberg catalog to resolve the actual metadata location for a table."""
    try:
        pyiceberg_table = catalog.load_table(table_identifier)
    except Exception as e:
        raise ValueError(
            f"Failed to load table {table_identifier!r} from catalog {catalog_name!r}: {e}"
        ) from e

    return IcebergTableFormat(
        catalog_name=catalog_name,
        table_name=requested_target,
        metadata_location=pyiceberg_table.metadata_location,
        io_options=dict(pyiceberg_table.io.properties),
    )


def _load_iceberg_catalog(catalog_name: str, catalog_options: dict[str, Any]) -> Any:
    """Loads and caches the underlying PyIceberg catalog implementation."""
    from pyiceberg.catalog import load_catalog

    try:
        return load_catalog(catalog_name, **catalog_options)
    except Exception as exc:
        raise ValueError(f"Failed to load catalog {catalog_name!r}: {exc}") from exc


def _ensure_iceberg_backend(
    catalog_name: str,
    requested_target: str,
    target_config: CatalogTargetConfig,
) -> None:
    """Rejects non-Iceberg target overrides until other backends are implemented."""
    backend_id = target_config.backend or "iceberg"
    if backend_id != "iceberg":
        raise ValueError(
            f"Unsupported backend {backend_id!r} for target {requested_target!r} in "
            f"catalog {catalog_name!r}; only 'iceberg' is currently supported"
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
        )
    except Exception as exc:
        raise ValueError(f"Failed to instantiate catalog {catalog_config.name!r}: {exc}") from exc
