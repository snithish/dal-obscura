from __future__ import annotations

import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from functools import partial
from threading import RLock
from typing import Any, cast

from dal_obscura.sources.contracts import (
    CatalogPlugin,
    CatalogTableListing,
    TableFormat,
)
from dal_obscura.sources.paths import PathRuleEnforcer
from dal_obscura.sources.plugins import PluginRegistry

# ``type`` remains a descriptive runtime discriminator for built-ins. Public
# plugins are selected by ``plugin_id`` and use the generic ``plugin`` value,
# so adding a plugin does not require changing this core type alias.
CatalogType = str
DEFAULT_MAX_DISCOVERY_NAMESPACES = 1_000
DEFAULT_MAX_DISCOVERY_TABLES = 10_000


@dataclass(frozen=True)
class CatalogConfig:
    """One named built-in catalog configured by the control plane."""

    name: str
    type: CatalogType
    options: dict[str, Any] = field(default_factory=dict)
    path_enforcer: PathRuleEnforcer | None = None
    plugin_id: str = "iceberg.sql"
    revision: int = 0

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("Catalog configuration requires a non-empty logical name")
        if self.type not in {"iceberg", "plugin"}:
            raise ValueError(f"Unsupported catalog type: {self.type}")
        if self.revision < 0:
            raise ValueError("Catalog configuration revision cannot be negative")


@dataclass(frozen=True)
class ServiceConfig:
    """Live catalog configuration installed in a data-plane registry."""

    catalogs: dict[str, CatalogConfig]


class CatalogRegistry:
    """Resolves configured catalog targets into executable table formats."""

    def __init__(
        self,
        config: ServiceConfig,
        *,
        plugin_registry: PluginRegistry | None = None,
    ) -> None:
        self._plugin_registry = plugin_registry
        self._closed = False
        self._catalogs: dict[str, CatalogPlugin] = {}
        try:
            for name, catalog_config in config.catalogs.items():
                self._catalogs[name] = _build_catalog(
                    catalog_config, plugin_registry=plugin_registry
                )
        except BaseException:
            _close_catalogs(self._catalogs.values())
            raise
        self._swap_lock = RLock()

    def resolve(
        self,
        catalog: str | None,
        target: str,
    ) -> TableFormat:
        self._ensure_open()
        if catalog is None:
            raise ValueError("Catalog name is required to resolve a target")
        with self._swap_lock:
            implementation = self._catalogs.get(catalog)
            if implementation is None:
                raise ValueError(f"Unknown catalog: {catalog}")
            return implementation.resolve_table(target)

    def describe(
        self,
        catalog: str | None,
        target: str,
    ) -> TableFormat:
        return self.resolve(catalog, target)

    def list_tables(self, catalog_name: str) -> list[CatalogTableListing]:
        self._ensure_open()
        with self._swap_lock:
            implementation = self._catalogs.get(catalog_name)
            if implementation is None:
                raise ValueError(f"Unknown catalog: {catalog_name}")
            return implementation.list_tables()

    def close(self) -> None:
        with self._swap_lock:
            if self._closed:
                return
            self._closed = True
            catalogs = tuple(self._catalogs.values())
            self._catalogs = {}
        _close_catalogs(catalogs)

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("Catalog registry is closed")


def _close_catalogs(catalogs: Iterable[object]) -> None:
    active_error = sys.exc_info()[1]
    first_error: Exception | None = None
    for catalog in catalogs:
        close = getattr(catalog, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:
                if first_error is None:
                    first_error = exc
    if first_error is not None and active_error is None:
        raise first_error


def _build_catalog(
    config: CatalogConfig,
    *,
    plugin_registry: PluginRegistry | None = None,
) -> CatalogPlugin:
    if plugin_registry is None:
        from dal_obscura.sources.builtins import (
            create_builtin_plugin_registry,
        )

        plugin_registry = create_builtin_plugin_registry()
    factory = plugin_registry.load("catalog", config.plugin_id)
    if not callable(factory):
        raise ValueError(f"Plugin factory is invalid: {config.plugin_id}")
    from dal_obscura.sources.plugin_runtime import PublicPluginCatalogAdapter

    return PublicPluginCatalogAdapter(
        config.name,
        config.options,
        config.plugin_id,
        cast(Any, factory),
        lambda plugin_id: _load_format_factory(plugin_registry, plugin_id, config.path_enforcer),
        config.path_enforcer,
        config.revision,
    )


def _load_format_factory(
    registry: PluginRegistry, plugin_id: str, path_enforcer: PathRuleEnforcer | None
) -> object:
    from dal_obscura.sources.iceberg_plugin import (
        IcebergFormatPlugin,
    )

    factory = registry.load("table_format", plugin_id)
    # Bind native IO enforcement as a construction dependency, not handle data.
    return (
        partial(factory, path_enforcer=path_enforcer) if factory is IcebergFormatPlugin else factory
    )


def _check_returned_locations(
    metadata_location: str,
    storage_options: dict[str, Any],
    path_enforcer: PathRuleEnforcer | None,
) -> None:
    if path_enforcer is None or not path_enforcer.enabled:
        return
    path_enforcer.check(metadata_location)
    for value in _nested_strings(storage_options):
        # PyIceberg accepts both URI and local filesystem IO properties.  A
        # local absolute path must receive the same allowlist check as an S3
        # or file URI; otherwise a provider could redirect reads through an
        # unapproved local warehouse while the metadata location is valid.
        if "://" in value or value.startswith(("/", "file:")):
            path_enforcer.check(value)


def _nested_strings(value: object):
    if isinstance(value, Mapping):
        for item in value.values():
            yield from _nested_strings(item)
    elif isinstance(value, list | tuple):
        for item in value:
            yield from _nested_strings(item)
    elif isinstance(value, str):
        yield value


def _load_iceberg_catalog(catalog_name: str, catalog_options: dict[str, Any]) -> Any:
    """Loads and caches the underlying PyIceberg catalog implementation."""
    from pyiceberg.catalog import load_catalog

    try:
        return load_catalog(catalog_name, **catalog_options)
    except Exception as exc:
        raise ValueError(f"Failed to load catalog {catalog_name!r}: {exc}") from exc


def _provider_catalog_name(logical_name: str, options: dict[str, Any]) -> str:
    if "catalog_name" in options:
        raise ValueError("Catalog option 'catalog_name' is retired; use 'provider_catalog_name'")
    provider_name = options.get("provider_catalog_name")
    return str(provider_name) if provider_name else logical_name


def _catalog_options(options: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(options)
    cleaned.pop("provider_catalog_name", None)
    return cleaned
