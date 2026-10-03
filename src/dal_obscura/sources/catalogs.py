from __future__ import annotations

import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from functools import partial
from threading import RLock
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from dal_obscura.sources.plugin_runtime import PublicPluginCatalogAdapter

from dal_obscura.sources.contracts import (
    CatalogTableListing,
    Source,
)
from dal_obscura.sources.paths import PathRuleEnforcer
from dal_obscura.sources.plugins import PluginRegistry

DEFAULT_MAX_DISCOVERY_NAMESPACES = 1_000
DEFAULT_MAX_DISCOVERY_TABLES = 10_000


@dataclass(frozen=True)
class CatalogConfig:
    """One named built-in catalog configured by the control plane."""

    name: str
    options: dict[str, Any] = field(default_factory=dict)
    path_enforcer: PathRuleEnforcer | None = None
    plugin_id: str = "iceberg.sql"
    revision: int = 0

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("Catalog configuration requires a non-empty logical name")
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
        self._catalogs: dict[str, PublicPluginCatalogAdapter] = {}
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
    ) -> Source:
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
    ) -> Source:
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
) -> PublicPluginCatalogAdapter:
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
