from __future__ import annotations

import sys
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from threading import RLock
from typing import Any, cast

from dal_obscura.common.catalog.ports import (
    CatalogPlugin,
    CatalogTableDescriptor,
    CatalogTableListing,
    TableFormat,
)
from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.table_formats.iceberg import IcebergTableFormat

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
        self._config = config
        self._plugin_registry = plugin_registry
        self._closed = False
        self._catalogs = {
            name: _build_catalog(catalog_config, plugin_registry=plugin_registry)
            for name, catalog_config in config.catalogs.items()
        }
        self._swap_lock = RLock()

    @property
    def current_config(self) -> ServiceConfig:
        with self._swap_lock:
            return self._config

    def reload(self, config: ServiceConfig) -> None:
        self._ensure_open()
        # Build the complete candidate generation before exposing either its
        # metadata or executable adapters. A factory failure therefore leaves
        # both views on the previous generation.
        candidate_catalogs: dict[str, CatalogPlugin] = {}
        try:
            for name, catalog_config in config.catalogs.items():
                candidate_catalogs[name] = _build_catalog(
                    catalog_config,
                    plugin_registry=self._plugin_registry,
                )
        except Exception:
            _close_catalogs(candidate_catalogs.values())
            raise
        with self._swap_lock:
            old_catalogs = tuple(self._catalogs.values())
            self._config = config
            self._catalogs = candidate_catalogs
        _close_catalogs(old_catalogs)

    def resolve(
        self,
        catalog: str | None,
        target: str,
        *,
        tenant_id: str = "default",
    ) -> TableFormat:
        self._ensure_open()
        del tenant_id
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
        *,
        tenant_id: str = "default",
    ) -> TableFormat:
        return self.resolve(catalog, target, tenant_id=tenant_id)

    def describe_catalog(self, catalog_name: str, target: str) -> TableFormat:
        return self.resolve(catalog_name, target)

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
            path_enforcer=self._path_enforcer,
        )

    def list_tables(self) -> list[CatalogTableListing]:
        if self._catalog is None:
            self._catalog = _load_iceberg_catalog(
                _provider_catalog_name(self.name, self.options),
                _catalog_options(self.options),
            )
        table_names: set[str] = set()
        for namespace in _walk_namespaces(
            self._catalog,
            max_namespaces=DEFAULT_MAX_DISCOVERY_NAMESPACES,
        ):
            remaining = DEFAULT_MAX_DISCOVERY_TABLES - len(table_names)
            for identifier in _bounded_provider_items(
                _list_tables(self._catalog, namespace),
                limit=remaining,
                kind="table",
            ):
                table_names.add(_identifier_to_name(identifier))
        return [
            CatalogTableListing(
                name=table_name,
                provider_id="iceberg",
                table_identifier=table_name,
            )
            for table_name in table_names
        ]

    def close(self) -> None:
        catalog = self._catalog
        self._catalog = None
        close = getattr(catalog, "close", None)
        if callable(close):
            close()


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
    if plugin_registry is not None:
        factory = plugin_registry.load("catalog", config.plugin_id)
        if not callable(factory):
            raise ValueError(f"Plugin factory is invalid: {config.plugin_id}")
        if config.plugin_id != "iceberg.sql":
            # The public SDK is an optional runtime dependency. Keep the
            # built-in Iceberg service importable in a minimal installation;
            # only an explicitly selected external plugin needs this bridge.
            from dal_obscura.data_plane.infrastructure.adapters.public_plugin_adapter import (
                PublicPluginCatalogAdapter,
            )

            return PublicPluginCatalogAdapter(
                config.name,
                config.options,
                config.plugin_id,
                cast(Any, factory),
                lambda plugin_id: plugin_registry.load("table_format", plugin_id),
                config.path_enforcer,
                config.revision,
            )
        constructor = cast(
            Callable[[str, dict[str, Any], PathRuleEnforcer | None], CatalogPlugin],
            factory,
        )
        implementation = constructor(config.name, config.options, config.path_enforcer)
        if not hasattr(implementation, "resolve_table") or not hasattr(
            implementation, "list_tables"
        ):
            raise ValueError(f"Plugin factory returned an invalid catalog: {config.plugin_id}")
        return implementation
    if config.type == "iceberg":
        return IcebergCatalog(config.name, config.options, config.path_enforcer)
    raise ValueError(f"Unsupported catalog type: {config.type}")


def _resolve_iceberg_descriptor(
    catalog: Any,
    catalog_name: str,
    requested_target: str,
    table_identifier: str,
    *,
    path_enforcer: PathRuleEnforcer | None = None,
) -> CatalogTableDescriptor:
    """Contacts the Iceberg catalog to resolve the actual metadata location for a table."""
    try:
        pyiceberg_table = catalog.load_table(table_identifier)
    except Exception as e:
        raise ValueError(
            f"Failed to load table {table_identifier!r} from catalog {catalog_name!r}: {e}"
        ) from e

    metadata_location = pyiceberg_table.metadata_location
    if not isinstance(metadata_location, str) or not metadata_location.strip():
        raise ValueError("Iceberg catalog returned no metadata location")
    _check_returned_locations(
        metadata_location,
        dict(getattr(pyiceberg_table.io, "properties", {})),
        path_enforcer,
    )
    return CatalogTableDescriptor(
        catalog_name=catalog_name,
        requested_target=requested_target,
        provider_id="iceberg",
        table_identifier=table_identifier,
        metadata_location=metadata_location,
        storage_options=dict(pyiceberg_table.io.properties),
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
    if isinstance(value, dict):
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
    return str(options.get("provider_catalog_name") or options.get("catalog_name") or logical_name)


def _catalog_options(options: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(options)
    cleaned.pop("provider_catalog_name", None)
    cleaned.pop("catalog_name", None)
    return cleaned


def _walk_namespaces(
    catalog: Any,
    *,
    max_namespaces: int,
) -> Iterator[tuple[str, ...]]:
    if max_namespaces <= 0:
        raise ValueError("Catalog discovery namespace limit must be positive")
    pending: list[tuple[str, ...]] = [()]
    seen: set[tuple[str, ...]] = set()
    while pending:
        namespace = pending.pop(0)
        if namespace in seen:
            continue
        if len(seen) >= max_namespaces:
            raise ValueError("Catalog discovery exceeded the namespace limit")
        seen.add(namespace)
        yield namespace
        for child in _bounded_provider_items(
            _list_namespaces(catalog, namespace),
            limit=max_namespaces - len(seen),
            kind="namespace",
        ):
            pending.append(_namespace_tuple(child))


def _list_namespaces(catalog: Any, namespace: tuple[str, ...]) -> Iterable[object]:
    return catalog.list_namespaces(namespace)


def _list_tables(catalog: Any, namespace: tuple[str, ...]) -> Iterable[object]:
    try:
        return catalog.list_tables(namespace)
    except ValueError as exc:
        if not namespace and str(exc) == "Empty namespace identifier":
            return ()
        raise


def _bounded_provider_items(
    values: Iterable[object],
    *,
    limit: int,
    kind: str,
) -> Iterator[object]:
    if limit < 0:
        raise ValueError(f"Catalog discovery exceeded the {kind} limit")
    for index, value in enumerate(values):
        if index >= limit:
            raise ValueError(f"Catalog discovery exceeded the {kind} limit")
        yield value


def _namespace_tuple(namespace: object) -> tuple[str, ...]:
    if isinstance(namespace, str):
        parts = tuple(namespace.split("."))
        if any(not _valid_identifier_segment(part) for part in parts):
            raise ValueError("Catalog provider returned an invalid namespace")
        return parts
    if isinstance(namespace, tuple):
        parts = tuple(namespace)
        if any(not _valid_identifier_segment(part) for part in parts):
            raise ValueError("Catalog provider returned an invalid namespace")
        return cast(tuple[str, ...], parts)
    if isinstance(namespace, list):
        parts = tuple(namespace)
        if any(not _valid_identifier_segment(part) for part in parts):
            raise ValueError("Catalog provider returned an invalid namespace")
        return cast(tuple[str, ...], parts)
    raise ValueError("Catalog provider returned an invalid namespace")


def _identifier_to_name(identifier: object) -> str:
    if isinstance(identifier, str):
        parts = tuple(identifier.split("."))
        if any(not _valid_identifier_segment(part) for part in parts):
            raise ValueError("Catalog provider returned an invalid table identifier")
        return identifier
    if isinstance(identifier, tuple):
        parts = tuple(identifier)
        if any(not _valid_identifier_segment(part) for part in parts):
            raise ValueError("Catalog provider returned an invalid table identifier")
        return ".".join(cast(tuple[str, ...], parts))
    if isinstance(identifier, list):
        parts = tuple(identifier)
        if any(not _valid_identifier_segment(part) for part in parts):
            raise ValueError("Catalog provider returned an invalid table identifier")
        return ".".join(cast(tuple[str, ...], parts))
    raise ValueError("Catalog provider returned an invalid table identifier")


def _valid_identifier_segment(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and len(value) <= 256
        and all(ord(char) >= 0x20 and ord(char) != 0x7F for char in value)
    )
