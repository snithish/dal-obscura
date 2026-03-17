from __future__ import annotations

import fnmatch
import glob
import threading
from collections.abc import Callable, Mapping
from importlib import metadata
from pathlib import Path
from typing import cast

from dal_obscura.domain.query_planning.models import (
    BackendDescriptor,
    DatasetSelector,
    GenericBackendDescriptor,
)
from dal_obscura.infrastructure.adapters.file_backend import FileTableDescriptor
from dal_obscura.infrastructure.adapters.iceberg_backend import IcebergTableDescriptor
from dal_obscura.infrastructure.adapters.implementation_base import (
    CatalogImplementation,
    CatalogRegistration,
)
from dal_obscura.infrastructure.adapters.service_config import (
    DEFAULT_SAMPLE_FILES,
    DEFAULT_SAMPLE_ROWS,
    CatalogConfig,
    CatalogTargetConfig,
    PathConfig,
    ServiceConfig,
)

_COMPRESSION_SUFFIXES = (".gz", ".bz2", ".zst")
_CATALOG_PLUGIN_GROUP = "dal_obscura.catalog_implementations"


class DynamicCatalogRegistry:
    """Stores catalog implementations and resolves dataset requests into descriptors."""

    def __init__(self, config: ServiceConfig) -> None:
        self._lock = threading.RLock()
        self._catalog_implementations: dict[str, CatalogImplementation] = {}
        self._current_config = config

        self._register_builtin_catalogs()
        self._register_entrypoints()
        self.reload(config)

    @property
    def current_config(self) -> ServiceConfig:
        """Latest service config currently installed in the registry."""
        with self._lock:
            return self._current_config

    def register_catalog_type(self, type_name: str, implementation: CatalogRegistration) -> None:
        """Registers a catalog implementation under a service-config type name."""
        with self._lock:
            self._catalog_implementations[type_name.lower()] = _coerce_catalog_implementation(
                implementation
            )

    def reload(self, config: ServiceConfig) -> None:
        """Publishes the latest catalog configuration."""
        with self._lock:
            self._current_config = config

    def describe_catalog(self, catalog_name: str, target: str) -> BackendDescriptor:
        """Resolves a target within a named catalog."""
        with self._lock:
            catalog_config = self._current_config.catalogs.get(catalog_name)
            if catalog_config is None:
                raise ValueError(f"Unknown catalog: {catalog_name}")
            implementation = self._catalog_implementations.get(catalog_config.type)
            if implementation is None:
                raise ValueError(f"Unknown catalog type: {catalog_config.type}")
        return implementation.resolve(catalog_name, catalog_config, target)

    def describe(self, catalog: str | None, target: str) -> BackendDescriptor:
        """Describes either a catalog-based target or a raw filesystem target."""
        if catalog is not None:
            return self.describe_catalog(catalog, target)
        return _resolve_raw_target(self.current_config, target)

    def _register_builtin_catalogs(self) -> None:
        """Registers the built-in catalog types shipped with the service."""
        iceberg_catalog = IcebergCatalogImplementation()
        for type_name in ("sql", "glue", "hive", "unity", "polaris"):
            self._catalog_implementations[type_name] = iceberg_catalog
        self._catalog_implementations["static"] = StaticCatalogImplementation()

    def _register_entrypoints(self) -> None:
        """Loads optional catalog plugins from Python entry points."""
        for entrypoint in _entry_points_for_group(_CATALOG_PLUGIN_GROUP):
            self.register_catalog_type(entrypoint.name, entrypoint.load())


class IcebergCatalogImplementation(CatalogImplementation):
    """Catalog resolver for SQL-style catalogs configured in the service YAML."""

    def resolve(
        self,
        catalog_name: str,
        catalog_config: CatalogConfig,
        target: str,
    ) -> BackendDescriptor:
        """Resolves a catalog target to Iceberg by default, with target overrides."""
        selector = DatasetSelector(catalog=catalog_name, target=target)
        target_config = _match_catalog_target(catalog_config.targets, target)
        if target_config is None:
            return IcebergTableDescriptor(
                dataset_identity=selector,
                catalog_name=catalog_name,
                catalog_type=catalog_config.type,
                catalog_options=dict(catalog_config.options),
                table_identifier=target,
            )
        return _target_descriptor(selector, catalog_name, catalog_config, target, target_config)


class StaticCatalogImplementation(CatalogImplementation):
    """Catalog resolver for catalogs that require explicit target mappings in YAML."""

    def resolve(
        self,
        catalog_name: str,
        catalog_config: CatalogConfig,
        target: str,
    ) -> BackendDescriptor:
        """Returns the backend descriptor for the most specific matching target rule."""
        selector = DatasetSelector(catalog=catalog_name, target=target)
        target_config = _match_catalog_target(catalog_config.targets, target)
        if target_config is None:
            raise ValueError(f"Unknown target {target!r} for catalog {catalog_name!r}")
        return _target_descriptor(selector, catalog_name, catalog_config, target, target_config)


def _resolve_raw_target(service_config: ServiceConfig, target: str) -> BackendDescriptor:
    """Treats the target as a filesystem glob and resolves it to a file descriptor."""
    selector = DatasetSelector(target=target)
    paths = _expand_paths((target,))
    path_config = _select_path_config(service_config.paths, target)
    options = path_config.options.to_dict() if path_config else _default_file_options()
    file_format = _infer_file_format(paths)
    return FileTableDescriptor(
        dataset_identity=selector,
        format=file_format,
        paths=paths,
        options=options,
    )


def _default_file_options() -> dict[str, object]:
    """Default schema inference settings for ad-hoc file targets."""
    return {
        "sample_rows": DEFAULT_SAMPLE_ROWS,
        "sample_files": DEFAULT_SAMPLE_FILES,
    }


def _target_descriptor(
    selector: DatasetSelector,
    catalog_name: str,
    catalog_config: CatalogConfig,
    requested_target: str,
    target_config: CatalogTargetConfig,
) -> BackendDescriptor:
    """Builds the backend-specific descriptor returned by the catalog registry."""
    backend_id = target_config.backend or "iceberg"
    if backend_id == "duckdb_file":
        return FileTableDescriptor(
            dataset_identity=selector,
            format=str(target_config.format or ""),
            paths=_expand_paths(target_config.paths),
            options=dict(target_config.options),
        )
    if backend_id == "iceberg":
        return IcebergTableDescriptor(
            dataset_identity=selector,
            catalog_name=catalog_name,
            catalog_type=catalog_config.type,
            catalog_options=dict(catalog_config.options),
            table_identifier=target_config.table or requested_target,
        )
    return GenericBackendDescriptor(
        dataset_identity=selector,
        backend_id=backend_id,
        data={
            "catalog_name": catalog_name,
            "catalog_type": catalog_config.type,
            "catalog_options": dict(catalog_config.options),
            "requested_target": requested_target,
            "target_config": {
                "backend": target_config.backend,
                "table": target_config.table,
                "format": target_config.format,
                "paths": list(target_config.paths),
                "options": dict(target_config.options),
            },
        },
    )


def _match_catalog_target(
    targets: Mapping[str, CatalogTargetConfig], target: str
) -> CatalogTargetConfig | None:
    """Returns the most specific target pattern that matches the request."""
    matches = [
        (pattern, config) for pattern, config in targets.items() if fnmatch.fnmatch(target, pattern)
    ]
    if not matches:
        return None
    return max(matches, key=lambda item: len(item[0]))[1]


def _expand_paths(patterns: tuple[str, ...]) -> tuple[str, ...]:
    """Expands globs into a stable, de-duplicated list of files."""
    expanded: list[str] = []
    for pattern in patterns:
        matches = sorted(glob.glob(pattern, recursive=True))
        expanded.extend(match for match in matches if Path(match).is_file())
    unique_paths = tuple(dict.fromkeys(expanded))
    if not unique_paths:
        raise ValueError("Target did not resolve to any files")
    return unique_paths


def _select_path_config(path_configs: tuple[PathConfig, ...], target: str) -> PathConfig | None:
    """Returns the most specific raw-path config that matches the target."""
    matches = [config for config in path_configs if fnmatch.fnmatch(target, config.glob)]
    if not matches:
        return None
    return max(matches, key=lambda config: len(config.glob))


def _infer_file_format(paths: tuple[str, ...]) -> str:
    """Ensures every resolved file shares the same supported format."""
    formats = {_path_format(path) for path in paths}
    if len(formats) != 1:
        raise ValueError("Target resolved to mixed or unsupported file formats")
    file_format = formats.pop()
    if file_format is None:
        raise ValueError("Target resolved to mixed or unsupported file formats")
    return file_format


def _path_format(path: str) -> str | None:
    """Infers the data format from the file suffix, ignoring compression suffixes."""
    lower_path = path.lower()
    for suffix in _COMPRESSION_SUFFIXES:
        if lower_path.endswith(suffix):
            lower_path = lower_path[: -len(suffix)]
            break
    if lower_path.endswith(".csv"):
        return "csv"
    if lower_path.endswith((".json", ".jsonl", ".ndjson")):
        return "json"
    if lower_path.endswith(".parquet"):
        return "parquet"
    return None


def _coerce_catalog_implementation(candidate: CatalogRegistration) -> CatalogImplementation:
    """Instantiates or validates a catalog implementation registered by the runtime."""
    if isinstance(candidate, CatalogImplementation):
        return candidate
    if isinstance(candidate, type):
        if issubclass(candidate, CatalogImplementation):
            return candidate()
        raise TypeError("Catalog implementation must provide resolve()")
    try:
        created = cast(Callable[[], CatalogImplementation], candidate)()
    except TypeError as exc:
        raise TypeError("Catalog implementation must provide resolve()") from exc
    if isinstance(created, CatalogImplementation):
        return created
    raise TypeError("Catalog implementation must provide resolve()")


def _entry_points_for_group(group: str):
    """Handles `importlib.metadata.entry_points()` API differences across Python versions."""
    discovered = metadata.entry_points()
    if hasattr(discovered, "select"):
        return list(discovered.select(group=group))
    return list(discovered.get(group, []))
