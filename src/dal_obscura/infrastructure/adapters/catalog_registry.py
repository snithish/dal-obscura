from __future__ import annotations

import fnmatch
import glob
import importlib
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dal_obscura.domain.catalog.ports import CatalogPlugin, ResolvedTable
from dal_obscura.infrastructure.adapters.duckdb_handler import FileTable
from dal_obscura.infrastructure.adapters.iceberg_handler import IcebergTable
from dal_obscura.infrastructure.adapters.service_config import (
    DEFAULT_SAMPLE_FILES,
    DEFAULT_SAMPLE_ROWS,
    CatalogConfig,
    CatalogTargetConfig,
    PathConfig,
    ServiceConfig,
)

_COMPRESSION_SUFFIXES = (".gz", ".bz2", ".zst")


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

    def describe_catalog(self, catalog_name: str, target: str) -> ResolvedTable:
        """Resolves a target within a named catalog."""
        with self._lock:
            implementation = self._catalog_implementations.get(catalog_name)
            if implementation is None:
                raise ValueError(f"Unknown catalog: {catalog_name}")
        return implementation.get_table(target)

    def describe(self, catalog: str | None, target: str) -> ResolvedTable:
        """Describes either a catalog-based target or a raw filesystem target."""
        if catalog is not None:
            return self.describe_catalog(catalog, target)
        return _resolve_raw_target(self.current_config, target)


class IcebergCatalog:
    """Catalog resolver for SQL-style catalogs configured in the service YAML."""

    def __init__(self, name: str, options: dict[str, Any], targets: dict[str, CatalogTargetConfig]):
        self._name = name
        self.options = options
        self.targets = targets

    @property
    def name(self) -> str:
        return self._name

    def get_table(
        self,
        target: str,
    ) -> ResolvedTable:
        """Resolves a catalog target to Iceberg by default, with target overrides."""
        target_config = _match_catalog_target(self.targets, target)
        backend = target_config.backend if target_config else "iceberg"

        if backend == "iceberg":
            table_identifier = (target_config.table if target_config else None) or target
            return _resolve_iceberg_metadata(self.name, self.options, target, table_identifier)

        if target_config is None:
            raise ValueError(f"Unknown target {target!r} for catalog {self.name!r}")
        return _target_descriptor(self.name, self.options, target, target_config)


class StaticCatalog:
    """Catalog resolver for catalogs that require explicit target mappings in YAML."""

    def __init__(self, name: str, options: dict[str, Any], targets: dict[str, CatalogTargetConfig]):
        self._name = name
        self.options = options
        self.targets = targets

    @property
    def name(self) -> str:
        return self._name

    def get_table(
        self,
        target: str,
    ) -> ResolvedTable:
        """Returns the backend descriptor for the most specific matching target rule."""
        target_config = _match_catalog_target(self.targets, target)
        if target_config is None:
            raise ValueError(f"Unknown target {target!r} for catalog {self.name!r}")
        return _target_descriptor(self.name, self.options, target, target_config)


def _resolve_raw_target(service_config: ServiceConfig, target: str) -> ResolvedTable:
    """Treats the target as a filesystem glob and resolves it to a file descriptor."""
    paths = _expand_paths((target,))
    path_config = _select_path_config(service_config.paths, target)
    options = path_config.options.to_dict() if path_config else _default_file_options()
    file_format = _infer_file_format(paths)
    return FileTable(
        catalog_name="",
        table_name=target,
        format="duckdb_file",
        file_format=file_format,
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
    catalog_name: str,
    catalog_options: dict,
    requested_target: str,
    target_config: CatalogTargetConfig,
) -> ResolvedTable:
    """Builds the format-specific ResolvedTable returned by the catalog."""
    backend_id = target_config.backend or "iceberg"
    if backend_id == "duckdb_file":
        return FileTable(
            catalog_name=catalog_name,
            table_name=requested_target,
            format="duckdb_file",
            file_format=str(target_config.format or ""),
            paths=_expand_paths(target_config.paths),
            options=dict(target_config.options),
        )
    if backend_id == "iceberg":
        return _resolve_iceberg_metadata(
            catalog_name, catalog_options, requested_target, target_config.table or requested_target
        )

    raise ValueError(f"Unknown backend {backend_id!r} for target {requested_target!r}")


def _resolve_iceberg_metadata(
    catalog_name: str,
    catalog_options: dict,
    requested_target: str,
    table_identifier: str,
) -> ResolvedTable:
    """Contacts the Iceberg catalog to resolve the actual metadata location for a table."""
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(catalog_name, **catalog_options)
    try:
        pyiceberg_table = catalog.load_table(table_identifier)
    except Exception as e:
        raise ValueError(
            f"Failed to load table {table_identifier!r} from catalog {catalog_name!r}: {e}"
        ) from e

    return IcebergTable(
        catalog_name=catalog_name,
        table_name=requested_target,
        metadata_location=pyiceberg_table.metadata_location,
        io_options=dict(pyiceberg_table.io.properties),
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
