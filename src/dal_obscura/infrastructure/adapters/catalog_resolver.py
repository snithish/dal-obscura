from __future__ import annotations

import fnmatch
import glob
import threading
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Callable, Mapping, Protocol, cast

from dal_obscura.domain.query_planning import (
    BackendReference,
    DatasetSelector,
    ResolvedBackendTarget,
)

from .file_backend import DuckDBFileBackend
from .iceberg_backend import IcebergBackend
from .service_config import (
    DEFAULT_SAMPLE_FILES,
    DEFAULT_SAMPLE_ROWS,
    CatalogConfig,
    CatalogTargetConfig,
    PathConfig,
    ServiceConfig,
)

_COMPRESSION_SUFFIXES = (".gz", ".bz2", ".zst")
_CATALOG_PLUGIN_GROUP = "dal_obscura.catalog_implementations"
_BACKEND_PLUGIN_GROUP = "dal_obscura.backend_implementations"


class CatalogImplementation(Protocol):
    """Pluggable resolver that maps a logical target to a backend handle."""

    def resolve(
        self,
        generation: int,
        catalog_name: str,
        catalog_config: CatalogConfig,
        target: str,
    ) -> ResolvedBackendTarget: ...


@dataclass(frozen=True)
class _RuntimeSnapshot:
    """Immutable backend registry for a single configuration generation."""

    generation: int
    backends: Mapping[str, object]


class DynamicRegistryRuntime:
    """Routes requests to backend implementations using generation-bound handles."""

    def __init__(self, config: ServiceConfig) -> None:
        self._lock = threading.RLock()
        self._catalog_implementations: dict[str, CatalogImplementation] = {}
        self._backend_factories: dict[str, object] = {}
        self._snapshots: dict[int, _RuntimeSnapshot] = {}
        self._current_config = config
        self._current_generation = 0

        self._register_builtin_catalogs()
        self._register_builtin_backends()
        self._register_entrypoints()
        self.reload(config)

    @property
    def current_generation(self) -> int:
        """Generation attached to newly resolved backend references."""
        with self._lock:
            return self._current_generation

    @property
    def current_config(self) -> ServiceConfig:
        """Latest service config currently installed in the runtime."""
        with self._lock:
            return self._current_config

    def register_catalog_type(self, type_name: str, implementation: object) -> None:
        """Registers a catalog implementation under a service-config type name."""
        with self._lock:
            self._catalog_implementations[type_name.lower()] = _coerce_catalog_implementation(
                implementation
            )

    def register_backend(self, backend_id: str, implementation: object) -> None:
        """Registers a backend factory under the ID stored in backend references."""
        with self._lock:
            self._backend_factories[backend_id] = implementation

    def reload(self, config: ServiceConfig) -> int:
        """Publishes a new backend snapshot and increments the active generation."""
        with self._lock:
            self._current_config = config
            self._current_generation += 1
            generation = self._current_generation
            # Tickets include the backend generation so fetch requests can keep
            # using the exact backend snapshot they were planned against.
            self._snapshots[generation] = _RuntimeSnapshot(
                generation=generation,
                backends={
                    backend_id: _coerce_backend_implementation(implementation)
                    for backend_id, implementation in self._backend_factories.items()
                },
            )
            return generation

    def unload_generation(self, generation: int) -> None:
        """Drops an old generation once tickets for it can no longer be used."""
        with self._lock:
            if generation == self._current_generation:
                raise ValueError("Cannot unload the current backend generation")
            self._snapshots.pop(generation, None)

    def resolve_catalog(self, catalog_name: str, target: str) -> ResolvedBackendTarget:
        """Resolves a target within a named catalog."""
        with self._lock:
            catalog_config = self._current_config.catalogs.get(catalog_name)
            generation = self._current_generation
            if catalog_config is None:
                raise ValueError(f"Unknown catalog: {catalog_name}")
            implementation = self._catalog_implementations.get(catalog_config.type)
            if implementation is None:
                raise ValueError(f"Unknown catalog type: {catalog_config.type}")
        resolved = implementation.resolve(generation, catalog_name, catalog_config, target)
        self._backend_for_reference(resolved.backend)
        return resolved

    def resolve(self, catalog: str | None, target: str) -> ResolvedBackendTarget:
        """Resolves either a catalog-based target or a raw filesystem target."""
        if catalog is not None:
            return self.resolve_catalog(catalog, target)
        return _resolve_raw_target(self.current_generation, self.current_config, target)

    def get_schema(self, target: ResolvedBackendTarget):
        """Delegates schema lookup to the resolved backend implementation."""
        backend = self._backend_for_reference(target.backend)
        return backend.get_schema(target)

    def plan(self, target: ResolvedBackendTarget, columns, max_tickets: int):
        """Delegates plan creation to the resolved backend implementation."""
        backend = self._backend_for_reference(target.backend)
        return backend.plan(target, columns, max_tickets)

    def read_spec(self, backend: BackendReference, read_payload: bytes):
        """Reads ticket metadata using the generation-bound backend implementation."""
        implementation = self._backend_for_reference(backend)
        return implementation.read_spec(read_payload)

    def read_stream(self, backend: BackendReference, read_payload: bytes):
        """Executes a read stream using the generation-bound backend implementation."""
        implementation = self._backend_for_reference(backend)
        return implementation.read_stream(read_payload)

    def _backend_for_reference(self, reference: BackendReference):
        """Looks up the backend instance referenced by a ticket or plan result."""
        with self._lock:
            snapshot = self._snapshots.get(reference.generation)
            if snapshot is None:
                raise ValueError(f"Backend generation {reference.generation} is unavailable")
            backend = snapshot.backends.get(reference.backend_id)
            if backend is None:
                raise ValueError(
                    f"Backend {reference.backend_id!r} is unavailable in generation "
                    f"{reference.generation}"
                )
            return backend

    def _register_builtin_catalogs(self) -> None:
        """Registers the built-in catalog types shipped with the service."""
        default_iceberg_catalog = MappedCatalogImplementation(default_backend_id="iceberg")
        for type_name in ("sql", "glue", "hive", "unity", "polaris"):
            self._catalog_implementations[type_name] = default_iceberg_catalog
        self._catalog_implementations["static"] = MappedCatalogImplementation(
            default_backend_id=None
        )

    def _register_builtin_backends(self) -> None:
        """Registers the built-in backends available without plugins."""
        self._backend_factories["iceberg"] = IcebergBackend
        self._backend_factories["duckdb_file"] = DuckDBFileBackend

    def _register_entrypoints(self) -> None:
        """Loads optional catalog and backend plugins from Python entry points."""
        for entrypoint in _entry_points_for_group(_CATALOG_PLUGIN_GROUP):
            self.register_catalog_type(entrypoint.name, entrypoint.load())
        for entrypoint in _entry_points_for_group(_BACKEND_PLUGIN_GROUP):
            self.register_backend(entrypoint.name, entrypoint.load())


class MappedCatalogImplementation:
    """Catalog resolver that uses static target matching rules from config."""

    def __init__(self, default_backend_id: str | None) -> None:
        self._default_backend_id = default_backend_id

    def resolve(
        self,
        generation: int,
        catalog_name: str,
        catalog_config: CatalogConfig,
        target: str,
    ) -> ResolvedBackendTarget:
        """Returns the backend handle for the most specific matching target rule."""
        selector = DatasetSelector(catalog=catalog_name, target=target)
        target_config = _match_catalog_target(catalog_config.targets, target)
        if target_config is None:
            if self._default_backend_id is None:
                raise ValueError(f"Unknown target {target!r} for catalog {catalog_name!r}")
            return ResolvedBackendTarget(
                dataset_identity=selector,
                backend=BackendReference(
                    backend_id=self._default_backend_id,
                    generation=generation,
                ),
                handle={
                    "catalog_name": catalog_name,
                    "catalog_type": catalog_config.type,
                    "catalog_options": dict(catalog_config.options),
                    "table_identifier": target,
                },
            )

        backend_id = target_config.backend or "iceberg"
        handle = _target_handle(catalog_name, catalog_config, target, target_config)
        return ResolvedBackendTarget(
            dataset_identity=selector,
            backend=BackendReference(backend_id=backend_id, generation=generation),
            handle=handle,
        )


def _resolve_raw_target(
    generation: int, service_config: ServiceConfig, target: str
) -> ResolvedBackendTarget:
    """Treats the target as a filesystem glob and resolves it to the file backend."""
    selector = DatasetSelector(target=target)
    paths = _expand_paths((target,))
    path_config = _select_path_config(service_config.paths, target)
    options = path_config.options.to_dict() if path_config else _default_file_options()
    file_format = _infer_file_format(paths)
    return ResolvedBackendTarget(
        dataset_identity=selector,
        backend=BackendReference(backend_id="duckdb_file", generation=generation),
        handle={
            "format": file_format,
            "paths": list(paths),
            "options": options,
        },
    )


def _default_file_options() -> dict[str, int]:
    """Default schema inference settings for ad-hoc file targets."""
    return {
        "sample_rows": DEFAULT_SAMPLE_ROWS,
        "sample_files": DEFAULT_SAMPLE_FILES,
    }


def _target_handle(
    catalog_name: str,
    catalog_config: CatalogConfig,
    requested_target: str,
    target_config: CatalogTargetConfig,
) -> dict[str, object]:
    """Builds the backend-specific handle stored in `ResolvedBackendTarget`."""
    backend_id = target_config.backend or "iceberg"
    if backend_id == "duckdb_file":
        return {
            "catalog_name": catalog_name,
            "catalog_type": catalog_config.type,
            "requested_target": requested_target,
            "format": target_config.format,
            "paths": list(_expand_paths(target_config.paths)),
            "options": dict(target_config.options),
        }
    if backend_id == "iceberg":
        return {
            "catalog_name": catalog_name,
            "catalog_type": catalog_config.type,
            "catalog_options": dict(catalog_config.options),
            "table_identifier": target_config.table or requested_target,
        }
    return {
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
    }


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
    if (
        lower_path.endswith(".json")
        or lower_path.endswith(".jsonl")
        or lower_path.endswith(".ndjson")
    ):
        return "json"
    if lower_path.endswith(".parquet"):
        return "parquet"
    return None


def _coerce_catalog_implementation(candidate: object) -> CatalogImplementation:
    """Instantiates or validates a catalog implementation registered by the runtime."""
    if isinstance(candidate, type):
        candidate = candidate()
    if hasattr(candidate, "resolve"):
        return candidate  # type: ignore[return-value]
    if callable(candidate):
        created = cast(Callable[[], object], candidate)()
        if hasattr(created, "resolve"):
            return created  # type: ignore[return-value]
    raise TypeError("Catalog implementation must provide resolve()")


def _coerce_backend_implementation(candidate: object) -> object:
    """Instantiates or validates a backend implementation registered by the runtime."""
    if isinstance(candidate, type):
        candidate = candidate()
    if (
        hasattr(candidate, "get_schema")
        and hasattr(candidate, "plan")
        and hasattr(candidate, "read_spec")
    ):
        return candidate
    if callable(candidate):
        created = cast(Callable[[], object], candidate)()
        if (
            hasattr(created, "get_schema")
            and hasattr(created, "plan")
            and hasattr(created, "read_spec")
        ):
            return created
    raise TypeError("Backend implementation must provide get_schema(), plan(), and read_spec()")


def _entry_points_for_group(group: str):
    """Handles `importlib.metadata.entry_points()` API differences across Python versions."""
    discovered = metadata.entry_points()
    if hasattr(discovered, "select"):
        return list(discovered.select(group=group))
    return [entrypoint for entrypoint in discovered.get(group, [])]
