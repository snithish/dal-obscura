from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_SAMPLE_ROWS = 20_000
DEFAULT_SAMPLE_FILES = 4


@dataclass(frozen=True)
class SchemaInferenceOptions:
    """Sampling knobs passed to file readers during schema inference."""

    sample_rows: int = DEFAULT_SAMPLE_ROWS
    sample_files: int = DEFAULT_SAMPLE_FILES

    def to_dict(self) -> dict[str, object]:
        """Serializes the options into the plain dict shape stored in descriptors."""
        return {
            "sample_rows": self.sample_rows,
            "sample_files": self.sample_files,
        }


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
class PathConfig:
    """Schema inference options matched by raw path glob."""

    glob: str
    options: SchemaInferenceOptions


@dataclass(frozen=True)
class ServiceConfig:
    """Top-level runtime configuration loaded by the CLI."""

    catalogs: dict[str, CatalogConfig]
    paths: tuple[PathConfig, ...]


def load_service_config(path: str | Path) -> ServiceConfig:
    """Loads and validates the YAML/JSON service configuration file."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(config_path)

    suffix = config_path.suffix.lower()
    if suffix in {".yml", ".yaml"}:
        raw = yaml.safe_load(config_path.read_text())
    elif suffix == ".json":
        raw = json.loads(config_path.read_text())
    else:
        raise ValueError("Service config must be .yaml, .yml, or .json")

    if not isinstance(raw, dict):
        raise ValueError("Service config must contain a JSON/YAML object")

    catalogs = {
        str(name): _parse_catalog(str(name), value)
        for name, value in dict(raw.get("catalogs", {})).items()
    }
    paths = tuple(_parse_path_config(item) for item in raw.get("paths", []) or [])
    return ServiceConfig(catalogs=catalogs, paths=paths)


def _parse_catalog(name: str, raw: Any) -> CatalogConfig:
    """Parses a single catalog entry from the service config."""
    if not isinstance(raw, dict):
        raise ValueError(f"Catalog {name!r} must be an object")
    catalog_module = str(raw.get("module", "")).strip()
    if not catalog_module:
        raise ValueError(f"Catalog {name!r} must define a module")

    options = dict(raw.get("options", {}) or {})
    targets = {
        str(target_name): _parse_catalog_target(str(target_name), target_raw)
        for target_name, target_raw in dict(raw.get("targets", {}) or {}).items()
    }
    return CatalogConfig(name=name, module=catalog_module, options=options, targets=targets)


def _parse_catalog_target(name: str, raw: Any) -> CatalogTargetConfig:
    """Parses a target override and infers the backend when possible."""
    if not isinstance(raw, dict):
        raise ValueError(f"Target {name!r} must be an object")

    backend = raw.get("backend")
    backend_name = str(backend).strip().lower() if backend else None
    file_format = raw.get("format")
    normalized_format = str(file_format).strip().lower() if file_format else None
    if normalized_format is not None and normalized_format not in {"csv", "json", "parquet"}:
        raise ValueError(f"Target {name!r} format must be csv, json, or parquet")

    paths = tuple(str(item) for item in raw.get("paths", []) or [])
    table = str(raw.get("table", "")).strip() or None
    options = _parse_target_options(raw.get("options", {}))

    if backend_name is None:
        if normalized_format is not None or paths:
            backend_name = "duckdb_file"
        elif table is not None:
            backend_name = "iceberg"

    if backend_name == "duckdb_file" and normalized_format is None:
        raise ValueError(f"Target {name!r} with backend duckdb_file must define format")
    if backend_name == "duckdb_file" and not paths:
        raise ValueError(f"Target {name!r} with backend duckdb_file must define paths")

    return CatalogTargetConfig(
        backend=backend_name,
        table=table,
        format=normalized_format,
        paths=paths,
        options=options,
    )


def _parse_path_config(raw: Any) -> PathConfig:
    """Parses one raw-path matching rule used for ad-hoc file targets."""
    if not isinstance(raw, dict):
        raise ValueError("Path config entries must be objects")
    target_glob = str(raw.get("glob", "")).strip()
    if not target_glob:
        raise ValueError("Path config entries must define 'glob'")
    return PathConfig(glob=target_glob, options=_parse_schema_options(raw.get("options", {})))


def _parse_target_options(raw: Any) -> dict[str, Any]:
    """Adds default sampling options to file-backed targets."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("Target options must be an object")
    options = dict(raw)
    if "sample_rows" not in options:
        options["sample_rows"] = DEFAULT_SAMPLE_ROWS
    if "sample_files" not in options:
        options["sample_files"] = DEFAULT_SAMPLE_FILES
    return options


def _parse_schema_options(raw: Any) -> SchemaInferenceOptions:
    """Normalizes schema inference options into strongly typed values."""
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("Schema inference options must be an object")
    return SchemaInferenceOptions(
        sample_rows=int(raw.get("sample_rows", DEFAULT_SAMPLE_ROWS)),
        sample_files=int(raw.get("sample_files", DEFAULT_SAMPLE_FILES)),
    )
