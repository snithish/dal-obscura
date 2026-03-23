from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

DEFAULT_SAMPLE_ROWS = 20_000
DEFAULT_SAMPLE_FILES = 4


class _StrictModel(BaseModel):
    """Base model used by config schemas to reject unknown keys."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class _SchemaInferenceOptionsModel(_StrictModel):
    sample_rows: int = Field(default=DEFAULT_SAMPLE_ROWS, gt=0)
    sample_files: int = Field(default=DEFAULT_SAMPLE_FILES, gt=0)


class _CatalogTargetModel(_StrictModel):
    backend: str | None = None
    table: str | None = None

    @field_validator("backend")
    @classmethod
    def _validate_backend(cls, value: str | None) -> str | None:
        if value is None:
            return None
        backend = value.strip().lower()
        if not backend:
            return None
        if backend != "iceberg":
            raise ValueError(f"Unsupported backend {backend!r}; only 'iceberg' is supported")
        return backend

    @field_validator("table")
    @classmethod
    def _normalize_table(cls, value: str | None) -> str | None:
        if value is None:
            return None
        table = value.strip()
        return table or None


class _CatalogModel(_StrictModel):
    module: str = Field(min_length=1)
    options: dict[str, Any] = Field(default_factory=dict)
    targets: dict[str, _CatalogTargetModel] = Field(default_factory=dict)

    @field_validator("module")
    @classmethod
    def _validate_module(cls, value: str) -> str:
        module = value.strip()
        if not module:
            raise ValueError("Catalog module must be non-empty")
        return module


class _PathConfigModel(_StrictModel):
    glob: str = Field(min_length=1)
    options: _SchemaInferenceOptionsModel = Field(default_factory=_SchemaInferenceOptionsModel)

    @field_validator("glob")
    @classmethod
    def _validate_glob(cls, value: str) -> str:
        glob = value.strip()
        if not glob:
            raise ValueError("Path glob must be non-empty")
        return glob


class _CatalogConfigDocument(_StrictModel):
    catalogs: dict[str, _CatalogModel] = Field(default_factory=dict)
    paths: list[_PathConfigModel] = Field(default_factory=list)


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


def load_catalog_config(path: str | Path) -> ServiceConfig:
    """Loads and validates the YAML catalog configuration file."""
    config_path = Path(path)
    raw = _load_yaml_object(config_path, "Catalog config")
    try:
        document = _CatalogConfigDocument.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Invalid catalog config: {exc}") from exc

    catalogs = {name: _catalog_from_model(name, value) for name, value in document.catalogs.items()}
    paths = tuple(_path_from_model(item) for item in document.paths)
    return ServiceConfig(catalogs=catalogs, paths=paths)


def _load_yaml_object(path: Path, label: str) -> dict[str, object]:
    """Loads a YAML object document from disk."""
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() not in {".yaml", ".yml"}:
        raise ValueError(f"{label} must be .yaml or .yml")
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must contain a YAML object")
    return raw


def _catalog_from_model(name: str, model: _CatalogModel) -> CatalogConfig:
    """Converts validated catalog models into runtime dataclasses."""
    targets = {
        target_name: _catalog_target_from_model(target)
        for target_name, target in model.targets.items()
    }
    return CatalogConfig(
        name=name,
        module=model.module,
        options=dict(model.options),
        targets=targets,
    )


def _catalog_target_from_model(model: _CatalogTargetModel) -> CatalogTargetConfig:
    """Converts one validated target override into runtime dataclass."""
    return CatalogTargetConfig(
        backend=model.backend or "iceberg",
        table=model.table,
    )


def _path_from_model(model: _PathConfigModel) -> PathConfig:
    """Converts validated path models into runtime dataclasses."""
    return PathConfig(
        glob=model.glob,
        options=SchemaInferenceOptions(
            sample_rows=model.options.sample_rows,
            sample_files=model.options.sample_files,
        ),
    )
