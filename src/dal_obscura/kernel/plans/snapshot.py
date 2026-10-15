"""Detached published configuration captured before provider IO."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from math import isfinite
from types import MappingProxyType
from typing import Any
from uuid import UUID

from dal_obscura.kernel.policy.models import AssetPolicy


def _freeze_json(value: object) -> object:
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float) and isfinite(value):
        return value
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if isinstance(value, list | tuple):
        return tuple(_freeze_json(item) for item in value)
    raise ValueError("Published configuration requires JSON values")


def _freeze_mapping[K](value: Mapping[K, object]) -> Mapping[str, object]:
    frozen: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError("Published JSON objects require string keys")
        frozen[key] = _freeze_json(item)
    return MappingProxyType(frozen)


def _json_value(value: object) -> Any:
    if isinstance(value, Mapping):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, str | bool | int | float):
        return value
    raise ValueError("Published configuration requires JSON values")


@dataclass(frozen=True)
class PublishedCatalog:
    name: str
    plugin_id: str
    revision: int
    options: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "options", _freeze_mapping(self.options))

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "plugin_id": self.plugin_id,
            "revision": self.revision,
            "options": _json_value(self.options),
        }


@dataclass(frozen=True)
class PublishedSource:
    backend: str
    table: str | None
    options: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "options", _freeze_mapping(self.options))

    def to_json(self) -> dict[str, Any]:
        return {"backend": self.backend, "table": self.table, "options": _json_value(self.options)}


@dataclass(frozen=True)
class PublishedSchema:
    encoding: int
    fields: tuple[Mapping[str, object], ...]
    stable_ids: bool
    digest: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", tuple(_freeze_mapping(item) for item in self.fields))

    def to_json(self) -> dict[str, Any]:
        return {
            "encoding": self.encoding,
            "fields": [_json_value(item) for item in self.fields],
            "stable_ids": self.stable_ids,
            "digest": self.digest,
        }


@dataclass(frozen=True)
class PublishedAssetSnapshot:
    catalog: PublishedCatalog
    target: str
    source: PublishedSource
    policy: AssetPolicy
    admitted_schema: PublishedSchema | None = None
    asset_id: UUID | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "asset_id": None if self.asset_id is None else str(self.asset_id),
            "catalog": self.catalog.to_json(),
            "target": self.target,
            "source": self.source.to_json(),
            "policy": self.policy.to_json(),
            "admitted_schema": None
            if self.admitted_schema is None
            else self.admitted_schema.to_json(),
        }
