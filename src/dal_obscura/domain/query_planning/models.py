from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DatasetSelector:
    target: str
    catalog: str | None = None


@dataclass(frozen=True)
class BackendReference:
    backend_id: str
    generation: int


@dataclass(frozen=True)
class PlanRequest:
    target: str
    columns: list[str]
    catalog: str | None = None
    auth_token: str | None = None


@dataclass(frozen=True)
class ResolvedBackendTarget:
    dataset_identity: DatasetSelector
    backend: BackendReference
    handle: dict[str, Any]


@dataclass(frozen=True)
class ReadPayload:
    payload: bytes


@dataclass(frozen=True)
class ReadSpec:
    dataset: DatasetSelector
    columns: list[str]
    schema: Any


@dataclass(frozen=True)
class Plan:
    schema: Any
    tasks: list[ReadPayload]
