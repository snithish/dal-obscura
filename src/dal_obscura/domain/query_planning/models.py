from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DatasetSelector:
    """Logical dataset identity used in authz, planning, and ticket validation."""

    target: str
    catalog: str | None = None


@dataclass(frozen=True)
class BackendReference:
    """Stable pointer to a backend instance inside a runtime generation."""

    backend_id: str
    generation: int


@dataclass(frozen=True)
class PlanRequest:
    """Client request for a dataset plus the projected columns to expose."""

    target: str
    columns: list[str]
    catalog: str | None = None
    auth_token: str | None = None


@dataclass(frozen=True)
class ResolvedBackendTarget:
    """Dataset selector plus backend-specific handle information."""

    dataset_identity: DatasetSelector
    backend: BackendReference
    handle: dict[str, Any]


@dataclass(frozen=True)
class ReadPayload:
    """Opaque bytes handed back to the backend during the fetch phase."""

    payload: bytes


@dataclass(frozen=True)
class ReadSpec:
    """Metadata extracted from a read payload without executing the read."""

    dataset: DatasetSelector
    columns: list[str]
    schema: Any


@dataclass(frozen=True)
class Plan:
    """Backend plan containing the output schema and one payload per ticket."""

    schema: Any
    tasks: list[ReadPayload]
