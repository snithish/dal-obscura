from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


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


class BackendDescriptor(Protocol):
    """Catalog-resolved metadata that identifies which backend should bind the dataset."""

    @property
    def dataset_identity(self) -> DatasetSelector: ...

    @property
    def backend_id(self) -> str: ...


class BackendBinding(Protocol):
    """Backend-owned typed binding produced from a resolved descriptor."""

    @property
    def backend_id(self) -> str: ...


@dataclass(frozen=True)
class PlanRequest:
    """Client request for a dataset plus the projected columns to expose."""

    target: str
    columns: list[str]
    catalog: str | None = None


@dataclass(frozen=True)
class BoundBackendTarget:
    """Dataset identity plus the generation-bound backend reference and binding."""

    dataset_identity: DatasetSelector
    backend: BackendReference
    binding: BackendBinding


@dataclass(frozen=True)
class GenericBackendDescriptor:
    """Escape hatch for custom backends that need catalog-provided opaque data."""

    dataset_identity: DatasetSelector
    backend_id: str
    data: dict[str, Any]


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
