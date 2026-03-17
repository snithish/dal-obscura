from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from typing import Any

from dal_obscura.domain.query_planning.models import (
    BackendBinding,
    BackendDescriptor,
    BoundBackendTarget,
    Plan,
    ReadSpec,
)
from dal_obscura.infrastructure.adapters.service_config import CatalogConfig


class CatalogImplementation(ABC):
    """Abstract base for catalog implementations registered in the catalog registry."""

    @abstractmethod
    def resolve(
        self,
        catalog_name: str,
        catalog_config: CatalogConfig,
        target: str,
    ) -> BackendDescriptor:
        """Resolve a logical catalog target into a backend descriptor."""


CatalogRegistration = CatalogImplementation | Callable[[], CatalogImplementation]


class BackendImplementation(ABC):
    """Abstract base for backends registered in the backend registry."""

    @abstractmethod
    def bind(self, descriptor: BackendDescriptor) -> BackendBinding:
        """Materialize a backend-owned binding from a resolved descriptor."""

    @abstractmethod
    def get_schema(self, bound_target: BoundBackendTarget) -> Any:
        """Return the schema for a previously bound dataset."""

    @abstractmethod
    def plan(
        self, bound_target: BoundBackendTarget, columns: Iterable[str], max_tickets: int
    ) -> Plan:
        """Plan read tasks for a previously bound dataset."""

    @abstractmethod
    def read_spec(self, read_payload: bytes) -> ReadSpec:
        """Decode the dataset metadata embedded in a read payload."""

    @abstractmethod
    def read_stream(self, read_payload: bytes) -> Iterable[Any]:
        """Stream records for a previously planned read payload."""


BackendRegistration = BackendImplementation | Callable[[], BackendImplementation]
