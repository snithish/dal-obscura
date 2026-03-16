from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol

from dal_obscura.domain.query_planning.models import (
    BackendReference,
    Plan,
    ReadSpec,
    ResolvedBackendTarget,
)


class QueryBackendPort(Protocol):
    """Abstraction over dataset resolution, planning, and record retrieval."""

    def resolve(self, catalog: str | None, target: str) -> ResolvedBackendTarget: ...

    def get_schema(self, target: ResolvedBackendTarget) -> Any: ...

    def plan(
        self, target: ResolvedBackendTarget, columns: Iterable[str], max_tickets: int
    ) -> Plan: ...

    def read_spec(self, backend: BackendReference, read_payload: bytes) -> ReadSpec: ...

    def read_stream(self, backend: BackendReference, read_payload: bytes) -> Iterable[Any]: ...
