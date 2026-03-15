from __future__ import annotations

from typing import Any, Iterable, Protocol

from dal_obscura.domain.query_planning import (
    BackendReference,
    Plan,
    ReadSpec,
    ResolvedBackendTarget,
)


class QueryBackendPort(Protocol):
    def resolve(self, catalog: str | None, target: str) -> ResolvedBackendTarget: ...

    def get_schema(self, target: ResolvedBackendTarget) -> Any: ...

    def plan(
        self, target: ResolvedBackendTarget, columns: Iterable[str], max_tickets: int
    ) -> Plan: ...

    def read_spec(self, backend: BackendReference, read_payload: bytes) -> ReadSpec: ...

    def read_stream(self, backend: BackendReference, read_payload: bytes) -> Iterable[Any]: ...
