from __future__ import annotations

from typing import Any, Iterable, Protocol

from dal_obscura.domain.query_planning import Plan, ReadSpec


class PlanningBackendPort(Protocol):
    def get_schema(self, table: str) -> Any: ...

    def plan(self, table: str, columns: Iterable[str], max_tickets: int) -> Plan: ...


class ReadBackendPort(Protocol):
    def read_spec(self, read_payload: bytes) -> ReadSpec: ...

    def read_stream(self, read_payload: bytes) -> Iterable[Any]: ...
