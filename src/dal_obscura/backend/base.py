from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

import pyarrow as pa


@dataclass(frozen=True)
class ReadPayload:
    payload: bytes


@dataclass(frozen=True)
class ReadSpec:
    table: str
    columns: list[str]


@dataclass(frozen=True)
class Plan:
    schema: pa.Schema
    tasks: list[ReadPayload]


class Backend(Protocol):
    def get_schema(self, table: str) -> pa.Schema: ...

    def plan(self, table: str, columns: Iterable[str], max_tickets: int) -> Plan: ...

    def read_spec(self, read_payload: bytes) -> ReadSpec: ...

    def read_stream(self, read_payload: bytes) -> Iterable[pa.RecordBatch]: ...
