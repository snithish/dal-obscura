from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

import pyarrow as pa


@dataclass(frozen=True)
class ReadPayload:
    payload: bytes


@dataclass(frozen=True)
class Plan:
    schema: pa.Schema
    tasks: list[ReadPayload]


class Backend(Protocol):
    def plan(self, table: str, columns: Iterable[str], max_tickets: int) -> Plan: ...

    def read(self, read_payload: bytes) -> pa.Table: ...
