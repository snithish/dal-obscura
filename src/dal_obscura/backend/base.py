from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

import pyarrow as pa


@dataclass(frozen=True)
class ScanTask:
    descriptor: dict


@dataclass(frozen=True)
class Plan:
    snapshot: str
    tasks: list[ScanTask]


class Backend(Protocol):
    def plan(self, table: str, columns: Iterable[str]) -> Plan: ...

    def read(self, table: str, snapshot: str, task: ScanTask) -> pa.Table: ...
