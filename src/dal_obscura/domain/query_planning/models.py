from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PlanRequest:
    table: str
    columns: list[str]
    auth_token: str | None = None


@dataclass(frozen=True)
class ReadPayload:
    payload: bytes


@dataclass(frozen=True)
class ReadSpec:
    table: str
    columns: list[str]


@dataclass(frozen=True)
class Plan:
    schema: Any
    tasks: list[ReadPayload]
