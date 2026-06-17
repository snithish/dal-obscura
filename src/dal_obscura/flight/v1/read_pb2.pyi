from __future__ import annotations

from collections.abc import Iterable

from google.protobuf.message import Message

class PlanRequest(Message):
    protocol_version: int
    catalog: str
    target: str
    columns: list[str]
    row_filter: str

    def __init__(
        self,
        *,
        protocol_version: int = ...,
        catalog: str = ...,
        target: str = ...,
        columns: Iterable[str] = ...,
        row_filter: str = ...,
    ) -> None: ...
