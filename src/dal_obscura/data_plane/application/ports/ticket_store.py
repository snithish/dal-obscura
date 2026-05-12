from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from dal_obscura.common.ticket_delivery.models import TicketPayload


@dataclass(frozen=True)
class StoredTicket:
    payload: TicketPayload
    payload_hash: str
    exchange_count: int
    max_exchanges: int
    expires_at: int


class TicketStorePort(Protocol):
    def store(self, payload: TicketPayload, *, max_exchanges: int) -> None: ...

    def load(self, ticket_id: str) -> StoredTicket: ...

    def reserve_exchange(self, ticket_id: str, *, now: int) -> StoredTicket: ...

    def cleanup_expired_and_exhausted(self, *, now: int) -> int: ...
