"""Ticket-store contract used by data-plane fetch verification."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

from dal_obscura.read.tickets.payload import TicketPayload, TicketReference


@dataclass(frozen=True)
class StoredTicket:
    """Ticket payload plus server-side integrity and exchange metadata."""

    payload: TicketPayload
    payload_hash: str
    exchange_count: int
    max_exchanges: int
    expires_at: int


class TicketStorePort(Protocol):
    """Persists ticket payloads and atomically reserves `do_get` exchanges."""

    def store_many(self, payloads: Iterable[TicketPayload], *, max_exchanges: int) -> None: ...

    def load(self, ticket_id: str) -> StoredTicket: ...

    def reserve_exchange(self, ticket_id: str, *, now: int) -> StoredTicket: ...

    def ensure_active(self, ticket_id: str) -> None: ...

    def cleanup_expired(self, *, now: int) -> int: ...


class TicketCodecPort(Protocol):
    """Signs and verifies opaque transport tickets."""

    def sign_payload(self, payload: TicketPayload) -> str: ...

    def verify(self, token: str) -> TicketReference: ...
