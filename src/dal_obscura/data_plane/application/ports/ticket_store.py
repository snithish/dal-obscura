"""Ticket-store contract used by data-plane fetch verification.

Example:
    ```python
    store.store(payload, max_exchanges=2)
    stored = store.reserve_exchange(payload.ticket_id, now=now)
    ```
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from dal_obscura.common.ticket_delivery.models import TicketPayload


@dataclass(frozen=True)
class StoredTicket:
    """Ticket payload plus server-side integrity and exchange metadata.

    Example:
        ```python
        assert stored.exchange_count < stored.max_exchanges
        ```
    """

    payload: TicketPayload
    payload_hash: str
    exchange_count: int
    max_exchanges: int
    expires_at: int


class TicketStorePort(Protocol):
    """Persists ticket payloads and atomically reserves `do_get` exchanges.

    Example:
        ```python
        store.store(payload, max_exchanges=1)
        stored = store.load(payload.ticket_id)
        store.reserve_exchange(payload.ticket_id, now=now)
        ```
    """

    def store(self, payload: TicketPayload, *, max_exchanges: int) -> None: ...

    def load(self, ticket_id: str) -> StoredTicket: ...

    def reserve_exchange(self, ticket_id: str, *, now: int) -> StoredTicket: ...

    def cleanup_expired_and_exhausted(self, *, now: int) -> int: ...
