from __future__ import annotations

from typing import Protocol

from dal_obscura.domain.ticket_delivery.models import TicketPayload


class TicketCodecPort(Protocol):
    """Signs and verifies opaque transport tickets."""

    def sign_payload(self, payload: TicketPayload) -> str: ...

    def verify(self, token: str) -> TicketPayload: ...
