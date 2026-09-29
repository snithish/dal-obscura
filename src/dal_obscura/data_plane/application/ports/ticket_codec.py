from __future__ import annotations

from typing import Protocol

from dal_obscura.common.ticket_delivery.models import TicketPayload, TicketReference


class TicketCodecPort(Protocol):
    """Signs and verifies opaque transport tickets."""

    def sign_payload(self, payload: TicketPayload) -> str: ...

    def verify(self, token: str) -> TicketReference: ...
