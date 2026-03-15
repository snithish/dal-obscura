from __future__ import annotations

from typing import Protocol

from dal_obscura.domain.ticket_delivery import TicketPayload


class TicketCodecPort(Protocol):
    def sign_payload(self, payload: TicketPayload) -> str: ...

    def verify(self, token: str) -> TicketPayload: ...
