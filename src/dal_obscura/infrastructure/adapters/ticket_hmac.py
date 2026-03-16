from __future__ import annotations

import base64
import hmac
import json
import time
from hashlib import sha256

from dal_obscura.domain.ticket_delivery import TicketPayload


class HmacTicketCodecAdapter:
    """Signs ticket payloads with HMAC-SHA256 and verifies them on fetch."""

    def __init__(self, secret: str) -> None:
        if not secret:
            raise ValueError("Ticket signer secret is required")
        self._secret = secret.encode("utf-8")

    def sign_payload(self, payload: TicketPayload) -> str:
        """Produces a compact `payload.signature` token for transport."""
        raw = json.dumps(payload.to_dict(), separators=(",", ":"), sort_keys=True).encode("utf-8")
        signature = hmac.new(self._secret, raw, sha256).hexdigest()
        encoded_payload = base64.urlsafe_b64encode(raw).decode("utf-8")
        return f"{encoded_payload}.{signature}"

    def verify(self, token: str) -> TicketPayload:
        """Verifies the signature and expiry before restoring the ticket payload."""
        try:
            encoded_payload, signature = token.split(".", 1)
        except ValueError as exc:
            raise PermissionError("Invalid ticket format") from exc
        raw = base64.urlsafe_b64decode(encoded_payload.encode("utf-8"))
        expected = hmac.new(self._secret, raw, sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise PermissionError("Ticket signature mismatch")

        payload = json.loads(raw.decode("utf-8"))
        if int(payload.get("expires_at", 0)) < int(time.time()):
            raise PermissionError("Ticket expired")
        return TicketPayload.from_dict(payload)
