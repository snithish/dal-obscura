from __future__ import annotations

import base64
import hmac
import json
import time
from binascii import Error as BinasciiError
from hashlib import sha256

from dal_obscura.common.ticket_delivery.models import (
    TicketPayload,
)


class HmacTicketCodecAdapter:
    """Signs ticket payloads with HMAC-SHA256 and verifies them on fetch."""

    def __init__(self, secret: str) -> None:
        if not secret:
            raise ValueError("Ticket signer secret is required")
        self._secret = secret.encode("utf-8")

    def sign_payload(self, payload: TicketPayload) -> str:
        """Produces a compact opaque `reference.signature` token for transport."""
        if payload.ticket_id is None:
            raise ValueError("ticket_id is required")
        raw = _canonical_reference_bytes(payload)
        signature = hmac.new(self._secret, raw, sha256).hexdigest()
        encoded_payload = base64.urlsafe_b64encode(raw).decode("utf-8")
        return f"{encoded_payload}.{signature}"

    def verify(self, token: str) -> TicketPayload:
        """Verifies the signature and expiry before restoring the ticket payload."""
        try:
            encoded_payload, signature = token.split(".", 1)
        except ValueError as exc:
            raise PermissionError("Invalid ticket format") from exc
        try:
            raw = base64.urlsafe_b64decode(encoded_payload.encode("utf-8"))
            expected = hmac.new(self._secret, raw, sha256).hexdigest()
            if not hmac.compare_digest(expected, signature):
                raise PermissionError("Ticket signature mismatch")

            reference = json.loads(raw.decode("utf-8"))
            if not isinstance(reference, dict):
                raise PermissionError("Invalid ticket payload")
            if int(reference.get("expires_at", 0)) < int(time.time()):
                raise PermissionError("Ticket expired")
            ticket_id = reference.get("ticket_id")
            nonce = reference.get("nonce")
            if not isinstance(ticket_id, str) or not ticket_id:
                raise PermissionError("Invalid ticket payload")
            if not isinstance(nonce, str) or not nonce:
                raise PermissionError("Invalid ticket payload")
            return TicketPayload(
                ticket_id=ticket_id,
                target="",
                columns=[],
                scan={"read_payload": "", "full_row_filter": None, "masks": {}},
                policy_version=0,
                principal_id="",
                expires_at=int(reference["expires_at"]),
                nonce=nonce,
            )
        except PermissionError:
            raise
        except (
            BinasciiError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            raise PermissionError("Invalid ticket payload") from exc


def _canonical_reference_bytes(payload: TicketPayload) -> bytes:
    return json.dumps(
        {
            "expires_at": payload.expires_at,
            "nonce": payload.nonce,
            "ticket_id": payload.ticket_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
