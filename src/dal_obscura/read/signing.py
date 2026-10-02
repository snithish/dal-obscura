from __future__ import annotations

import base64
import hmac
import json
import time
from binascii import Error as BinasciiError
from collections.abc import Iterable
from hashlib import sha256

from dal_obscura.read.tickets import (
    TicketPayload,
    TicketReference,
)


class HmacTicketCodecAdapter:
    """Signs ticket payloads with HMAC-SHA256 and verifies them on fetch."""

    def __init__(self, secret: str, *, previous_secrets: Iterable[str] = ()) -> None:
        keys = (secret, *tuple(previous_secrets))
        if not keys or any(not isinstance(key, str) or not key for key in keys):
            raise ValueError("Ticket signer secret is required")
        if len(set(keys)) != len(keys):
            raise ValueError("Ticket signer keys must be unique")
        self._secrets = tuple(key.encode("utf-8") for key in keys)

    def sign_payload(self, payload: TicketPayload) -> str:
        """Produces a compact opaque `reference.signature` token for transport."""
        if payload.ticket_id is None:
            raise ValueError("ticket_id is required")
        raw = _canonical_reference_bytes(payload)
        signature = hmac.new(self._secrets[0], raw, sha256).hexdigest()
        encoded_payload = base64.urlsafe_b64encode(raw).decode("utf-8")
        return f"{encoded_payload}.{signature}"

    def verify(self, token: str) -> TicketReference:
        """Verifies the signature and expiry before restoring the ticket payload."""
        try:
            encoded_payload, signature = token.split(".", 1)
        except ValueError as exc:
            raise PermissionError("Invalid ticket format") from exc
        try:
            raw = base64.b64decode(
                encoded_payload.encode("ascii"),
                altchars=b"-_",
                validate=True,
            )
            if not any(
                hmac.compare_digest(
                    hmac.new(secret, raw, sha256).hexdigest(),
                    signature,
                )
                for secret in self._secrets
            ):
                raise PermissionError("Ticket signature mismatch")

            reference = json.loads(raw.decode("utf-8"))
            if not isinstance(reference, dict) or set(reference) != {
                "expires_at",
                "nonce",
                "ticket_id",
            }:
                raise PermissionError("Invalid ticket payload")
            expires_at = reference.get("expires_at")
            if isinstance(expires_at, bool) or not isinstance(expires_at, int):
                raise PermissionError("Invalid ticket payload")
            if expires_at <= int(time.time()):
                raise PermissionError("Ticket expired")
            ticket_id = reference.get("ticket_id")
            nonce = reference.get("nonce")
            if not isinstance(ticket_id, str) or not ticket_id:
                raise PermissionError("Invalid ticket payload")
            if not isinstance(nonce, str) or not nonce:
                raise PermissionError("Invalid ticket payload")
            return TicketReference(ticket_id=ticket_id, expires_at=expires_at, nonce=nonce)
        except PermissionError:
            raise
        except (
            BinasciiError,
            UnicodeError,
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
