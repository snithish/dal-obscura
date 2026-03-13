from __future__ import annotations

import base64
import hmac
import json
import os
import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Dict


@dataclass(frozen=True)
class TicketPayload:
    table: str
    columns: list[str]
    scan: dict[str, Any]
    policy_version: int
    principal_id: str
    expires_at: int
    nonce: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "columns": self.columns,
            "scan": self.scan,
            "policy_version": self.policy_version,
            "principal_id": self.principal_id,
            "expires_at": self.expires_at,
            "nonce": self.nonce,
        }


@dataclass(frozen=True)
class Ticket:
    payload: TicketPayload
    signature: str

    def encode(self) -> str:
        raw = json.dumps(self.payload.to_dict(), separators=(",", ":"), sort_keys=True)
        return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("utf-8") + "." + self.signature


class TicketSigner:
    def __init__(self, secret: str) -> None:
        if not secret:
            raise ValueError("Ticket signer secret is required")
        self._secret = secret.encode("utf-8")

    def sign_payload(self, payload: TicketPayload) -> Ticket:
        raw = json.dumps(payload.to_dict(), separators=(",", ":"), sort_keys=True).encode("utf-8")
        signature = hmac.new(self._secret, raw, sha256).hexdigest()
        return Ticket(payload=payload, signature=signature)

    def verify(self, token: str) -> TicketPayload:
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
        return TicketPayload(
            table=str(payload["table"]),
            columns=list(payload.get("columns", [])),
            scan=dict(payload.get("scan", {})),
            policy_version=int(payload.get("policy_version", 0)),
            principal_id=str(payload.get("principal_id", "")),
            expires_at=int(payload.get("expires_at", 0)),
            nonce=str(payload.get("nonce", "")),
        )


def new_ticket_payload(
    table: str,
    columns: list[str],
    scan: dict[str, Any],
    policy_version: int,
    principal_id: str,
    ttl_seconds: int,
) -> TicketPayload:
    return TicketPayload(
        table=table,
        columns=columns,
        scan=scan,
        policy_version=policy_version,
        principal_id=principal_id,
        expires_at=int(time.time()) + ttl_seconds,
        nonce=base64.urlsafe_b64encode(os.urandom(12)).decode("utf-8"),
    )
