"""Authenticated, short-lived catalog cursors with no server-side browse state."""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import time

from cryptography.fernet import Fernet, InvalidToken

from dal_obscura.application.governance.errors import ValidationFailure

CURSOR_TTL_SECONDS = 600
MAX_CURSOR_LENGTH = 12_000
_INVALID_CURSOR = "Discovery cursor is invalid or expired; restart discovery"


class DiscoveryCursors:
    def __init__(self, secret: bytes | None = None) -> None:
        key = secrets.token_bytes(32) if secret is None else secret
        if len(key) < 32:
            raise ValueError("Catalog cursor secret must contain at least 32 bytes")
        derived = hashlib.sha256(b"dal-obscura:catalog-discovery\x00" + key).digest()
        self._fernet = Fernet(base64.urlsafe_b64encode(derived))

    def issue(self, continuation: str, *, scope: tuple[str, int, str, int]) -> str:
        payload = json.dumps([scope, continuation], separators=(",", ":"), ensure_ascii=True)
        token = self._fernet.encrypt(payload.encode()).decode("ascii")
        if len(token) > MAX_CURSOR_LENGTH:
            raise ValidationFailure("Catalog continuation exceeds the cursor budget")
        return token

    def resolve(self, token: str, *, scope: tuple[str, int, str, int]) -> str:
        try:
            if len(token) > MAX_CURSOR_LENGTH:
                raise ValueError("oversized cursor")
            payload = self._fernet.decrypt_at_time(
                token.encode("ascii"), ttl=CURSOR_TTL_SECONDS, current_time=int(time.time())
            )
            value = json.loads(payload)
            if (
                not isinstance(value, list)
                or len(value) != 2
                or value[0] != list(scope)
                or not isinstance(value[1], str)
                or not value[1]
            ):
                raise ValueError("cursor scope changed")
            return value[1]
        except (InvalidToken, ValueError, UnicodeError) as exc:
            raise ValidationFailure(_INVALID_CURSOR) from exc
