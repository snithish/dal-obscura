"""Durable opaque browser-session storage.

Example:
    ```python
    token = BrowserSessionStore(session).issue(actor, ttl_seconds=3600)
    actor = BrowserSessionStore(session).resolve(token)
    ```
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from dal_obscura.common.config_store.orm import BrowserSessionRecord, utcnow
from dal_obscura.control_plane.application.access import ControlPlaneActor


class BrowserSessionStore:
    """Creates, resolves, and revokes random browser session secrets."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def issue(self, actor: ControlPlaneActor, *, ttl_seconds: int) -> str:
        if ttl_seconds <= 0:
            raise ValueError("session TTL must be positive")
        token = secrets.token_urlsafe(32)
        now = utcnow()
        self._session.add(
            BrowserSessionRecord(
                id=uuid4(),
                token_hash=_token_hash(token),
                principal=actor.principal,
                groups_json=list(actor.groups),
                platform_admin=actor.platform_admin,
                created_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
                last_seen_at=now,
            )
        )
        self._session.flush()
        return token

    def resolve(self, token: str) -> ControlPlaneActor | None:
        digest = _token_hash(token)
        now = utcnow()
        record = self._session.scalar(
            select(BrowserSessionRecord).where(
                BrowserSessionRecord.token_hash == digest,
                BrowserSessionRecord.revoked_at.is_(None),
                BrowserSessionRecord.expires_at > now,
            )
        )
        if record is None:
            return None
        record.last_seen_at = now
        self._session.flush()
        return ControlPlaneActor(
            principal=record.principal,
            groups=tuple(record.groups_json),
            platform_admin=record.platform_admin,
        )

    def revoke(self, token: str) -> bool:
        record = self._session.scalar(
            select(BrowserSessionRecord).where(
                BrowserSessionRecord.token_hash == _token_hash(token),
                BrowserSessionRecord.revoked_at.is_(None),
            )
        )
        if record is None:
            return False
        record.revoked_at = utcnow()
        self._session.flush()
        return True


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
