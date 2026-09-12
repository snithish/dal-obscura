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
from dataclasses import dataclass
from datetime import timedelta, timezone
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from dal_obscura.common.config_store.orm import (
    BrowserSessionRecord,
    LoginTransactionRecord,
    utcnow,
)
from dal_obscura.control_plane.application.access import ControlPlaneActor


class BrowserSessionStore:
    """Creates, resolves, and revokes random browser session secrets."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def issue(self, actor: ControlPlaneActor, *, ttl_seconds: int) -> str:
        token, _csrf_token = self.issue_with_csrf(actor, ttl_seconds=ttl_seconds)
        return token

    def issue_with_csrf(
        self,
        actor: ControlPlaneActor,
        *,
        ttl_seconds: int,
    ) -> tuple[str, str]:
        if ttl_seconds <= 0:
            raise ValueError("session TTL must be positive")
        token = secrets.token_urlsafe(32)
        csrf_token = secrets.token_urlsafe(32)
        now = utcnow()
        self._session.add(
            BrowserSessionRecord(
                id=uuid4(),
                token_hash=_token_hash(token),
                csrf_hash=_token_hash(csrf_token),
                principal=actor.principal,
                groups_json=list(actor.groups),
                platform_admin=actor.platform_admin,
                created_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
                last_seen_at=now,
            )
        )
        self._session.flush()
        return token, csrf_token

    def resolve(
        self,
        token: str,
        *,
        csrf_token: str | None = None,
        idle_ttl_seconds: int | None = None,
    ) -> ControlPlaneActor | None:
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
        if csrf_token is not None and (
            record.csrf_hash is None
            or not secrets.compare_digest(record.csrf_hash, _token_hash(csrf_token))
        ):
            raise ValueError("CSRF secret is not bound to this browser session")
        if idle_ttl_seconds is not None:
            if idle_ttl_seconds <= 0:
                raise ValueError("idle session TTL must be positive")
            last_seen_at = record.last_seen_at
            if last_seen_at.tzinfo is None:
                last_seen_at = last_seen_at.replace(tzinfo=timezone.utc)
            if last_seen_at + timedelta(seconds=idle_ttl_seconds) <= now:
                record.revoked_at = now
                self._session.flush()
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


@dataclass(frozen=True)
class LoginTransaction:
    """Server-held values needed to complete one OIDC callback."""

    nonce_hash: str
    code_verifier: str
    redirect_uri: str


class LoginTransactionStore:
    """Persists and consumes short-lived authorization-code transactions."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def issue(
        self,
        *,
        state: str,
        nonce: str,
        code_verifier: str,
        redirect_uri: str,
        ttl_seconds: int = 600,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("login transaction TTL must be positive")
        now = utcnow()
        self._session.add(
            LoginTransactionRecord(
                id=uuid4(),
                state_hash=_token_hash(state),
                nonce_hash=_token_hash(nonce),
                code_verifier=code_verifier,
                redirect_uri=redirect_uri,
                created_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
            )
        )
        self._session.flush()

    def consume(self, state: str) -> LoginTransaction | None:
        record = self._session.scalar(
            select(LoginTransactionRecord).where(
                LoginTransactionRecord.state_hash == _token_hash(state),
                LoginTransactionRecord.consumed_at.is_(None),
                LoginTransactionRecord.expires_at > utcnow(),
            )
        )
        if record is None:
            return None
        record.consumed_at = utcnow()
        self._session.flush()
        return LoginTransaction(
            nonce_hash=record.nonce_hash,
            code_verifier=record.code_verifier,
            redirect_uri=record.redirect_uri,
        )
