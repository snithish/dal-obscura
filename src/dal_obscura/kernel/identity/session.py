"""Detached login/session values and verified-claim resolver contracts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

OidcActorResolver = Callable[[str], object]
OidcNonceActorResolver = Callable[[str, str], object]


@dataclass(frozen=True)
class LoginRateLimitDecision:
    """Result of a durable login limiter check."""

    allowed: bool
    retry_after_seconds: int = 0


@dataclass(frozen=True)
class LoginTransaction:
    """Server-held values needed to complete one OIDC callback."""

    nonce_hash: str
    code_verifier: str
    redirect_uri: str
