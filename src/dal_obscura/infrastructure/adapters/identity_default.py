from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import jwt

from dal_obscura.domain.access_control.models import Principal


@dataclass(frozen=True)
class AuthConfig:
    """Authentication settings for JWT validation."""

    jwt_secret: str
    jwt_issuer: str | None = None
    jwt_audience: str | None = None


class DefaultIdentityAdapter:
    """Authenticates callers using bearer JWTs from the Authorization header."""

    def __init__(self, config: AuthConfig) -> None:
        self._config = config

    def authenticate(self, headers: Mapping[str, str]) -> Principal:
        """Authenticates the request and returns the resolved principal."""
        token = _parse_bearer(headers.get("authorization"))
        if not token:
            raise PermissionError("Missing token")

        jwt_principal = _decode_jwt(token, self._config)
        if jwt_principal:
            return jwt_principal

        raise PermissionError("Invalid token")


def _parse_bearer(header: str | None) -> str | None:
    """Extracts the bearer token from an Authorization header."""
    if not header:
        return None
    parts = header.split(" ", 1)
    if len(parts) != 2:
        return None
    if parts[0].lower() == "bearer":
        return parts[1].strip()
    return None


def _decode_jwt(token: str, config: AuthConfig) -> Principal | None:
    """Validates a JWT and maps common claims into a `Principal`."""
    try:
        payload = jwt.decode(
            token,
            config.jwt_secret,
            algorithms=["HS256"],
            audience=config.jwt_audience,
            issuer=config.jwt_issuer,
        )
    except jwt.PyJWTError:
        return None

    principal_id = str(payload.get("sub") or payload.get("principal") or "")
    if not principal_id:
        return None
    groups = payload.get("groups") or []
    attributes = payload.get("attrs") or payload.get("attributes") or {}
    return Principal(
        id=principal_id,
        groups=[str(group) for group in groups],
        attributes={str(key): str(value) for key, value in attributes.items()},
    )
