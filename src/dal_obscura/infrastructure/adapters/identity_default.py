from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import jwt

from dal_obscura.domain.access_control import Principal


@dataclass(frozen=True)
class AuthConfig:
    """Authentication settings for API keys and optional JWT validation."""

    api_keys: dict[str, str]
    jwt_secret: str | None = None
    jwt_issuer: str | None = None
    jwt_audience: str | None = None


class DefaultIdentityAdapter:
    """Authenticates callers using bearer JWTs or static API keys."""

    def __init__(self, config: AuthConfig) -> None:
        self._config = config

    def authenticate(self, headers: Mapping[str, str]) -> Principal:
        """Authenticates the request and returns the resolved principal."""
        token = _parse_bearer(headers.get("authorization")) or headers.get("x-api-key")
        if not token:
            raise PermissionError("Missing token")

        jwt_principal = _decode_jwt(token, self._config)
        if jwt_principal:
            return jwt_principal

        api_key_principal = _decode_api_key(token, self._config)
        if api_key_principal:
            return api_key_principal

        raise PermissionError("Invalid token")


def _parse_bearer(header: str | None) -> str | None:
    """Extracts a credential from common Authorization header schemes."""
    if not header:
        return None
    parts = header.split(" ", 1)
    if len(parts) != 2:
        return None
    if parts[0].lower() in {"bearer", "token", "apikey"}:
        return parts[1].strip()
    return None


def _decode_jwt(token: str, config: AuthConfig) -> Principal | None:
    """Validates a JWT and maps common claims into a `Principal`."""
    if not config.jwt_secret:
        return None
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


def _decode_api_key(token: str, config: AuthConfig) -> Principal | None:
    """Resolves a static API key into a lightweight principal."""
    principal_id = config.api_keys.get(token)
    if not principal_id:
        return None
    return Principal(id=principal_id, groups=[], attributes={})
