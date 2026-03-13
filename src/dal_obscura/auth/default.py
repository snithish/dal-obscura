from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import Dict, Mapping, Optional

import jwt

from .base import Authenticator, AuthResult


@dataclass(frozen=True)
class AuthConfig:
    api_keys: Dict[str, str]
    jwt_secret: Optional[str] = None
    jwt_issuer: Optional[str] = None
    jwt_audience: Optional[str] = None


class DefaultAuthenticator(Authenticator):
    def __init__(self, config: AuthConfig) -> None:
        self._config = config

    def authenticate(self, headers: Mapping[str, str]) -> AuthResult:
        token = _parse_bearer(headers.get("authorization")) or headers.get("x-api-key")
        if not token:
            raise PermissionError("Missing token")

        jwt_result = _decode_jwt(token, self._config)
        if jwt_result:
            return jwt_result

        api_key_result = _decode_api_key(token, self._config)
        if api_key_result:
            return api_key_result

        raise PermissionError("Invalid token")


def issue_api_key(principal_id: str) -> str:
    raw = f"{principal_id}:{int(time.time())}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("utf-8")


def _parse_bearer(header: Optional[str]) -> Optional[str]:
    if not header:
        return None
    parts = header.split(" ", 1)
    if len(parts) != 2:
        return None
    if parts[0].lower() in {"bearer", "token", "apikey"}:
        return parts[1].strip()
    return None


def _decode_jwt(token: str, config: AuthConfig) -> Optional[AuthResult]:
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
    return AuthResult(
        principal_id=principal_id,
        groups=[str(g) for g in groups],
        attributes={str(k): str(v) for k, v in attributes.items()},
    )


def _decode_api_key(token: str, config: AuthConfig) -> Optional[AuthResult]:
    principal_id = config.api_keys.get(token)
    if not principal_id:
        return None
    return AuthResult(principal_id=principal_id, groups=[], attributes={})
