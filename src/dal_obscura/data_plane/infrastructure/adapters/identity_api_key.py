from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from typing import Any

from dal_obscura.common.access_control.models import Principal
from dal_obscura.data_plane.application.ports.identity import (
    AuthenticationRequest,
    InvalidCredentialsError,
    MissingCredentialsError,
)


@dataclass(frozen=True)
class _ApiKeyRecord:
    id: str
    digest: bytes
    groups: list[str]
    attributes: dict[str, str]


class ApiKeyIdentityProvider:
    """Authenticates static service-account API keys from a configured header."""

    def __init__(
        self,
        *,
        keys: list[dict[str, Any]],
        header: str = "x-api-key",
        scheme: str | None = None,
    ) -> None:
        if not keys:
            raise ValueError("ApiKeyIdentityProvider requires at least one key")
        self._header = header.lower()
        self._scheme = scheme.lower() if scheme else None
        self._keys = [_record_from_config(item) for item in keys]

    def authenticate(self, request: AuthenticationRequest) -> Principal:
        raw_value = request.header(self._header)
        token = self._extract_token(raw_value)
        if not token:
            raise MissingCredentialsError("Missing API key")
        digest = _digest(token)
        for key in self._keys:
            if hmac.compare_digest(digest, key.digest):
                return Principal(id=key.id, groups=key.groups, attributes=key.attributes)
        raise InvalidCredentialsError("Invalid API key")

    def _extract_token(self, raw_value: str | None) -> str:
        if not raw_value:
            return ""
        value = raw_value.strip()
        if not self._scheme:
            return value
        prefix = f"{self._scheme} "
        if not value.lower().startswith(prefix):
            return ""
        return value[len(prefix) :].strip()


def _record_from_config(raw: dict[str, Any]) -> _ApiKeyRecord:
    principal_id = str(raw.get("id") or "").strip()
    secret = str(raw.get("secret") or "").strip()
    if not principal_id:
        raise ValueError("API key entry requires id")
    if not secret:
        raise ValueError(f"API key entry {principal_id!r} requires secret")
    groups = [str(group) for group in raw.get("groups", [])]
    raw_attributes = raw.get("attributes", {})
    if not isinstance(raw_attributes, dict):
        raise ValueError(f"API key entry {principal_id!r} attributes must be an object")
    return _ApiKeyRecord(
        id=principal_id,
        digest=_digest(secret),
        groups=groups,
        attributes={str(key): str(value) for key, value in raw_attributes.items()},
    )


def _digest(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()
