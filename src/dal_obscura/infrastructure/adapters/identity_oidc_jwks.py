from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.request import urlopen

import jwt

from dal_obscura.domain.access_control.models import Principal

JsonObject = Mapping[str, Any]
JsonFetcher = Callable[[str], JsonObject]


@dataclass(frozen=True)
class OidcJwksConfig:
    issuer: str
    audience: str | Sequence[str] | None
    jwks_url: str
    algorithms: tuple[str, ...]
    subject_claim: str
    leeway_seconds: int


class OidcJwksIdentityProvider:
    """Authenticates bearer JWTs with OIDC issuer/audience checks and JWKS keys."""

    def __init__(
        self,
        *,
        issuer: str,
        audience: str | Sequence[str] | None = None,
        jwks_url: str | None = None,
        algorithms: Sequence[str] | None = None,
        subject_claim: str = "sub",
        leeway_seconds: int = 0,
        jwks_fetcher: JsonFetcher | None = None,
    ) -> None:
        normalized_issuer = issuer.rstrip("/")
        resolved_jwks_url = jwks_url or _discover_jwks_url(normalized_issuer, jwks_fetcher)
        self._config = OidcJwksConfig(
            issuer=normalized_issuer,
            audience=audience,
            jwks_url=resolved_jwks_url,
            algorithms=tuple(algorithms or ("RS256", "RS384", "RS512", "ES256", "ES384", "ES512")),
            subject_claim=subject_claim,
            leeway_seconds=leeway_seconds,
        )
        self._jwks = _JwksCache(resolved_jwks_url, jwks_fetcher or _fetch_json)

    def authenticate(self, headers: Mapping[str, str]) -> Principal:
        token = _parse_bearer(headers.get("authorization"))
        if not token:
            raise PermissionError("Missing token")
        payload = self._decode(token)
        subject = str(payload.get(self._config.subject_claim) or "").strip()
        if not subject:
            raise PermissionError("Missing subject")
        return Principal(id=subject, groups=[], attributes={})

    def _decode(self, token: str) -> JsonObject:
        try:
            key = self._jwks.key_for_token(token)
            payload = jwt.decode(
                token,
                key,
                algorithms=list(self._config.algorithms),
                audience=self._config.audience,
                issuer=self._config.issuer,
                leeway=self._config.leeway_seconds,
                options={"verify_aud": self._config.audience is not None},
            )
        except jwt.PyJWTError as exc:
            raise PermissionError("Invalid token") from exc
        if not isinstance(payload, Mapping):
            raise PermissionError("Invalid token")
        return payload


class _JwksCache:
    def __init__(self, jwks_url: str, fetcher: JsonFetcher) -> None:
        self._jwks_url = jwks_url
        self._fetcher = fetcher
        self._keys_by_kid: dict[str, Any] = {}

    def key_for_token(self, token: str) -> Any:
        header = jwt.get_unverified_header(token)
        kid = str(header.get("kid") or "")
        if not kid:
            raise jwt.InvalidTokenError("JWT header is missing kid")
        if kid not in self._keys_by_kid:
            self._refresh()
        key = self._keys_by_kid.get(kid)
        if key is None:
            raise jwt.InvalidTokenError(f"Unknown signing key id {kid!r}")
        return key

    def _refresh(self) -> None:
        jwks = self._fetcher(self._jwks_url)
        keys = jwks.get("keys")
        if not isinstance(keys, list):
            raise jwt.InvalidTokenError("JWKS response does not contain keys")
        refreshed: dict[str, Any] = {}
        for item in keys:
            if not isinstance(item, Mapping):
                continue
            kid = str(item.get("kid") or "")
            if not kid:
                continue
            refreshed[kid] = jwt.PyJWK.from_dict(dict(item)).key
        self._keys_by_kid = refreshed


def _parse_bearer(header: str | None) -> str | None:
    if not header:
        return None
    parts = header.split(" ", 1)
    if len(parts) != 2:
        return None
    if parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


def _discover_jwks_url(issuer: str, fetcher: JsonFetcher | None) -> str:
    loader = fetcher or _fetch_json
    metadata = loader(f"{issuer}/.well-known/openid-configuration")
    jwks_url = metadata.get("jwks_uri")
    if not isinstance(jwks_url, str) or not jwks_url.strip():
        raise ValueError("OIDC discovery response did not include jwks_uri")
    return jwks_url.strip()


def _fetch_json(url: str) -> JsonObject:
    with urlopen(url, timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"Expected JSON object from {url!r}")
    return payload
