from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import jwt

from dal_obscura.application.ports.identity import (
    AuthenticationRequest,
    InvalidCredentialsError,
    MissingCredentialsError,
)
from dal_obscura.domain.access_control.models import Principal
from dal_obscura.infrastructure.adapters.identity_claims import PrincipalClaimMapper

JsonObject = Mapping[str, Any]
JsonFetcher = Callable[[str], JsonObject]


@dataclass(frozen=True)
class OidcJwksConfig:
    issuer: str
    audience: str | Sequence[str] | None
    jwks_url: str
    algorithms: tuple[str, ...]
    subject_claim: str
    group_claims: tuple[str, ...]
    attribute_claims: Mapping[str, str]
    leeway_seconds: int


class OidcJwksIdentityProvider:
    """Authenticates bearer JWTs with OIDC issuer/audience checks and JWKS keys."""

    def __init__(
        self,
        *,
        issuer: str,
        audience: str | Sequence[str] | None = None,
        jwks_url: str | None = None,
        jwks: JsonObject | None = None,
        jwks_file: str | None = None,
        algorithms: Sequence[str] | None = None,
        subject_claim: str = "sub",
        group_claims: Sequence[str] | None = None,
        attribute_claims: Mapping[str, str] | None = None,
        leeway_seconds: int = 0,
        jwks_fetcher: JsonFetcher | None = None,
    ) -> None:
        normalized_issuer = issuer.rstrip("/")
        static_jwks = _load_static_jwks(jwks, jwks_file)
        if static_jwks is not None:
            resolved_jwks_url = jwks_url or ""
        else:
            resolved_jwks_url = jwks_url or _discover_jwks_url(normalized_issuer, jwks_fetcher)
        self._config = OidcJwksConfig(
            issuer=normalized_issuer,
            audience=audience,
            jwks_url=resolved_jwks_url,
            algorithms=tuple(algorithms or ("RS256", "RS384", "RS512", "ES256", "ES384", "ES512")),
            subject_claim=subject_claim,
            group_claims=tuple(group_claims or ()),
            attribute_claims=dict(attribute_claims or {}),
            leeway_seconds=leeway_seconds,
        )
        self._jwks = _JwksCache(
            resolved_jwks_url,
            jwks_fetcher or _fetch_json,
            static_jwks=static_jwks,
        )
        self._mapper = PrincipalClaimMapper(
            subject_claim=subject_claim,
            group_claims=group_claims,
            attribute_claims=attribute_claims,
        )

    def authenticate(self, request: AuthenticationRequest) -> Principal:
        token = _parse_bearer(request.header("authorization"))
        if not token:
            raise MissingCredentialsError("Missing token")
        payload = self._decode(token)
        return self._mapper.map_claims(payload)

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
            raise InvalidCredentialsError("Invalid token") from exc
        if not isinstance(payload, Mapping):
            raise InvalidCredentialsError("Invalid token")
        return payload


class _JwksCache:
    def __init__(
        self,
        jwks_url: str,
        fetcher: JsonFetcher,
        *,
        static_jwks: JsonObject | None = None,
    ) -> None:
        self._jwks_url = jwks_url
        self._fetcher = fetcher
        self._static_jwks = static_jwks
        self._keys_by_kid: dict[str, Any] = {}
        if static_jwks is not None:
            self._refresh_from_jwks(static_jwks)

    def key_for_token(self, token: str) -> Any:
        header = jwt.get_unverified_header(token)
        kid = str(header.get("kid") or "")
        if not kid:
            raise jwt.InvalidTokenError("JWT header is missing kid")
        if kid not in self._keys_by_kid and self._static_jwks is None:
            self._refresh()
        key = self._keys_by_kid.get(kid)
        if key is None and self._static_jwks is None:
            self._refresh()
            key = self._keys_by_kid.get(kid)
        if key is None:
            raise jwt.InvalidTokenError(f"Unknown signing key id {kid!r}")
        return key

    def _refresh(self) -> None:
        if not self._jwks_url:
            raise jwt.InvalidTokenError("JWKS URL is not configured")
        jwks = self._fetcher(self._jwks_url)
        self._refresh_from_jwks(jwks)

    def _refresh_from_jwks(self, jwks: JsonObject) -> None:
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
            if item.get("use") not in (None, "sig"):
                continue
            try:
                refreshed[kid] = jwt.PyJWK.from_dict(dict(item)).key
            except jwt.PyJWTError:
                continue
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


def _load_static_jwks(jwks: JsonObject | None, jwks_file: str | None) -> JsonObject | None:
    if jwks is not None and jwks_file is not None:
        raise ValueError("Pass either jwks or jwks_file, not both")
    if jwks is not None:
        return jwks
    if jwks_file is None:
        return None
    payload = json.loads(Path(jwks_file).read_text())
    if not isinstance(payload, Mapping):
        raise ValueError("JWKS file must contain a JSON object")
    return payload


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
