from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.utils import base64url_encode

from dal_obscura.infrastructure.adapters.identity_oidc_jwks import OidcJwksIdentityProvider

ISSUER = "https://keycloak.example.test/realms/acme"
AUDIENCE = "dal-obscura"


def _rsa_key_pair(kid: str):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_numbers = private_key.public_key().public_numbers()
    jwk = {
        "kty": "RSA",
        "use": "sig",
        "kid": kid,
        "alg": "RS256",
        "n": base64url_encode(
            public_numbers.n.to_bytes((public_numbers.n.bit_length() + 7) // 8, "big")
        ).decode("ascii"),
        "e": base64url_encode(
            public_numbers.e.to_bytes((public_numbers.e.bit_length() + 7) // 8, "big")
        ).decode("ascii"),
    }
    return private_key, jwk


def _token(
    private_key,
    *,
    kid: str,
    subject: str | None = "user-123",
    issuer: str = ISSUER,
    audience: str = AUDIENCE,
    expires_delta: timedelta = timedelta(minutes=5),
    extra_claims: dict[str, Any] | None = None,
) -> str:
    now = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "iss": issuer,
        "aud": audience,
        "exp": now + expires_delta,
        "iat": now,
        "nbf": now - timedelta(seconds=1),
    }
    if subject is not None:
        payload["sub"] = subject
    if extra_claims:
        payload.update(extra_claims)
    return jwt.encode(payload, private_key, algorithm="RS256", headers={"kid": kid})


def _provider(jwk: dict[str, Any]) -> OidcJwksIdentityProvider:
    return OidcJwksIdentityProvider(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks_url="https://keycloak.example.test/certs",
        jwks_fetcher=lambda _url: {"keys": [jwk]},
    )


def test_valid_keycloak_like_access_token_authenticates():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)
    token = _token(private_key, kid="kid-1", subject="user-123")

    principal = provider.authenticate({"authorization": f"Bearer {token}"})

    assert principal.id == "user-123"
    assert principal.groups == []
    assert principal.attributes == {}


def test_rejects_missing_bearer_token():
    _private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)

    with pytest.raises(PermissionError, match="Missing token"):
        provider.authenticate({})


def test_rejects_wrong_issuer():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)
    token = _token(private_key, kid="kid-1", issuer="https://wrong.example.test/realm")

    with pytest.raises(PermissionError, match="Invalid token"):
        provider.authenticate({"authorization": f"Bearer {token}"})


def test_rejects_wrong_audience():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)
    token = _token(private_key, kid="kid-1", audience="wrong-audience")

    with pytest.raises(PermissionError, match="Invalid token"):
        provider.authenticate({"authorization": f"Bearer {token}"})


def test_rejects_expired_token():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)
    token = _token(private_key, kid="kid-1", expires_delta=timedelta(seconds=-1))

    with pytest.raises(PermissionError, match="Invalid token"):
        provider.authenticate({"authorization": f"Bearer {token}"})


def test_rejects_missing_subject_claim():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)
    token = _token(private_key, kid="kid-1", subject=None)

    with pytest.raises(PermissionError, match="Missing subject"):
        provider.authenticate({"authorization": f"Bearer {token}"})
