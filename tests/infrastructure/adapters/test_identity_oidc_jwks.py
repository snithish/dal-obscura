from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.request import Request

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.utils import base64url_encode

import dal_obscura.identity.oidc as jwks_module
from dal_obscura.identity.contracts import (
    AuthenticationRequest,
    MissingCredentialsError,
)
from dal_obscura.identity.oidc import (
    OidcJwksIdentityProvider,
)

ISSUER = "https://keycloak.example.test/realms/acme"
AUDIENCE = "dal-obscura"


def test_verified_jwt_attributes_enforce_configured_domain():
    private_key, jwk = _rsa_key_pair("attributes-key")
    provider = OidcJwksIdentityProvider(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks={"keys": [jwk]},
        attribute_claims={"department": "employee.department"},
        attribute_definitions={"department": {"allowed_values": ["Finance"]}},
    )
    token = _token(
        private_key, kid="attributes-key", extra_claims={"employee": {"department": "Finance"}}
    )
    assert provider.authenticate(
        AuthenticationRequest(headers={"authorization": f"Bearer {token}"})
    ).attributes == {"department": "Finance"}
    invalid = _token(
        private_key, kid="attributes-key", extra_claims={"employee": {"department": "Engineering"}}
    )
    with pytest.raises(PermissionError, match="outside its allowed values"):
        provider.authenticate(AuthenticationRequest(headers={"authorization": f"Bearer {invalid}"}))


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


def _auth_request(token: str | None = None) -> AuthenticationRequest:
    headers = {} if token is None else {"authorization": f"Bearer {token}"}
    return AuthenticationRequest(headers=headers)


def test_valid_keycloak_like_access_token_authenticates():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)
    token = _token(private_key, kid="kid-1", subject="user-123")

    principal = provider.authenticate(_auth_request(token))

    assert principal.id == "user-123"
    assert principal.issuer == ISSUER
    assert principal.expires_at is not None
    assert principal.groups == ()
    assert principal.attributes == {}


def test_preserves_trailing_slash_in_exact_issuer_comparison():
    private_key, jwk = _rsa_key_pair("kid-1")
    issuer = f"{ISSUER}/"
    provider = OidcJwksIdentityProvider(
        issuer=issuer,
        audience=AUDIENCE,
        jwks_url="https://keycloak.example.test/certs",
        jwks_fetcher=lambda _url: {"keys": [jwk]},
    )

    principal = provider.authenticate(
        _auth_request(_token(private_key, kid="kid-1", issuer=issuer))
    )

    assert principal.issuer == issuer


def test_oidc_discovery_does_not_double_append_issuer_slash():
    requested: list[str] = []

    def fetcher(url: str):
        requested.append(url)
        return {"jwks_uri": "https://keycloak.example.test/certs"}

    OidcJwksIdentityProvider(issuer=f"{ISSUER}/", jwks_fetcher=fetcher)

    assert requested == [f"{ISSUER}/.well-known/openid-configuration"]


def test_jwks_transport_does_not_follow_redirects(monkeypatch):
    seen: dict[str, Any] = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"keys": []}'

    class Opener:
        def open(self, request, *, timeout):
            seen["request"] = request
            seen["timeout"] = timeout
            return Response()

    def build(handler):
        seen["handler"] = handler
        return Opener()

    monkeypatch.setattr(jwks_module, "build_opener", build)
    assert jwks_module._fetch_json("http://127.0.0.1:9999/jwks") == {"keys": []}
    assert isinstance(seen["request"], Request)
    assert seen["request"].full_url == "http://127.0.0.1:9999/jwks"
    assert seen["timeout"] == 5
    assert isinstance(seen["handler"], jwks_module._NoRedirectHandler)
    assert (
        seen["handler"].redirect_request(None, None, 302, "Found", {}, "https://attacker.example")
        is None
    )


def test_ignores_non_signing_jwks_entries():
    private_key, jwk = _rsa_key_pair("signing-kid")
    encryption_jwk = {**jwk, "kid": "encryption-kid", "use": "enc", "alg": "RSA-OAEP"}
    provider = OidcJwksIdentityProvider(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks_url="https://keycloak.example.test/certs",
        jwks_fetcher=lambda _url: {"keys": [encryption_jwk, jwk]},
    )
    token = _token(private_key, kid="signing-kid", subject="user-123")

    principal = provider.authenticate(_auth_request(token))

    assert principal.id == "user-123"


def test_rejects_missing_bearer_token():
    _private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)

    with pytest.raises(MissingCredentialsError, match="Missing token"):
        provider.authenticate(AuthenticationRequest())


def test_rejects_wrong_issuer():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)
    token = _token(private_key, kid="kid-1", issuer="https://wrong.example.test/realm")

    with pytest.raises(PermissionError, match="Invalid token"):
        provider.authenticate(_auth_request(token))


def test_rejects_wrong_audience():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)
    token = _token(private_key, kid="kid-1", audience="wrong-audience")

    with pytest.raises(PermissionError, match="Invalid token"):
        provider.authenticate(_auth_request(token))


def test_rejects_expired_token():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)
    token = _token(private_key, kid="kid-1", expires_delta=timedelta(seconds=-1))

    with pytest.raises(PermissionError, match="Invalid token"):
        provider.authenticate(_auth_request(token))


def test_rejects_missing_subject_claim():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = _provider(jwk)
    token = _token(private_key, kid="kid-1", subject=None)

    with pytest.raises(PermissionError, match="Missing subject"):
        provider.authenticate(_auth_request(token))


def test_extracts_groups_roles_and_scalar_attributes_from_configured_claims():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = OidcJwksIdentityProvider(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks_url="https://keycloak.example.test/certs",
        group_claims=["groups", "realm_access.roles", "resource_access.dal-obscura.roles"],
        attribute_claims={"department": "department", "clearance": "custom.clearance"},
        jwks_fetcher=lambda _url: {"keys": [jwk]},
    )
    token = _token(
        private_key,
        kid="kid-1",
        extra_claims={
            "groups": ["/analytics", "finance"],
            "realm_access": {"roles": ["analyst"]},
            "resource_access": {"dal-obscura": {"roles": ["reader"]}},
            "department": "acme",
            "custom": {"clearance": "high"},
        },
    )

    principal = provider.authenticate(_auth_request(token))

    assert principal.groups == ("/analytics", "finance", "analyst", "reader")
    assert principal.attributes == {"department": "acme", "clearance": "high"}


def test_ignores_missing_optional_group_and_attribute_claims():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = OidcJwksIdentityProvider(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks_url="https://keycloak.example.test/certs",
        group_claims=["groups", "realm_access.roles"],
        attribute_claims={"department": "department"},
        jwks_fetcher=lambda _url: {"keys": [jwk]},
    )
    token = _token(private_key, kid="kid-1")

    principal = provider.authenticate(_auth_request(token))

    assert principal.groups == ()
    assert principal.attributes == {}


def test_rejects_non_scalar_attribute_claim_values():
    private_key, jwk = _rsa_key_pair("kid-1")
    provider = OidcJwksIdentityProvider(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks_url="https://keycloak.example.test/certs",
        attribute_claims={"department": "department"},
        jwks_fetcher=lambda _url: {"keys": [jwk]},
    )
    token = _token(private_key, kid="kid-1", extra_claims={"department": ["acme"]})

    with pytest.raises(PermissionError, match="Invalid attribute claim"):
        provider.authenticate(_auth_request(token))


def test_refreshes_jwks_once_when_token_references_new_kid():
    old_private_key, old_jwk = _rsa_key_pair("old-kid")
    new_private_key, new_jwk = _rsa_key_pair("new-kid")
    responses = [{"keys": [old_jwk]}, {"keys": [old_jwk, new_jwk]}]

    def fetcher(_url: str):
        return responses.pop(0)

    now = [0.0]
    provider = OidcJwksIdentityProvider(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks_url="https://keycloak.example.test/certs",
        jwks_fetcher=fetcher,
        clock=lambda: now[0],
    )
    old_token = _token(old_private_key, kid="old-kid", subject="old-user")
    new_token = _token(new_private_key, kid="new-kid", subject="new-user")

    assert provider.authenticate(_auth_request(old_token)).id == "old-user"
    now[0] = 31.0
    assert provider.authenticate(_auth_request(new_token)).id == "new-user"
    assert responses == []


def test_oidc_provider_accepts_inline_jwks_without_network_fetch():
    private_key, jwk = _rsa_key_pair("kid-inline")
    provider = OidcJwksIdentityProvider(issuer=ISSUER, audience=AUDIENCE, jwks={"keys": [jwk]})
    token = _token(private_key, kid="kid-inline", subject="inline-user")

    principal = provider.authenticate(_auth_request(token))

    assert principal.id == "inline-user"


def test_oidc_provider_accepts_jwks_file_without_discovery(tmp_path):
    private_key, jwk = _rsa_key_pair("kid-file")
    jwks_path = tmp_path / "jwks.json"
    jwks_path.write_text(json.dumps({"keys": [jwk]}))
    provider = OidcJwksIdentityProvider(issuer=ISSUER, audience=AUDIENCE, jwks_file=str(jwks_path))
    token = _token(private_key, kid="kid-file", subject="file-user")

    principal = provider.authenticate(_auth_request(token))

    assert principal.id == "file-user"


def test_unknown_kids_are_refresh_rate_limited():
    known_private_key, known_jwk = _rsa_key_pair("known-kid")
    unknown_private_key, _unknown_jwk = _rsa_key_pair("unknown-kid")
    another_private_key, _another_jwk = _rsa_key_pair("another-unknown-kid")
    calls = 0

    def fetcher(_url: str):
        nonlocal calls
        calls += 1
        return {"keys": [known_jwk]}

    provider = OidcJwksIdentityProvider(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks_url="https://keycloak.example.test/certs",
        jwks_fetcher=fetcher,
    )
    known_token = _token(known_private_key, kid="known-kid")
    unknown_token = _token(unknown_private_key, kid="unknown-kid")

    assert provider.authenticate(_auth_request(known_token)).id == "user-123"
    with pytest.raises(PermissionError, match="Invalid token"):
        provider.authenticate(_auth_request(unknown_token))
    with pytest.raises(PermissionError, match="Invalid token"):
        provider.authenticate(_auth_request(_token(another_private_key, kid="another-unknown-kid")))

    assert calls == 1


def test_rejects_jwks_response_above_configured_key_limit():
    private_key, jwk = _rsa_key_pair("kid-1")
    _second_private_key, second_jwk = _rsa_key_pair("kid-2")
    provider = OidcJwksIdentityProvider(
        issuer=ISSUER,
        audience=AUDIENCE,
        jwks_url="https://keycloak.example.test/certs",
        jwks_fetcher=lambda _url: {"keys": [jwk, second_jwk]},
        max_jwks_keys=1,
    )

    with pytest.raises(PermissionError, match="Invalid token"):
        provider.authenticate(_auth_request(_token(private_key, kid="kid-1")))
