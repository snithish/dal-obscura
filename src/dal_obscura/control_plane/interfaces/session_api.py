"""Session and UI-auth helpers for the control-plane API.

Example:
    ```python
    actor = oidc_actor_from_header("Bearer token", resolver=resolver, admin_group="admins")
    ```
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import cast
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, build_opener
from urllib.request import Request as UrlRequest

from fastapi import HTTPException

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest
from dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks import (
    OidcJwksIdentityProvider,
)

OidcActorResolver = Callable[[str], object]
OidcNonceActorResolver = Callable[[str, str], object]


def create_oidc_actor_resolver(
    *,
    issuer: str,
    audience: str | None,
    jwks_url: str | None,
    subject_claim: str,
    group_claims: tuple[str, ...],
) -> OidcActorResolver:
    """Builds an OIDC bearer-token resolver for control-plane actors.

    Example:
        ```python
        resolver = create_oidc_actor_resolver(
            issuer="https://idp.example.com",
            audience="dal-obscura-ui",
            jwks_url=None,
            subject_claim="preferred_username",
            group_claims=("groups",),
        )
        ```
    """

    provider = OidcJwksIdentityProvider(
        issuer=issuer,
        audience=audience or None,
        jwks_url=jwks_url or None,
        subject_claim=subject_claim,
        group_claims=group_claims,
    )

    def resolve(token: str) -> dict[str, object]:
        principal = provider.authenticate(
            AuthenticationRequest(headers={"authorization": f"Bearer {token}"})
        )
        payload: dict[str, object] = {"principal": principal.id, "groups": principal.groups}
        if principal.issuer:
            payload["issuer"] = principal.issuer
        return payload

    return resolve


def create_oidc_nonce_actor_resolver(
    *,
    issuer: str,
    audience: str | None,
    jwks_url: str | None,
    subject_claim: str,
    group_claims: tuple[str, ...],
) -> OidcNonceActorResolver:
    """Builds a resolver that validates an ID-token nonce before mapping claims."""

    provider = OidcJwksIdentityProvider(
        issuer=issuer,
        audience=audience or None,
        jwks_url=jwks_url or None,
        subject_claim=subject_claim,
        group_claims=group_claims,
    )

    def resolve(token: str, nonce_hash: str) -> dict[str, object]:
        principal = provider.authenticate(
            AuthenticationRequest(headers={"authorization": f"Bearer {token}"}),
            expected_nonce_hash=nonce_hash,
        )
        payload: dict[str, object] = {"principal": principal.id, "groups": principal.groups}
        if principal.issuer:
            payload["issuer"] = principal.issuer
        return payload

    return resolve


def actor_response(actor: ControlPlaneActor) -> dict[str, object]:
    """Serializes a control-plane actor for the session API.

    Example:
        ```python
        payload = actor_response(actor)
        ```
    """

    return {
        "principal": actor.principal,
        "groups": list(actor.groups),
        "platform_admin": actor.platform_admin,
        "capabilities": ["workspace:admin"] if actor.platform_admin else [],
        **({"issuer": actor.issuer} if actor.issuer else {}),
    }


def public_ui_auth_config(config: Mapping[str, object]) -> dict[str, object]:
    """Returns only browser-safe UI authentication settings.

    Example:
        ```python
        public = public_ui_auth_config(raw_config)
        ```
    """

    public_keys = (
        "authority",
        "client_id",
        "redirect_uri",
        "post_logout_redirect_uri",
        "scope",
    )
    public: dict[str, object] = {
        key: value for key in public_keys if (value := str(config.get(key, "")).strip())
    }
    return public


def exchange_authorization_code(
    config: Mapping[str, object],
    code: str,
    code_verifier: str,
) -> Mapping[str, object]:
    """Exchanges an authorization code using the public OIDC UI client."""

    body = urlencode(
        {
            "grant_type": "authorization_code",
            "client_id": str(config["client_id"]),
            "code": code,
            "code_verifier": code_verifier,
            "redirect_uri": str(config["redirect_uri"]),
        }
    ).encode()
    request = UrlRequest(
        _token_endpoint(config),
        data=body,
        headers={"content-type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with _open_token_endpoint(request) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=502, detail="OIDC code exchange failed") from exc
    if not isinstance(payload, Mapping):
        raise HTTPException(status_code=502, detail="OIDC token response was invalid")
    if not str(payload.get("access_token", "")).strip():
        raise HTTPException(status_code=502, detail="OIDC token response omitted access token")
    if not str(payload.get("id_token", "")).strip():
        raise HTTPException(status_code=502, detail="OIDC token response omitted ID token")
    return cast(Mapping[str, object], payload)


def _token_endpoint(config: Mapping[str, object]) -> str:
    """Resolve and validate the configured OIDC token endpoint."""

    configured = str(config.get("token_endpoint", "")).strip()
    if not configured:
        authority = str(config.get("authority", "")).strip().rstrip("/")
        configured = f"{authority}/protocol/openid-connect/token" if authority else ""
    try:
        parsed = urlsplit(configured)
        hostname = parsed.hostname
        _ = parsed.port
    except ValueError as exc:
        raise HTTPException(status_code=503, detail="OIDC token endpoint is invalid") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise HTTPException(status_code=503, detail="OIDC token endpoint is invalid")
    return configured


class _RejectRedirects(HTTPRedirectHandler):
    """Keep an operator-configured token endpoint on its exact origin."""

    def redirect_request(self, request, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _open_token_endpoint(request: UrlRequest):
    """Open a token request without following a cross-origin redirect."""

    return build_opener(_RejectRedirects()).open(request, timeout=10)


def oidc_actor_from_header(
    authorization: str,
    *,
    resolver: OidcActorResolver,
    admin_group: str | None,
) -> ControlPlaneActor | None:
    """Converts an authorization header into a control-plane actor.

    Example:
        ```python
        actor = oidc_actor_from_header(
            "Bearer token",
            resolver=resolver,
            admin_group="platform-admins",
        )
        ```
    """

    token = _bearer_token(authorization)
    if token is None:
        return None
    try:
        resolved = resolver(token)
    except Exception:
        return None
    principal = _attribute_text(resolved, "principal")
    if not principal:
        return None
    groups = tuple(_attribute_list(resolved, "groups"))
    return ControlPlaneActor(
        principal=principal,
        groups=groups,
        platform_admin=bool(admin_group and admin_group in groups),
        issuer=_attribute_text(resolved, "issuer"),
    )


def _bearer_token(authorization: str) -> str | None:
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


def _attribute_text(value: object, name: str) -> str:
    raw = (
        cast(Mapping[str, object], value).get(name)
        if isinstance(value, Mapping)
        else getattr(value, name, None)
    )
    if raw is None or isinstance(raw, list | tuple | dict):
        return ""
    return str(raw).strip()


def _attribute_list(value: object, name: str) -> list[str]:
    raw = (
        cast(Mapping[str, object], value).get(name, ())
        if isinstance(value, Mapping)
        else getattr(value, name, ())
    )
    if isinstance(raw, str):
        return [raw] if raw else []
    if not isinstance(raw, list | tuple | set):
        return []
    return [str(item).strip() for item in raw if str(item).strip()]
