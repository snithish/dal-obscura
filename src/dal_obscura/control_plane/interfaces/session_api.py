from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import cast
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest
from urllib.request import urlopen

from fastapi import HTTPException

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest
from dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks import (
    OidcJwksIdentityProvider,
)

OidcActorResolver = Callable[[str], object]


def create_oidc_actor_resolver(
    *,
    issuer: str,
    audience: str | None,
    jwks_url: str | None,
    subject_claim: str,
    group_claims: tuple[str, ...],
) -> OidcActorResolver:
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
        return {"principal": principal.id, "groups": principal.groups}

    return resolve


def actor_response(actor: ControlPlaneActor) -> dict[str, object]:
    return {
        "principal": actor.principal,
        "groups": list(actor.groups),
        "platform_admin": actor.platform_admin,
    }


def public_ui_auth_config(config: Mapping[str, object]) -> dict[str, object]:
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
    demo_passwords = demo_login_passwords(config)
    login_shortcuts = public_login_shortcuts(config.get("login_shortcuts"))
    if login_shortcuts:
        public["login_shortcuts"] = [
            {
                **shortcut,
                **(
                    {"demo_login_path": "/v1/demo-login"}
                    if shortcut["login_hint"] in demo_passwords
                    else {}
                ),
            }
            for shortcut in login_shortcuts
        ]
    return public


def demo_login_config(config: Mapping[str, object]) -> dict[str, object]:
    value = config.get("demo_login")
    if not isinstance(value, Mapping):
        return {}
    demo_login = cast(Mapping[str, object], value)
    token_url = str(demo_login.get("token_url", "")).strip()
    client_id = str(demo_login.get("client_id", "")).strip()
    client_secret = str(demo_login.get("client_secret", "")).strip()
    passwords = demo_login_passwords(config)
    if not token_url or not client_id or not client_secret or not passwords:
        return {}
    return {
        "token_url": token_url,
        "client_id": client_id,
        "client_secret": client_secret,
        "passwords": passwords,
    }


def exchange_demo_password_token(config: Mapping[str, object], username: str) -> str:
    passwords = cast(dict[str, str], config["passwords"])
    body = urlencode(
        {
            "grant_type": "password",
            "client_id": str(config["client_id"]),
            "client_secret": str(config["client_secret"]),
            "username": username,
            "password": passwords[username],
        }
    ).encode()
    request = UrlRequest(
        str(config["token_url"]),
        data=body,
        headers={"content-type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    token = str(payload.get("access_token", "")).strip()
    if not token:
        raise HTTPException(status_code=502, detail="Demo identity provider did not return a token")
    return token


def oidc_actor_from_header(
    authorization: str,
    *,
    resolver: OidcActorResolver,
    admin_group: str | None,
) -> ControlPlaneActor | None:
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
    )


def public_login_shortcuts(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    shortcuts: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        shortcut = cast(Mapping[str, object], item)
        label = str(shortcut.get("label", "")).strip()
        login_hint = str(shortcut.get("login_hint", "")).strip()
        if label and login_hint:
            shortcuts.append({"label": label, "login_hint": login_hint})
    return shortcuts


def demo_login_passwords(config: Mapping[str, object]) -> dict[str, str]:
    value = config.get("demo_login")
    if not isinstance(value, Mapping):
        return {}
    passwords = cast(Mapping[str, object], value).get("passwords")
    if not isinstance(passwords, Mapping):
        return {}
    return {
        str(username).strip(): str(password).strip()
        for username, password in cast(Mapping[object, object], passwords).items()
        if str(username).strip() and str(password).strip()
    }


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
