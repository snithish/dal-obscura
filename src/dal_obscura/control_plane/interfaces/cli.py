from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

import uvicorn

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    ensure_config_store_schema,
    session_factory,
)
from dal_obscura.control_plane.interfaces.api import create_app, create_oidc_actor_resolver


def main() -> None:
    database_url = os.environ["DAL_OBSCURA_DATABASE_URL"]
    admin_token = os.environ["DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN"]
    host = os.getenv("DAL_OBSCURA_CONTROL_PLANE_HOST", "0.0.0.0")
    port = int(os.getenv("DAL_OBSCURA_CONTROL_PLANE_PORT", "8820"))
    oidc_issuer = os.getenv("DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER")
    oidc_resolver = None
    if oidc_issuer:
        raw_group_claims = os.getenv(
            "DAL_OBSCURA_CONTROL_PLANE_OIDC_GROUP_CLAIMS",
            "groups",
        )
        oidc_resolver = create_oidc_actor_resolver(
            issuer=oidc_issuer,
            audience=os.getenv("DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE"),
            jwks_url=os.getenv("DAL_OBSCURA_CONTROL_PLANE_OIDC_JWKS_URL"),
            subject_claim=os.getenv(
                "DAL_OBSCURA_CONTROL_PLANE_OIDC_SUBJECT_CLAIM",
                "preferred_username",
            ),
            group_claims=tuple(
                item.strip() for item in raw_group_claims.split(",") if item.strip()
            ),
        )
    engine = create_engine_from_url(database_url)
    ensure_config_store_schema(engine)
    app = create_app(
        session_factory(engine),
        admin_token=admin_token,
        oidc_actor_resolver=oidc_resolver,
        oidc_admin_group=os.getenv("DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP"),
        ui_auth_config=ui_auth_config_from_env(os.environ),
    )
    uvicorn.run(app, host=host, port=port)


def ui_auth_config_from_env(env: Mapping[str, str]) -> dict[str, Any] | None:
    issuer = _env_text(
        env,
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER",
    ) or _env_text(env, "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER")
    client_id = _env_text(env, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID")
    redirect_uri = _env_text(env, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI")
    post_logout_redirect_uri = _env_text(
        env,
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI",
    )
    if not issuer or not client_id or not redirect_uri:
        return None
    config: dict[str, Any] = {
        "authority": issuer,
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": _env_text(env, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_SCOPE") or "openid profile",
    }
    if post_logout_redirect_uri:
        config["post_logout_redirect_uri"] = post_logout_redirect_uri
    login_shortcuts = _login_shortcuts_from_env(env)
    if login_shortcuts:
        config["login_shortcuts"] = login_shortcuts
    demo_login = _demo_login_from_env(env)
    if demo_login:
        config["demo_login"] = demo_login
    return config


def _env_text(env: Mapping[str, str], name: str) -> str:
    return env.get(name, "").strip()


def _login_shortcuts_from_env(env: Mapping[str, str]) -> list[dict[str, str]]:
    raw = _env_text(env, "DAL_OBSCURA_CONTROL_PLANE_UI_LOGIN_SHORTCUTS")
    shortcuts: list[dict[str, str]] = []
    for item in raw.split(";"):
        label, separator, login_hint = item.partition("=")
        if not separator:
            continue
        label = label.strip()
        login_hint = login_hint.strip()
        if label and login_hint:
            shortcuts.append({"label": label, "login_hint": login_hint})
    return shortcuts


def _demo_login_from_env(env: Mapping[str, str]) -> dict[str, object]:
    token_url = _env_text(env, "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_TOKEN_URL")
    client_id = _env_text(env, "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_ID")
    client_secret = _env_text(env, "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_SECRET")
    passwords = _passwords_from_env(env)
    if not token_url or not client_id or not client_secret or not passwords:
        return {}
    return {
        "token_url": token_url,
        "client_id": client_id,
        "client_secret": client_secret,
        "passwords": passwords,
    }


def _passwords_from_env(env: Mapping[str, str]) -> dict[str, str]:
    raw = _env_text(env, "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_PASSWORDS")
    passwords: dict[str, str] = {}
    for item in raw.split(";"):
        username, separator, password = item.partition("=")
        if not separator:
            continue
        username = username.strip()
        password = password.strip()
        if username and password:
            passwords[username] = password
    return passwords
