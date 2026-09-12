"""Control-plane HTTP server command.

Example:
    ```bash
    DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:///control-plane.db \
    DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN=local-admin \
    dal-obscura-control-plane
    ```
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence

import uvicorn

from dal_obscura.common.config_store.db import (
    ConfigStoreSchemaError,
    check_config_store_schema,
    create_engine_from_url,
    session_factory,
)
from dal_obscura.control_plane.interfaces.api import (
    _create_oidc_nonce_actor_resolver,
    create_app,
    create_oidc_actor_resolver,
)


def main() -> None:
    """Runs the configured control-plane HTTP server."""

    raise SystemExit(run(argv=sys.argv[1:]))


def run(environment: Mapping[str, str] | None = None, argv: Sequence[str] | None = None) -> int:
    """Starts the control plane from environment configuration.

    The command intentionally does not migrate the database. Operators run the
    explicit migration command before starting a service process.
    """

    _parser().parse_args(argv or ())
    values = os.environ if environment is None else environment
    database_url = _required(values, "DAL_OBSCURA_DATABASE_URL")
    admin_token = _required(values, "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN")
    if database_url is None or admin_token is None:
        return 2
    try:
        _validate_profile(values, admin_token, database_url)
        port = _port(values.get("DAL_OBSCURA_CONTROL_PLANE_PORT", "8820"))
        engine = create_engine_from_url(database_url)
        check_config_store_schema(engine)
        oidc_resolver = _oidc_resolver(values)
        app = create_app(
            session_factory(engine),
            admin_token=admin_token,
            oidc_actor_resolver=oidc_resolver,
            oidc_admin_group=_optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP"),
            cors_origins=_csv(values.get("DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS", "")),
            ui_auth_config=_ui_auth_config(values),
            session_ttl_seconds=_positive_int(
                values.get("DAL_OBSCURA_CONTROL_PLANE_SESSION_TTL_SECONDS", "28800"),
                "DAL_OBSCURA_CONTROL_PLANE_SESSION_TTL_SECONDS",
            ),
            session_idle_ttl_seconds=_positive_int(
                values.get("DAL_OBSCURA_CONTROL_PLANE_SESSION_IDLE_TTL_SECONDS", "1800"),
                "DAL_OBSCURA_CONTROL_PLANE_SESSION_IDLE_TTL_SECONDS",
            ),
            oidc_nonce_actor_resolver=_ui_nonce_resolver(values),
            require_review=values.get("DAL_OBSCURA_CONTROL_PLANE_PROFILE", "local").strip().lower()
            == "production",
            catalog_egress_allowlist=_csv(
                values.get("DAL_OBSCURA_CONTROL_PLANE_CATALOG_EGRESS_ALLOWLIST", "")
            ),
        )
    except (ConfigStoreSchemaError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    uvicorn.run(
        app,
        host=values.get("DAL_OBSCURA_CONTROL_PLANE_HOST", "127.0.0.1"),
        port=port,
        log_level=values.get("DAL_OBSCURA_CONTROL_PLANE_LOG_LEVEL", "info").lower(),
    )
    return 0


def _required(values: Mapping[str, str], name: str) -> str | None:
    value = _optional(values, name)
    if value is None:
        print(f"{name} is required", file=sys.stderr)
    return value


def _optional(values: Mapping[str, str], name: str) -> str | None:
    value = values.get(name, "").strip()
    return value or None


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise ValueError("DAL_OBSCURA_CONTROL_PLANE_PORT must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ValueError("DAL_OBSCURA_CONTROL_PLANE_PORT must be between 1 and 65535")
    return port


def _positive_int(value: str, name: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if number <= 0:
        raise ValueError(f"{name} must be positive")
    return number


def _csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _validate_profile(
    values: Mapping[str, str],
    admin_token: str,
    database_url: str | None = None,
) -> None:
    profile = values.get("DAL_OBSCURA_CONTROL_PLANE_PROFILE", "local").strip().lower()
    if profile not in {"local", "production"}:
        raise ValueError("DAL_OBSCURA_CONTROL_PLANE_PROFILE must be local or production")
    if profile != "production":
        return
    if len(admin_token) < 32:
        raise ValueError(
            "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN must contain at least 32 "
            "characters in production"
        )
    if database_url is not None and not database_url.lower().startswith("postgresql"):
        raise ValueError("Production requires a PostgreSQL control-plane database")
    oidc_issuer = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER")
    oidc_audience = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE")
    ui_issuer = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER")
    ui_client = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID")
    redirect_uri = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI")
    if not oidc_issuer or not oidc_issuer.startswith("https://") or not oidc_audience:
        raise ValueError("Production requires an HTTPS bearer OIDC issuer and audience")
    if not ui_issuer or not ui_issuer.startswith("https://") or not ui_client:
        raise ValueError("Production requires an HTTPS browser OIDC issuer and client ID")
    if not redirect_uri or not redirect_uri.startswith("https://"):
        raise ValueError("Production requires an HTTPS browser redirect URI")
    origins = _csv(values.get("DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS", ""))
    if not origins or any(not origin.startswith("https://") for origin in origins):
        raise ValueError("Production requires at least one HTTPS CORS origin")
    if not _csv(values.get("DAL_OBSCURA_CONTROL_PLANE_CATALOG_EGRESS_ALLOWLIST", "")):
        raise ValueError("Production requires an explicit catalog egress allowlist")
    if any(
        _optional(values, name)
        for name in (
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_TOKEN_URL",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_ID",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_SECRET",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_PASSWORDS",
            "DAL_OBSCURA_CONTROL_PLANE_UI_LOGIN_SHORTCUTS",
        )
    ):
        raise ValueError("Demo login shortcuts are forbidden in production")


def _oidc_resolver(values: Mapping[str, str]):
    issuer = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER")
    if issuer is None:
        return None
    return create_oidc_actor_resolver(
        issuer=issuer,
        audience=_optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE"),
        jwks_url=_optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_JWKS_URL"),
        subject_claim=values.get("DAL_OBSCURA_CONTROL_PLANE_OIDC_SUBJECT_CLAIM", "sub"),
        group_claims=_csv(values.get("DAL_OBSCURA_CONTROL_PLANE_OIDC_GROUP_CLAIMS", "groups")),
    )


def _ui_auth_config(values: Mapping[str, str]) -> dict[str, object] | None:
    issuer = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER")
    client_id = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID")
    if issuer is None and client_id is None:
        return None
    if issuer is None or client_id is None:
        raise ValueError("UI OIDC issuer and client ID must be configured together")
    config: dict[str, object] = {
        "authority": issuer,
        "client_id": client_id,
        "redirect_uri": _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI"),
        "post_logout_redirect_uri": _optional(
            values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI"
        ),
        "post_login_redirect_uri": _optional(
            values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGIN_REDIRECT_URI"
        ),
        "scope": values.get("DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_SCOPE", "openid profile"),
        "authorization_endpoint": _optional(
            values,
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_AUTHORIZATION_ENDPOINT",
        ),
        "token_endpoint": _optional(
            values,
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_TOKEN_ENDPOINT",
        ),
        "login_shortcuts": _login_shortcuts(
            values.get("DAL_OBSCURA_CONTROL_PLANE_UI_LOGIN_SHORTCUTS", "")
        ),
    }
    demo_login = _demo_login_config(values)
    if demo_login:
        config["demo_login"] = demo_login
    return {key: value for key, value in config.items() if value is not None}


def _ui_nonce_resolver(values: Mapping[str, str]):
    issuer = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER")
    client_id = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID")
    if issuer is None or client_id is None:
        return None
    jwks_url = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_JWKS_URL") or _optional(
        values,
        "DAL_OBSCURA_CONTROL_PLANE_OIDC_JWKS_URL",
    )
    subject_claim = values.get(
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_SUBJECT_CLAIM",
        values.get("DAL_OBSCURA_CONTROL_PLANE_OIDC_SUBJECT_CLAIM", "sub"),
    )
    group_claims = _csv(
        values.get(
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_GROUP_CLAIMS",
            values.get("DAL_OBSCURA_CONTROL_PLANE_OIDC_GROUP_CLAIMS", "groups"),
        )
    )
    return _create_oidc_nonce_actor_resolver(
        issuer=issuer,
        audience=client_id,
        jwks_url=jwks_url,
        subject_claim=subject_claim,
        group_claims=group_claims,
    )


def _login_shortcuts(value: str) -> list[dict[str, str]]:
    shortcuts = []
    for entry in value.split(";"):
        if not entry.strip():
            continue
        label, separator, login_hint = entry.partition("=")
        if not separator or not label.strip() or not login_hint.strip():
            raise ValueError("UI login shortcuts must use 'label=login_hint' entries")
        shortcuts.append({"label": label.strip(), "login_hint": login_hint.strip()})
    return shortcuts


def _demo_login_config(values: Mapping[str, str]) -> dict[str, object]:
    token_url = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_TOKEN_URL")
    client_id = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_ID")
    client_secret = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_SECRET")
    passwords = _key_values(values.get("DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_PASSWORDS", ""))
    configured = (token_url, client_id, client_secret, passwords)
    if not any(configured):
        return {}
    if not all(configured):
        raise ValueError("Demo login requires token URL, client ID, secret, and passwords")
    return {
        "token_url": token_url,
        "client_id": client_id,
        "client_secret": client_secret,
        "passwords": passwords,
    }


def _key_values(value: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for entry in value.split(";"):
        if not entry.strip():
            continue
        key, separator, item = entry.partition("=")
        if not separator or not key.strip() or not item.strip():
            raise ValueError("Expected semicolon-separated key=value entries")
        if key.strip() in pairs:
            raise ValueError(f"Duplicate configured key {key.strip()!r}")
        pairs[key.strip()] = item.strip()
    return pairs


def _parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="dal-obscura-control-plane",
        description="Start the dal-obscura control-plane HTTP server.",
    )
