"""Control-plane environment parsing and deployment profile validation."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import SplitResult, urlsplit


def optional(values: Mapping[str, str], name: str) -> str | None:
    value = values.get(name, "").strip()
    return value or None


def port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise ValueError("DAL_OBSCURA_CONTROL_PLANE_PORT must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ValueError("DAL_OBSCURA_CONTROL_PLANE_PORT must be between 1 and 65535")
    return port


def positive_int(value: str, name: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if number <= 0:
        raise ValueError(f"{name} must be positive")
    return number


def csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _is_https_url(value: str | None) -> bool:
    if not value:
        return False
    parsed = _safe_urlsplit(value)
    if parsed is None:
        return False
    try:
        hostname = parsed.hostname
        _ = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme.lower() == "https"
        and bool(hostname)
        and parsed.username is None
        and parsed.password is None
        and not parsed.fragment
    )


def _is_https_origin(value: str) -> bool:
    parsed = _safe_urlsplit(value)
    if parsed is None:
        return False
    return _is_https_url(value) and not parsed.path.rstrip("/") and not parsed.query


def _origin(value: str | None) -> str:
    if not value:
        return ""
    parsed = _safe_urlsplit(value)
    if parsed is None:
        return ""
    try:
        _ = parsed.port
    except ValueError:
        return ""
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"


def _safe_urlsplit(value: str) -> SplitResult | None:
    try:
        return urlsplit(value.strip())
    except ValueError:
        return None


def _validate_optional_https(values: Mapping[str, str], name: str, label: str) -> None:
    value = optional(values, name)
    if value is not None and not _is_https_url(value):
        raise ValueError(f"Production requires an HTTPS {label}")


def validate_profile(  # noqa: C901
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
    oidc_issuer = optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER")
    oidc_audience = optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE")
    oidc_admin_group = optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP")
    ui_issuer = optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER")
    ui_client = optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID")
    redirect_uri = optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI")
    if not _is_https_url(oidc_issuer) or not oidc_audience or not oidc_admin_group:
        raise ValueError(
            "Production requires an HTTPS bearer OIDC issuer, audience, and admin group"
        )
    if not _is_https_url(ui_issuer) or not ui_client:
        raise ValueError("Production requires an HTTPS browser OIDC issuer and client ID")
    if not _is_https_url(redirect_uri):
        raise ValueError("Production requires an HTTPS browser redirect URI")
    origins = csv(values.get("DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS", ""))
    if not origins or any(not _is_https_origin(origin) for origin in origins):
        raise ValueError("Production requires at least one HTTPS CORS origin")
    _validate_optional_https(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_JWKS_URL", "OIDC JWKS URL")
    _validate_optional_https(
        values,
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_JWKS_URL",
        "browser OIDC JWKS URL",
    )
    _validate_optional_https(
        values,
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_AUTHORIZATION_ENDPOINT",
        "browser authorization endpoint",
    )
    _validate_optional_https(
        values,
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_TOKEN_ENDPOINT",
        "browser token endpoint",
    )
    _validate_optional_https(
        values,
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_END_SESSION_ENDPOINT",
        "browser logout endpoint",
    )
    _validate_optional_https(
        values,
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGIN_REDIRECT_URI",
        "browser post-login redirect URI",
    )
    _validate_optional_https(
        values,
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI",
        "browser post-logout redirect URI",
    )
    if not csv(values.get("DAL_OBSCURA_CONTROL_PLANE_CATALOG_EGRESS_ALLOWLIST", "")):
        raise ValueError("Production requires an explicit catalog egress allowlist")
    bootstrap_enabled = (
        values.get(
            "DAL_OBSCURA_CONTROL_PLANE_BOOTSTRAP_ENABLED",
            "false",
        )
        .strip()
        .lower()
    )
    if bootstrap_enabled != "false":
        raise ValueError("Static bootstrap admin access must be disabled in production")
    if any(
        optional(values, name)
        for name in (
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_TOKEN_URL",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_ID",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_CLIENT_SECRET",
            "DAL_OBSCURA_CONTROL_PLANE_UI_DEMO_LOGIN_PASSWORDS",
            "DAL_OBSCURA_CONTROL_PLANE_UI_LOGIN_SHORTCUTS",
        )
    ):
        raise ValueError("Demo login shortcuts are forbidden in production")
    post_logout_redirect_uri = optional(
        values,
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI",
    )
    if not _is_https_url(post_logout_redirect_uri):
        raise ValueError("Production requires an HTTPS browser post-logout redirect URI")
    allowed_origins = {_origin(origin) for origin in origins}
    if (
        _origin(redirect_uri) not in allowed_origins
        or _origin(post_logout_redirect_uri) not in allowed_origins
    ):
        raise ValueError("Production OIDC redirect origins must be listed in CORS origins")
    cursor_secret = optional(values, "DAL_OBSCURA_CONTROL_PLANE_CATALOG_CURSOR_SECRET")
    if cursor_secret is None or len(cursor_secret.encode("utf-8")) < 32:
        raise ValueError(
            "DAL_OBSCURA_CONTROL_PLANE_CATALOG_CURSOR_SECRET must contain at least "
            "32 bytes in production"
        )


def bootstrap_enabled(values: Mapping[str, str], profile: str) -> bool:
    raw = (
        values.get(
            "DAL_OBSCURA_CONTROL_PLANE_BOOTSTRAP_ENABLED",
            "true" if profile == "local" else "false",
        )
        .strip()
        .lower()
    )
    if raw not in {"true", "false"}:
        raise ValueError("DAL_OBSCURA_CONTROL_PLANE_BOOTSTRAP_ENABLED must be true or false")
    return raw == "true"


def ui_auth_config(values: Mapping[str, str]) -> dict[str, object] | None:
    issuer = optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER")
    client_id = optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID")
    if issuer is None and client_id is None:
        return None
    if issuer is None or client_id is None:
        raise ValueError("UI OIDC issuer and client ID must be configured together")
    config: dict[str, object] = {
        "authority": issuer,
        "client_id": client_id,
        "redirect_uri": optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI"),
        "post_logout_redirect_uri": optional(
            values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI"
        ),
        "post_login_redirect_uri": optional(
            values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGIN_REDIRECT_URI"
        ),
        "scope": values.get("DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_SCOPE", "openid profile"),
        "authorization_endpoint": optional(
            values,
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_AUTHORIZATION_ENDPOINT",
        ),
        "token_endpoint": optional(
            values,
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_TOKEN_ENDPOINT",
        ),
        "end_session_endpoint": optional(
            values,
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_END_SESSION_ENDPOINT",
        ),
    }
    return {key: value for key, value in config.items() if value is not None}
