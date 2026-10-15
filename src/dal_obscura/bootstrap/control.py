"""Control-plane dependency construction and owned process startup."""

from __future__ import annotations

import sys
from collections.abc import Mapping
from contextlib import ExitStack
from urllib.parse import SplitResult, urlsplit

import uvicorn
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.adapters.duckdb.compiler import DuckDBProjectionCompiler
from dal_obscura.adapters.duckdb.transform import (
    DuckDBRowTransformAdapter,
    SupervisedRowTransformAdapter,
)
from dal_obscura.adapters.providers.governance import (
    ProviderAssetSchemaLoader,
    ProviderCatalogBrowser,
    SyntheticPreviewIdentityMapper,
)
from dal_obscura.adapters.providers.plugin_control import AdmittedPluginControl
from dal_obscura.adapters.sql.database.db import (
    ConfigStoreSchemaError,
    check_config_store_schema,
    create_engine_from_url,
    session_factory,
)
from dal_obscura.adapters.sql.unit_of_work import SqlGovernanceUnitOfWorkFactory
from dal_obscura.application.governance.assets.service import AssetsCommands
from dal_obscura.application.governance.audit_service import AuditQueries
from dal_obscura.application.governance.catalogs.discovery import DiscoveryCursors
from dal_obscura.application.governance.catalogs.service import CatalogsCommands
from dal_obscura.application.governance.plugins import PluginCommands
from dal_obscura.application.governance.policies.service import PoliciesCommands
from dal_obscura.application.governance.sessions import SessionCommands
from dal_obscura.application.governance.workspace.service import WorkspaceCommands
from dal_obscura.application.reads.execution import ExecutionSupervisor
from dal_obscura.identity.http_session import (
    create_oidc_actor_resolver,
    create_oidc_nonce_actor_resolver,
    exchange_authorization_code,
    oidc_actor_from_header,
)
from dal_obscura.interfaces.http.app import create_app
from dal_obscura.interfaces.http.routes.deps import AuthorizationCodeExchange, ControlPlaneDeps
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from dal_obscura.kernel.identity.session import OidcActorResolver, OidcNonceActorResolver
from dal_obscura.plugins import load_plugin_lock_file
from dal_obscura.plugins.bootstrap import create_plugin_registry
from dal_obscura.plugins.registry import PluginRegistry
from dal_obscura.runtime.secrets import SecretProvider, load_secret_provider_from_environment


def build_control_app(
    session_maker: sessionmaker[Session],
    *,
    admin_token: str,
    oidc_actor_resolver: OidcActorResolver | None = None,
    oidc_admin_group: str | None = None,
    cors_origins: tuple[str, ...] = (),
    ui_auth_config: Mapping[str, object] | None = None,
    session_ttl_seconds: int = 28800,
    session_idle_ttl_seconds: int = 1800,
    oidc_nonce_actor_resolver: OidcNonceActorResolver | None = None,
    authorization_code_exchange: AuthorizationCodeExchange | None = None,
    catalog_egress_allowlist: tuple[str, ...] = (),
    bootstrap_enabled: bool | None = None,
    max_request_bytes: int = 1048576,
    login_rate_limit_attempts: int = 20,
    login_rate_limit_window_seconds: int = 60,
    login_rate_limit_block_seconds: int = 300,
    login_rate_limit_aggregate_attempts: int = 200,
    trusted_proxy_peers: tuple[str, ...] = (),
    plugin_registry: PluginRegistry | None = None,
    secret_provider: SecretProvider | None = None,
    catalog_cursor_secret: bytes | None = None,
) -> FastAPI:
    """Compose governed HTTP operations using the same path in startup and tests."""
    if max_request_bytes <= 0:
        raise ValueError("max_request_bytes must be positive")
    if (
        login_rate_limit_attempts <= 0
        or login_rate_limit_window_seconds <= 0
        or login_rate_limit_block_seconds <= 0
        or login_rate_limit_aggregate_attempts <= 0
    ):
        raise ValueError("login rate-limit values must be positive")

    # Every serving process has one admitted registry generation. The
    # composition root may replace it with an operator-allowlisted registry,
    # but routes must never synthesize a duplicate descriptor set or silently
    # fall back when admission is absent.
    if plugin_registry is None:
        plugin_registry = create_plugin_registry()

    def resolve_actor(token: str) -> ControlPlaneActor | None:
        if oidc_actor_resolver is None:
            return None
        return oidc_actor_from_header(
            f"Bearer {token}", resolver=oidc_actor_resolver, admin_group=oidc_admin_group
        )

    work_factory = SqlGovernanceUnitOfWorkFactory(session_maker)
    schema_loader = ProviderAssetSchemaLoader(
        egress_allowlist=catalog_egress_allowlist,
        plugin_registry=plugin_registry,
        secret_provider=secret_provider,
    )
    assets_commands = AssetsCommands(
        work_factory,
        schema_loader=schema_loader,
        plugin_registry=plugin_registry,
        egress_allowlist=catalog_egress_allowlist,
    )
    policies_commands = PoliciesCommands(
        work_factory,
        schema_loader=schema_loader,
        plugin_registry=plugin_registry,
        egress_allowlist=catalog_egress_allowlist,
        compiler=DuckDBProjectionCompiler(),
        transform=SupervisedRowTransformAdapter(
            DuckDBRowTransformAdapter(), supervisor=ExecutionSupervisor()
        ),
        identity_mapper=SyntheticPreviewIdentityMapper(),
    )
    catalogs_commands = CatalogsCommands(
        work_factory,
        browser=ProviderCatalogBrowser(
            plugin_registry=plugin_registry, secret_provider=secret_provider
        ),
        plugin_registry=plugin_registry,
        discovery_cursors=DiscoveryCursors(catalog_cursor_secret),
        egress_allowlist=catalog_egress_allowlist,
    )
    workspace_commands = WorkspaceCommands(
        work_factory, identity_mapper=SyntheticPreviewIdentityMapper()
    )
    audit_queries = AuditQueries(work_factory)
    plugin_commands = PluginCommands(work_factory, registry=AdmittedPluginControl(plugin_registry))
    deps = ControlPlaneDeps(
        catalogs_commands=catalogs_commands,
        workspace_commands=workspace_commands,
        audit_queries=audit_queries,
        plugin_commands=plugin_commands,
        assets_commands=assets_commands,
        policies_commands=policies_commands,
        sessions=SessionCommands(
            work_factory,
            session_ttl_seconds=session_ttl_seconds,
            session_idle_ttl_seconds=session_idle_ttl_seconds,
            login_rate_limit_attempts=login_rate_limit_attempts,
            login_rate_limit_window_seconds=login_rate_limit_window_seconds,
            login_rate_limit_block_seconds=login_rate_limit_block_seconds,
            login_rate_limit_aggregate_attempts=login_rate_limit_aggregate_attempts,
        ),
        admin_token=admin_token,
        actor_resolver=resolve_actor,
        oidc_admin_group=oidc_admin_group,
        ui_auth_config=ui_auth_config,
        bootstrap_enabled=True if bootstrap_enabled is None else bootstrap_enabled,
        allowed_origins=cors_origins,
        oidc_nonce_actor_resolver=oidc_nonce_actor_resolver,
        trusted_proxy_peers=trusted_proxy_peers,
        authorization_code_exchange=(
            authorization_code_exchange
            or (lambda config, code, verifier: exchange_authorization_code(config, code, verifier))
        ),
    )

    def readiness() -> None:
        with session_maker() as session:
            session.execute(text("SELECT 1"))

    return create_app(
        deps, readiness=readiness, cors_origins=cors_origins, max_request_bytes=max_request_bytes
    )


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
    value = _optional(values, name)
    if value is not None and not _is_https_url(value):
        raise ValueError(f"Production requires an HTTPS {label}")


def _validate_profile(  # noqa: C901
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
    oidc_admin_group = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP")
    ui_issuer = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER")
    ui_client = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID")
    redirect_uri = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI")
    if not _is_https_url(oidc_issuer) or not oidc_audience or not oidc_admin_group:
        raise ValueError(
            "Production requires an HTTPS bearer OIDC issuer, audience, and admin group"
        )
    if not _is_https_url(ui_issuer) or not ui_client:
        raise ValueError("Production requires an HTTPS browser OIDC issuer and client ID")
    if not _is_https_url(redirect_uri):
        raise ValueError("Production requires an HTTPS browser redirect URI")
    origins = _csv(values.get("DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS", ""))
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
    if not _csv(values.get("DAL_OBSCURA_CONTROL_PLANE_CATALOG_EGRESS_ALLOWLIST", "")):
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
    post_logout_redirect_uri = _optional(
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
    cursor_secret = _optional(values, "DAL_OBSCURA_CONTROL_PLANE_CATALOG_CURSOR_SECRET")
    if cursor_secret is None or len(cursor_secret.encode("utf-8")) < 32:
        raise ValueError(
            "DAL_OBSCURA_CONTROL_PLANE_CATALOG_CURSOR_SECRET must contain at least "
            "32 bytes in production"
        )


def _oidc_resolver(values: Mapping[str, str]) -> OidcActorResolver | None:
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


def _bootstrap_enabled(values: Mapping[str, str], profile: str) -> bool:
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
        "end_session_endpoint": _optional(
            values,
            "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_END_SESSION_ENDPOINT",
        ),
    }
    return {key: value for key, value in config.items() if value is not None}


def _ui_nonce_resolver(values: Mapping[str, str]) -> OidcNonceActorResolver | None:
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
    return create_oidc_nonce_actor_resolver(
        issuer=issuer,
        audience=client_id,
        jwks_url=jwks_url,
        subject_claim=subject_claim,
        group_claims=group_claims,
    )


def run_control_plane(values: Mapping[str, str], database_url: str, admin_token: str) -> int:
    with ExitStack() as resources:
        try:
            _validate_profile(values, admin_token, database_url)
            profile = values.get("DAL_OBSCURA_CONTROL_PLANE_PROFILE", "local").strip().lower()
            cors_origins = _csv(values.get("DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS", ""))
            if profile == "local" and not cors_origins:
                # The supported local UI is served by the Vite dev server. Keep
                # browser CSRF/origin checks enabled without requiring a second
                # environment variable for the disposable local profile.
                cors_origins = ("http://127.0.0.1:5173", "http://localhost:5173")
            port = _port(values.get("DAL_OBSCURA_CONTROL_PLANE_PORT", "8820"))
            engine = create_engine_from_url(database_url)
            resources.callback(engine.dispose)
            check_config_store_schema(engine)
            oidc_resolver = _oidc_resolver(values)
            plugin_lock_file = _optional(values, "DAL_OBSCURA_PLUGIN_LOCK_FILE")
            plugin_registry = create_plugin_registry(
                allowlist=load_plugin_lock_file(plugin_lock_file) if plugin_lock_file else None,
                environment=values,
            )
            secret_provider = load_secret_provider_from_environment(
                values,
                require_scope_grants=profile == "production",
                registry=plugin_registry,
            )
            resources.callback(secret_provider.close)
            app = build_control_app(
                session_factory(engine),
                admin_token=admin_token,
                oidc_actor_resolver=oidc_resolver,
                oidc_admin_group=_optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP"),
                cors_origins=cors_origins,
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
                catalog_egress_allowlist=_csv(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_CATALOG_EGRESS_ALLOWLIST", "")
                ),
                bootstrap_enabled=_bootstrap_enabled(values, profile),
                max_request_bytes=_positive_int(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_MAX_REQUEST_BYTES", "1048576"),
                    "DAL_OBSCURA_CONTROL_PLANE_MAX_REQUEST_BYTES",
                ),
                login_rate_limit_attempts=_positive_int(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_ATTEMPTS", "20"),
                    "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_ATTEMPTS",
                ),
                login_rate_limit_window_seconds=_positive_int(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_WINDOW_SECONDS", "60"),
                    "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_WINDOW_SECONDS",
                ),
                login_rate_limit_block_seconds=_positive_int(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_BLOCK_SECONDS", "300"),
                    "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_BLOCK_SECONDS",
                ),
                login_rate_limit_aggregate_attempts=_positive_int(
                    values.get(
                        "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_AGGREGATE_ATTEMPTS",
                        "200",
                    ),
                    "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_AGGREGATE_ATTEMPTS",
                ),
                trusted_proxy_peers=_csv(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_TRUSTED_PROXY_PEERS", "")
                ),
                plugin_registry=plugin_registry,
                secret_provider=secret_provider,
                catalog_cursor_secret=(
                    cursor_secret.encode("utf-8")
                    if (
                        cursor_secret := _optional(
                            values, "DAL_OBSCURA_CONTROL_PLANE_CATALOG_CURSOR_SECRET"
                        )
                    )
                    else None
                ),
            )
        except (ConfigStoreSchemaError, ValueError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            return 1
        except SQLAlchemyError:
            print("Control-plane database unavailable", file=sys.stderr)
            return 1
        uvicorn.run(
            app,
            host=values.get("DAL_OBSCURA_CONTROL_PLANE_HOST", "127.0.0.1"),
            port=port,
            log_level=values.get("DAL_OBSCURA_CONTROL_PLANE_LOG_LEVEL", "info").lower(),
        )
        return 0
