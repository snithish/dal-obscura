"""Control-plane dependency construction and owned process startup."""

from __future__ import annotations

import sys
from collections.abc import Mapping
from contextlib import ExitStack

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
from dal_obscura.application.governance.plugins import PluginQueries
from dal_obscura.application.governance.policies.service import PoliciesCommands
from dal_obscura.application.governance.sessions import SessionCommands
from dal_obscura.application.governance.workspace.service import WorkspaceCommands
from dal_obscura.application.reads.execution import ExecutionSupervisor
from dal_obscura.bootstrap import control_config
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
            plugin_registry=plugin_registry,
            secret_provider=secret_provider,
            egress_allowlist=catalog_egress_allowlist,
        ),
        plugin_registry=plugin_registry,
        discovery_cursors=DiscoveryCursors(catalog_cursor_secret),
        egress_allowlist=catalog_egress_allowlist,
    )
    workspace_commands = WorkspaceCommands(
        work_factory, identity_mapper=SyntheticPreviewIdentityMapper()
    )
    audit_queries = AuditQueries(work_factory)
    plugin_queries = PluginQueries(registry=plugin_registry)
    deps = ControlPlaneDeps(
        catalogs_commands=catalogs_commands,
        workspace_commands=workspace_commands,
        audit_queries=audit_queries,
        plugin_queries=plugin_queries,
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


def _oidc_resolver(values: Mapping[str, str]) -> OidcActorResolver | None:
    issuer = control_config.optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_ISSUER")
    if issuer is None:
        return None
    return create_oidc_actor_resolver(
        issuer=issuer,
        audience=control_config.optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_AUDIENCE"),
        jwks_url=control_config.optional(values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_JWKS_URL"),
        subject_claim=values.get("DAL_OBSCURA_CONTROL_PLANE_OIDC_SUBJECT_CLAIM", "sub"),
        group_claims=control_config.csv(
            values.get("DAL_OBSCURA_CONTROL_PLANE_OIDC_GROUP_CLAIMS", "groups")
        ),
    )


def _ui_nonce_resolver(values: Mapping[str, str]) -> OidcNonceActorResolver | None:
    issuer = control_config.optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_ISSUER")
    client_id = control_config.optional(values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_CLIENT_ID")
    if issuer is None or client_id is None:
        return None
    jwks_url = control_config.optional(
        values, "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_JWKS_URL"
    ) or control_config.optional(
        values,
        "DAL_OBSCURA_CONTROL_PLANE_OIDC_JWKS_URL",
    )
    subject_claim = values.get(
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_SUBJECT_CLAIM",
        values.get("DAL_OBSCURA_CONTROL_PLANE_OIDC_SUBJECT_CLAIM", "sub"),
    )
    group_claims = control_config.csv(
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
            control_config.validate_profile(values, admin_token, database_url)
            profile = values.get("DAL_OBSCURA_CONTROL_PLANE_PROFILE", "local").strip().lower()
            cors_origins = control_config.csv(
                values.get("DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS", "")
            )
            if profile == "local" and not cors_origins:
                # The supported local UI is served by the Vite dev server. Keep
                # browser CSRF/origin checks enabled without requiring a second
                # environment variable for the disposable local profile.
                cors_origins = ("http://127.0.0.1:5173", "http://localhost:5173")
            port = control_config.port(values.get("DAL_OBSCURA_CONTROL_PLANE_PORT", "8820"))
            engine = create_engine_from_url(database_url)
            resources.callback(engine.dispose)
            check_config_store_schema(engine)
            oidc_resolver = _oidc_resolver(values)
            plugin_lock_file = control_config.optional(values, "DAL_OBSCURA_PLUGIN_LOCK_FILE")
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
                oidc_admin_group=control_config.optional(
                    values, "DAL_OBSCURA_CONTROL_PLANE_OIDC_ADMIN_GROUP"
                ),
                cors_origins=cors_origins,
                ui_auth_config=control_config.ui_auth_config(values),
                session_ttl_seconds=control_config.positive_int(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_SESSION_TTL_SECONDS", "28800"),
                    "DAL_OBSCURA_CONTROL_PLANE_SESSION_TTL_SECONDS",
                ),
                session_idle_ttl_seconds=control_config.positive_int(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_SESSION_IDLE_TTL_SECONDS", "1800"),
                    "DAL_OBSCURA_CONTROL_PLANE_SESSION_IDLE_TTL_SECONDS",
                ),
                oidc_nonce_actor_resolver=_ui_nonce_resolver(values),
                catalog_egress_allowlist=control_config.csv(
                    values.get("DAL_OBSCURA_CATALOG_EGRESS_ALLOWLIST", "")
                ),
                bootstrap_enabled=control_config.bootstrap_enabled(values, profile),
                max_request_bytes=control_config.positive_int(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_MAX_REQUEST_BYTES", "1048576"),
                    "DAL_OBSCURA_CONTROL_PLANE_MAX_REQUEST_BYTES",
                ),
                login_rate_limit_attempts=control_config.positive_int(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_ATTEMPTS", "20"),
                    "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_ATTEMPTS",
                ),
                login_rate_limit_window_seconds=control_config.positive_int(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_WINDOW_SECONDS", "60"),
                    "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_WINDOW_SECONDS",
                ),
                login_rate_limit_block_seconds=control_config.positive_int(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_BLOCK_SECONDS", "300"),
                    "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_BLOCK_SECONDS",
                ),
                login_rate_limit_aggregate_attempts=control_config.positive_int(
                    values.get(
                        "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_AGGREGATE_ATTEMPTS",
                        "200",
                    ),
                    "DAL_OBSCURA_CONTROL_PLANE_LOGIN_RATE_LIMIT_AGGREGATE_ATTEMPTS",
                ),
                trusted_proxy_peers=control_config.csv(
                    values.get("DAL_OBSCURA_CONTROL_PLANE_TRUSTED_PROXY_PEERS", "")
                ),
                plugin_registry=plugin_registry,
                secret_provider=secret_provider,
                catalog_cursor_secret=(
                    cursor_secret.encode("utf-8")
                    if (
                        cursor_secret := control_config.optional(
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
            # The application needs the socket peer to enforce its proxy trust policy.
            proxy_headers=False,
            host=values.get("DAL_OBSCURA_CONTROL_PLANE_HOST", "127.0.0.1"),
            port=port,
            log_level=values.get("DAL_OBSCURA_CONTROL_PLANE_LOG_LEVEL", "info").lower(),
        )
        return 0
