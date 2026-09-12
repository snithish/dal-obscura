"""FastAPI control-plane application factory.

Example:
    ```python
    app = create_app(session_factory(engine), admin_token="dev-admin")
    ```
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.control_plane.infrastructure.request_context import (
    reset_request_id,
    set_request_id,
)
from dal_obscura.control_plane.interfaces.health import install_health_routes
from dal_obscura.control_plane.interfaces.routes import (
    assets as asset_routes,
)
from dal_obscura.control_plane.interfaces.routes import (
    catalogs as catalog_routes,
)
from dal_obscura.control_plane.interfaces.routes import (
    policies as policy_routes,
)
from dal_obscura.control_plane.interfaces.routes import (
    session as session_routes,
)
from dal_obscura.control_plane.interfaces.routes import (
    settings as settings_routes,
)
from dal_obscura.control_plane.interfaces.routes import (
    workspace as workspace_routes,
)
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.session_api import (
    OidcActorResolver,
    OidcNonceActorResolver,
    create_oidc_nonce_actor_resolver,
    exchange_authorization_code,
    exchange_demo_password_token,
)
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest
from dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks import (
    OidcJwksIdentityProvider,
)


class _RequestBodyTooLarge(Exception):
    """Raised by the receive wrapper when a streamed request exceeds its bound."""


_REQUEST_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def create_oidc_actor_resolver(
    *,
    issuer: str,
    audience: str | None,
    jwks_url: str | None,
    subject_claim: str,
    group_claims: tuple[str, ...],
) -> OidcActorResolver:
    """Builds a bearer-token resolver for control-plane OIDC users.

    Example:
        ```python
        resolver = create_oidc_actor_resolver(
            issuer="https://idp.example.com/realms/data",
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
        return {"principal": principal.id, "groups": principal.groups}

    return resolve


_create_oidc_nonce_actor_resolver = create_oidc_nonce_actor_resolver
_exchange_authorization_code = exchange_authorization_code


_exchange_demo_password_token = exchange_demo_password_token


def create_app(  # noqa: C901
    session_maker: sessionmaker[Session],
    *,
    admin_token: str,
    oidc_actor_resolver: OidcActorResolver | None = None,
    oidc_admin_group: str | None = None,
    cors_origins: tuple[str, ...] = (),
    ui_auth_config: Mapping[str, object] | None = None,
    session_ttl_seconds: int = 28_800,
    session_idle_ttl_seconds: int = 1_800,
    oidc_nonce_actor_resolver: OidcNonceActorResolver | None = None,
    require_review: bool = False,
    review_secret: str | None = None,
    catalog_egress_allowlist: tuple[str, ...] = (),
    bootstrap_enabled: bool | None = None,
    max_request_bytes: int = 1_048_576,
    login_rate_limit_attempts: int = 20,
    login_rate_limit_window_seconds: int = 60,
    login_rate_limit_block_seconds: int = 300,
) -> FastAPI:
    """Creates the control-plane FastAPI app with all workspace routes installed.

    Example:
        ```python
        app = create_app(session_factory(engine), admin_token="dev-admin")
        ```
    """

    if max_request_bytes <= 0:
        raise ValueError("max_request_bytes must be positive")
    if (
        login_rate_limit_attempts <= 0
        or login_rate_limit_window_seconds <= 0
        or login_rate_limit_block_seconds <= 0
    ):
        raise ValueError("login rate-limit values must be positive")

    app = FastAPI(
        title="dal-obscura control-plane API",
        summary="Configuration, catalog, asset, policy, and session API for dal-obscura.",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    @app.middleware("http")
    async def security_headers(request, call_next):
        response = await call_next(request)
        if request.url.path.startswith(("/v1/", "/auth/")):
            response.headers["cache-control"] = "no-store"
            response.headers["pragma"] = "no-cache"
        response.headers.setdefault("x-content-type-options", "nosniff")
        response.headers.setdefault("referrer-policy", "no-referrer")
        return response

    @app.middleware("http")
    async def request_correlation(request: Request, call_next):
        supplied = request.headers.get("x-request-id", "").strip()
        request_id = supplied if _REQUEST_ID.fullmatch(supplied) else uuid4().hex
        token = set_request_id(request_id)
        try:
            response = await call_next(request)
            response.headers["x-request-id"] = request_id
            return response
        finally:
            reset_request_id(token)

    @app.middleware("http")
    async def request_size_limit(request: Request, call_next):
        raw_length = request.headers.get("content-length")
        if raw_length:
            try:
                content_length = int(raw_length)
            except ValueError:
                return JSONResponse(status_code=400, content={"detail": "Invalid content length"})
            if content_length > max_request_bytes:
                return JSONResponse(status_code=413, content={"detail": "Request body too large"})
        received = 0
        original_receive = request.receive

        async def limited_receive():
            nonlocal received
            message = await original_receive()
            if message.get("type") == "http.request":
                received += len(message.get("body", b""))
                if received > max_request_bytes:
                    raise _RequestBodyTooLarge
            return message

        # Starlette's ``call_next`` rebuilds the downstream Request from the
        # scope, so replace the receive callable on the original request rather
        # than passing a second Request instance.
        request._receive = limited_receive
        try:
            return await call_next(request)
        except _RequestBodyTooLarge:
            return JSONResponse(status_code=413, content={"detail": "Request body too large"})

    if cors_origins:
        app.add_middleware(
            CORSMiddleware,  # ty: ignore[invalid-argument-type]
            allow_origins=list(cors_origins),
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "OPTIONS"],
            allow_headers=["authorization", "content-type", "accept", "x-csrf-token"],
        )
    install_health_routes(app, session_maker)
    deps = ControlPlaneDeps(
        session_maker=session_maker,
        admin_token=admin_token,
        oidc_actor_resolver=oidc_actor_resolver,
        oidc_admin_group=oidc_admin_group,
        ui_auth_config=ui_auth_config,
        demo_token_exchange=lambda config, username: _exchange_demo_password_token(
            config,
            username,
        ),
        session_ttl_seconds=session_ttl_seconds,
        session_idle_ttl_seconds=session_idle_ttl_seconds,
        require_review=require_review,
        # Local callers may continue to use the test/admin secret. Production
        # wiring supplies a separate review key so compromising one boundary
        # does not automatically forge review evidence.
        review_secret=(review_secret or (admin_token if require_review else "")),
        catalog_egress_allowlist=catalog_egress_allowlist,
        bootstrap_enabled=(not require_review if bootstrap_enabled is None else bootstrap_enabled),
        allowed_origins=cors_origins,
        oidc_nonce_actor_resolver=oidc_nonce_actor_resolver,
        login_rate_limit_attempts=login_rate_limit_attempts,
        login_rate_limit_window_seconds=login_rate_limit_window_seconds,
        login_rate_limit_block_seconds=login_rate_limit_block_seconds,
        authorization_code_exchange=lambda config, code, verifier: _exchange_authorization_code(
            config,
            code,
            verifier,
        ),
    )

    for route in (
        session_routes.router,
        workspace_routes.router,
        catalog_routes.router,
        policy_routes.router,
        asset_routes.router,
        settings_routes.router,
    ):
        app.include_router(route(deps))
    return app
