"""Shared FastAPI dependencies for control-plane route modules.

Example:
    ```python
    deps = ControlPlaneDeps(
        session_maker=session_maker,
        admin_token="dev-admin",
        oidc_actor_resolver=None,
        oidc_admin_group=None,
        ui_auth_config=None,
    )
    ```
"""

from __future__ import annotations

import ipaddress
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import cast
from urllib.parse import urlsplit

from fastapi import Cookie, Header, HTTPException, Request
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.common.plugin_api.registry import PluginRegistry
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import (
    AuthorizationFailure,
    PublicationConflictError,
    RevisionPreconditionRequired,
    ValidationFailure,
)
from dal_obscura.control_plane.application.provisioning import ProvisioningService
from dal_obscura.control_plane.infrastructure.session_store import (
    BrowserSessionStore,
    LoginRateLimitDecision,
    LoginRateLimiter,
    LoginTransaction,
    LoginTransactionStore,
)
from dal_obscura.control_plane.interfaces.session_api import (
    OidcActorResolver,
    OidcNonceActorResolver,
    oidc_actor_from_header,
)
from dal_obscura.data_plane.infrastructure.adapters.secret_providers import SecretProvider

AuthorizationCodeExchange = Callable[[Mapping[str, object], str, str], Mapping[str, object]]
MAX_BROWSER_SESSION_TTL_SECONDS = 86_400
MAX_BROWSER_IDLE_TTL_SECONDS = 7_200


@dataclass(frozen=True)
class ControlPlaneDeps:
    """Route dependency bundle for auth checks and service session handling.

    Example:
        ```python
        actor = deps.require_admin("Bearer dev-admin")
        ```
    """

    session_maker: sessionmaker[Session]
    admin_token: str
    oidc_actor_resolver: OidcActorResolver | None
    oidc_admin_group: str | None
    ui_auth_config: Mapping[str, object] | None
    session_ttl_seconds: int = 28_800
    session_idle_ttl_seconds: int = 1_800
    allowed_origins: tuple[str, ...] = ()
    oidc_nonce_actor_resolver: OidcNonceActorResolver | None = None
    authorization_code_exchange: AuthorizationCodeExchange | None = None
    require_review: bool = False
    review_secret: str = ""
    catalog_egress_allowlist: tuple[str, ...] = ()
    bootstrap_enabled: bool = True
    login_rate_limit_attempts: int = 20
    login_rate_limit_window_seconds: int = 60
    login_rate_limit_block_seconds: int = 300
    login_rate_limit_aggregate_attempts: int = 200
    trusted_proxy_peers: tuple[str, ...] = ()
    plugin_registry: PluginRegistry | None = None
    secret_provider: SecretProvider | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.session_ttl_seconds <= MAX_BROWSER_SESSION_TTL_SECONDS:
            raise ValueError("session TTL must be between 1 second and 24 hours")
        if not 1 <= self.session_idle_ttl_seconds <= MAX_BROWSER_IDLE_TTL_SECONDS:
            raise ValueError("idle session TTL must be between 1 second and 2 hours")
        if self.session_idle_ttl_seconds > self.session_ttl_seconds:
            raise ValueError("idle session TTL cannot exceed session TTL")
        if self.login_rate_limit_aggregate_attempts <= 0:
            raise ValueError("aggregate login rate-limit attempts must be positive")
        for peer in self.trusted_proxy_peers:
            _parse_proxy_network(peer)

    def require_actor(
        self,
        request: Request,
        authorization: str = Header(default=""),
        session_token: str | None = Cookie(default=None, alias="dal_obscura_session"),
        csrf_cookie: str | None = Cookie(default=None, alias="dal_obscura_csrf"),
        host_session_token: str | None = Cookie(default=None, alias="__Host-dal_obscura_session"),
        host_csrf_cookie: str | None = Cookie(default=None, alias="__Host-dal_obscura_csrf"),
    ) -> ControlPlaneActor:
        """Authenticates bearer clients or an HttpOnly browser session."""
        # FastAPI normally injects cookie parameters as strings.  Keep the
        # dependency boundary defensive because direct dependency invocation
        # and older Starlette/Pydantic combinations can leave the marker
        # object in place when the cookie is absent.
        session_token = _coalesce_cookie(host_session_token, session_token)
        csrf_cookie = _coalesce_cookie(host_csrf_cookie, csrf_cookie)
        expected = f"Bearer {self.admin_token}"
        if self.bootstrap_enabled and authorization == expected:
            return ControlPlaneActor.for_platform_admin("platform:admin")
        using_cookie = not authorization and session_token is not None
        if using_cookie and request.method not in {"GET", "HEAD", "OPTIONS"}:
            self.validate_browser_mutation(request, csrf_cookie)
        if using_cookie:
            if session_token is None:
                raise HTTPException(status_code=401, detail="Unauthorized")
            try:
                actor = self.resolve_browser_session(session_token, csrf_cookie)
            except ValueError as exc:
                raise HTTPException(status_code=403, detail="CSRF validation failed") from exc
        else:
            token = _bearer_value(authorization)
            actor = self.resolve_bearer_token(token) if token is not None else None
        if actor is None:
            raise HTTPException(status_code=401, detail="Unauthorized")
        return actor

    def validate_browser_mutation(
        self,
        request: Request,
        csrf_cookie: str | None,
    ) -> None:
        """Checks CSRF and origin before a browser state mutation.

        Logout uses this check even when the server session has already
        expired, so a stale browser can clear its credentials idempotently.
        """

        if not csrf_cookie or request.headers.get("x-csrf-token") != csrf_cookie:
            raise HTTPException(status_code=403, detail="CSRF validation failed")
        origin = request.headers.get("origin", "").strip()
        if origin and _canonical_origin(origin) not in self._allowed_browser_origins():
            raise HTTPException(status_code=403, detail="Origin validation failed")

    def _allowed_browser_origins(self) -> set[str]:
        origins = {
            normalized for value in self.allowed_origins if (normalized := _canonical_origin(value))
        }
        if self.ui_auth_config is None:
            return origins
        for key in ("redirect_uri", "post_login_redirect_uri", "post_logout_redirect_uri"):
            value = self.ui_auth_config.get(key)
            if isinstance(value, str):
                normalized = _canonical_origin(value)
                if normalized:
                    origins.add(normalized)
        return origins

    def resolve_bearer_token(self, token: str) -> ControlPlaneActor | None:
        """Resolves a provider bearer token into an actor."""

        if self.oidc_actor_resolver is None:
            return None
        return oidc_actor_from_header(
            f"Bearer {token}",
            resolver=self.oidc_actor_resolver,
            admin_group=self.oidc_admin_group,
        )

    def resolve_browser_session(
        self,
        token: str,
        csrf_token: str | None = None,
    ) -> ControlPlaneActor | None:
        """Loads an unexpired browser session without exposing provider tokens."""

        with self.session_maker() as session:
            actor = BrowserSessionStore(session).resolve(
                token,
                csrf_token=csrf_token,
                idle_ttl_seconds=self.session_idle_ttl_seconds,
            )
            if actor is not None:
                session.commit()
            return actor

    def issue_browser_session(self, actor: ControlPlaneActor) -> str:
        """Mints a random, durable browser session secret."""

        token, _csrf_token = self.issue_browser_session_credentials(actor)
        return token

    def issue_browser_session_credentials(self, actor: ControlPlaneActor) -> tuple[str, str]:
        """Mints the session and its browser-readable, session-bound CSRF secret."""

        with self.session_maker() as session:
            credentials = BrowserSessionStore(session).issue_with_csrf(
                actor,
                ttl_seconds=self.session_ttl_seconds,
            )
            session.commit()
            return credentials

    def revoke_browser_session(self, token: str) -> None:
        """Revokes a browser session secret, if it exists."""

        with self.session_maker() as session:
            BrowserSessionStore(session).revoke(token)
            session.commit()

    def issue_login_transaction(
        self,
        *,
        state: str,
        nonce: str,
        code_verifier: str,
        redirect_uri: str,
    ) -> None:
        with self.session_maker() as session:
            LoginTransactionStore(session).issue(
                state=state,
                nonce=nonce,
                code_verifier=code_verifier,
                redirect_uri=redirect_uri,
            )
            session.commit()

    def check_login_rate_limit(
        self,
        client_key: str,
        *,
        aggregate_key: str | None = None,
    ) -> LoginRateLimitDecision:
        """Records a login start and returns the generic admission decision."""

        with self.session_maker() as session:
            limiter = LoginRateLimiter(session)
            decision = limiter.allow(
                client_key,
                max_attempts=self.login_rate_limit_attempts,
                window_seconds=self.login_rate_limit_window_seconds,
                block_seconds=self.login_rate_limit_block_seconds,
            )
            if decision.allowed and aggregate_key and aggregate_key != client_key:
                decision = limiter.allow(
                    aggregate_key,
                    max_attempts=self.login_rate_limit_aggregate_attempts,
                    window_seconds=self.login_rate_limit_window_seconds,
                    block_seconds=self.login_rate_limit_block_seconds,
                )
            session.commit()
            return decision

    def record_login_failure(
        self,
        client_key: str,
        *,
        aggregate_key: str | None = None,
    ) -> LoginRateLimitDecision:
        """Records a failed callback using the same durable client window."""

        with self.session_maker() as session:
            limiter = LoginRateLimiter(session)
            decision = limiter.record_failure(
                client_key,
                max_attempts=self.login_rate_limit_attempts,
                window_seconds=self.login_rate_limit_window_seconds,
                block_seconds=self.login_rate_limit_block_seconds,
            )
            if decision.allowed and aggregate_key and aggregate_key != client_key:
                decision = limiter.record_failure(
                    aggregate_key,
                    max_attempts=self.login_rate_limit_aggregate_attempts,
                    window_seconds=self.login_rate_limit_window_seconds,
                    block_seconds=self.login_rate_limit_block_seconds,
                )
            session.commit()
            return decision

    def clear_login_rate_limit(self, client_key: str) -> None:
        """Clears login abuse state after a successful callback."""

        with self.session_maker() as session:
            LoginRateLimiter(session).clear(client_key)
            session.commit()

    def consume_login_transaction(self, state: str) -> LoginTransaction | None:
        with self.session_maker() as session:
            transaction = LoginTransactionStore(session).consume(state)
            session.commit()
            return transaction

    def resolve_nonce_token(self, token: str, nonce_hash: str) -> ControlPlaneActor | None:
        if self.oidc_nonce_actor_resolver is None:
            return None
        try:
            resolved = self.oidc_nonce_actor_resolver(token, nonce_hash)
        except Exception:
            return None
        if not isinstance(resolved, Mapping):
            return None
        resolved_payload = cast(Mapping[str, object], resolved)
        principal = resolved_payload.get("principal")
        raw_groups = resolved_payload.get("groups", ())
        if isinstance(raw_groups, str):
            groups = (raw_groups,)
        elif isinstance(raw_groups, (list, tuple, set)):
            groups = tuple(str(group) for group in raw_groups)
        else:
            groups = ()
        if not principal:
            return None
        return ControlPlaneActor(
            principal=str(principal),
            groups=tuple(str(group) for group in groups if str(group).strip()),
            platform_admin=bool(
                self.oidc_admin_group and self.oidc_admin_group in {str(group) for group in groups}
            ),
            issuer=str(resolved_payload.get("issuer", "")).strip(),
        )

    def require_admin(
        self,
        request: Request,
        authorization: str = Header(default=""),
        session_token: str | None = Cookie(default=None, alias="dal_obscura_session"),
        csrf_cookie: str | None = Cookie(default=None, alias="dal_obscura_csrf"),
        host_session_token: str | None = Cookie(default=None, alias="__Host-dal_obscura_session"),
        host_csrf_cookie: str | None = Cookie(default=None, alias="__Host-dal_obscura_csrf"),
    ) -> ControlPlaneActor:
        actor = self.require_actor(
            request=request,
            authorization=authorization,
            session_token=_coalesce_cookie(host_session_token, session_token),
            csrf_cookie=_coalesce_cookie(host_csrf_cookie, csrf_cookie),
        )
        if not actor.platform_admin:
            raise HTTPException(status_code=403, detail="Platform admin required")
        return actor

    def with_service(self, callback: Callable[[ProvisioningService], object]) -> object:
        with self.session_maker() as session:
            service = ProvisioningService(
                session,
                review_secret=self.review_secret,
                require_review=self.require_review,
                catalog_egress_allowlist=self.catalog_egress_allowlist,
                plugin_registry=self.plugin_registry,
                secret_provider=self.secret_provider,
            )
            try:
                result = callback(service)
                session.commit()
                return result
            except ValidationFailure as exc:
                session.rollback()
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except AuthorizationFailure as exc:
                session.rollback()
                raise HTTPException(status_code=403, detail=str(exc)) from exc
            except PublicationConflictError as exc:
                session.rollback()
                raise HTTPException(
                    status_code=428 if isinstance(exc, RevisionPreconditionRequired) else 409,
                    detail=str(exc),
                ) from exc
            except LookupError as exc:
                session.rollback()
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except Exception:
                session.rollback()
                raise


def _bearer_value(authorization: str) -> str | None:
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None


def _cookie_text(value: object) -> str | None:
    """Return a cookie value only when FastAPI supplied a real string."""

    if isinstance(value, str):
        return value or None
    return None


def _coalesce_cookie(primary: object, fallback: object) -> str | None:
    """Accept identical duplicate cookie names but reject conflicting values."""

    first = _cookie_text(primary)
    second = _cookie_text(fallback)
    if first and second and not secrets.compare_digest(first, second):
        raise HTTPException(status_code=400, detail="Conflicting browser credentials")
    return first or second


def _canonical_origin(value: str) -> str | None:
    """Normalizes an origin or URL without trusting request Host headers."""

    parsed = urlsplit(value.strip())
    if not parsed.scheme or not parsed.netloc or parsed.username or parsed.password:
        return None
    try:
        port = parsed.port
    except ValueError:
        return None
    scheme = parsed.scheme.lower()
    hostname = parsed.hostname
    if not hostname:
        return None
    host = hostname.lower()
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    default_port = (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    return f"{scheme}://{host}{'' if port is None or default_port else f':{port}'}"


def _parse_proxy_network(value: str) -> ipaddress.IPv4Network | ipaddress.IPv6Network:
    """Parse an operator-configured proxy peer without DNS resolution."""

    text = value.strip()
    try:
        if "/" in text:
            return ipaddress.ip_network(text, strict=False)
        return ipaddress.ip_network(f"{text}/32" if ":" not in text else f"{text}/128")
    except ValueError as exc:
        raise ValueError(f"trusted proxy peer must be an IP address or CIDR: {value!r}") from exc
