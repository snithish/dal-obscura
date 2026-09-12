"""Shared FastAPI dependencies for control-plane route modules.

Example:
    ```python
    deps = ControlPlaneDeps(
        session_maker=session_maker,
        admin_token="dev-admin",
        oidc_actor_resolver=None,
        oidc_admin_group=None,
        ui_auth_config=None,
        demo_token_exchange=lambda config, username: "",
    )
    ```
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import cast

from fastapi import Cookie, Header, HTTPException, Request
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import (
    AuthorizationFailure,
    PublicationConflictError,
    ValidationFailure,
)
from dal_obscura.control_plane.application.provisioning import ProvisioningService
from dal_obscura.control_plane.infrastructure.session_store import (
    BrowserSessionStore,
    LoginTransaction,
    LoginTransactionStore,
)
from dal_obscura.control_plane.interfaces.session_api import (
    OidcActorResolver,
    OidcNonceActorResolver,
    oidc_actor_from_header,
)

DemoTokenExchange = Callable[[Mapping[str, object], str], str]
AuthorizationCodeExchange = Callable[[Mapping[str, object], str, str], Mapping[str, object]]


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
    demo_token_exchange: DemoTokenExchange
    session_ttl_seconds: int = 28_800
    session_idle_ttl_seconds: int = 1_800
    allowed_origins: tuple[str, ...] = ()
    oidc_nonce_actor_resolver: OidcNonceActorResolver | None = None
    authorization_code_exchange: AuthorizationCodeExchange | None = None
    require_review: bool = False
    review_secret: str = ""
    catalog_egress_allowlist: tuple[str, ...] = ()

    def require_actor(
        self,
        request: Request,
        authorization: str = Header(default=""),
        session_token: str | None = Cookie(default=None, alias="dal_obscura_session"),
        csrf_cookie: str | None = Cookie(default=None, alias="dal_obscura_csrf"),
    ) -> ControlPlaneActor:
        """Authenticates bearer clients or an HttpOnly browser session."""
        expected = f"Bearer {self.admin_token}"
        if authorization == expected:
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
        origin = request.headers.get("origin")
        request_origin = f"{request.url.scheme}://{request.url.netloc}"
        if origin and origin not in self.allowed_origins and origin != request_origin:
            raise HTTPException(status_code=403, detail="Origin validation failed")

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
        )

    def require_admin(
        self,
        request: Request,
        authorization: str = Header(default=""),
        session_token: str | None = Cookie(default=None, alias="dal_obscura_session"),
        csrf_cookie: str | None = Cookie(default=None, alias="dal_obscura_csrf"),
    ) -> ControlPlaneActor:
        actor = self.require_actor(
            request=request,
            authorization=authorization,
            session_token=session_token,
            csrf_cookie=csrf_cookie,
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
                raise HTTPException(status_code=409, detail=str(exc)) from exc
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
