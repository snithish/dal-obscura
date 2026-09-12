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

from fastapi import Cookie, Header, HTTPException, Request
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control_plane.application.provisioning import ProvisioningService
from dal_obscura.control_plane.infrastructure.session_store import BrowserSessionStore
from dal_obscura.control_plane.interfaces.session_api import (
    OidcActorResolver,
    oidc_actor_from_header,
)

DemoTokenExchange = Callable[[Mapping[str, object], str], str]


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
    allowed_origins: tuple[str, ...] = ()

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
        if (
            using_cookie
            and request.method not in {"GET", "HEAD", "OPTIONS"}
            and (not csrf_cookie or request.headers.get("x-csrf-token") != csrf_cookie)
        ):
            raise HTTPException(status_code=403, detail="CSRF validation failed")
        if using_cookie and request.method not in {"GET", "HEAD", "OPTIONS"}:
            origin = request.headers.get("origin")
            request_origin = f"{request.url.scheme}://{request.url.netloc}"
            if origin and origin not in self.allowed_origins and origin != request_origin:
                raise HTTPException(status_code=403, detail="Origin validation failed")
        if using_cookie:
            if session_token is None:
                raise HTTPException(status_code=401, detail="Unauthorized")
            actor = self.resolve_browser_session(session_token)
        else:
            token = _bearer_value(authorization)
            actor = self.resolve_bearer_token(token) if token is not None else None
        if actor is None:
            raise HTTPException(status_code=401, detail="Unauthorized")
        return actor

    def resolve_bearer_token(self, token: str) -> ControlPlaneActor | None:
        """Resolves a provider bearer token into an actor."""

        if self.oidc_actor_resolver is None:
            return None
        return oidc_actor_from_header(
            f"Bearer {token}",
            resolver=self.oidc_actor_resolver,
            admin_group=self.oidc_admin_group,
        )

    def resolve_browser_session(self, token: str) -> ControlPlaneActor | None:
        """Loads an unexpired browser session without exposing provider tokens."""

        with self.session_maker() as session:
            actor = BrowserSessionStore(session).resolve(token)
            if actor is not None:
                session.commit()
            return actor

    def issue_browser_session(self, actor: ControlPlaneActor) -> str:
        """Mints a random, durable browser session secret."""

        with self.session_maker() as session:
            token = BrowserSessionStore(session).issue(
                actor,
                ttl_seconds=self.session_ttl_seconds,
            )
            session.commit()
            return token

    def revoke_browser_session(self, token: str) -> None:
        """Revokes a browser session secret, if it exists."""

        with self.session_maker() as session:
            BrowserSessionStore(session).revoke(token)
            session.commit()

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
            service = ProvisioningService(session)
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
