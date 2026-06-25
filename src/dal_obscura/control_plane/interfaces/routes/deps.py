from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from fastapi import Header, HTTPException
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.application.errors import AuthorizationFailure, ValidationFailure
from dal_obscura.control_plane.application.provisioning import ProvisioningService
from dal_obscura.control_plane.interfaces.session_api import (
    OidcActorResolver,
    oidc_actor_from_header,
)

DemoTokenExchange = Callable[[Mapping[str, object], str], str]


@dataclass(frozen=True)
class ControlPlaneDeps:
    session_maker: sessionmaker[Session]
    admin_token: str
    oidc_actor_resolver: OidcActorResolver | None
    oidc_admin_group: str | None
    ui_auth_config: Mapping[str, object] | None
    demo_token_exchange: DemoTokenExchange

    def require_actor(self, authorization: str = Header(default="")) -> ControlPlaneActor:
        expected = f"Bearer {self.admin_token}"
        if authorization == expected:
            return ControlPlaneActor.for_platform_admin("platform:admin")
        if self.oidc_actor_resolver is None:
            raise HTTPException(status_code=401, detail="Unauthorized")
        actor = oidc_actor_from_header(
            authorization,
            resolver=self.oidc_actor_resolver,
            admin_group=self.oidc_admin_group,
        )
        if actor is None:
            raise HTTPException(status_code=401, detail="Unauthorized")
        return actor

    def require_admin(self, authorization: str = Header(default="")) -> ControlPlaneActor:
        actor = self.require_actor(authorization)
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
