from __future__ import annotations

from collections.abc import Mapping

from fastapi import FastAPI
from sqlalchemy.orm import Session, sessionmaker

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
    exchange_demo_password_token,
)
from dal_obscura.control_plane.interfaces.ui import install_ui
from dal_obscura.data_plane.application.ports.identity import AuthenticationRequest
from dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks import (
    OidcJwksIdentityProvider,
)


def create_oidc_actor_resolver(
    *,
    issuer: str,
    audience: str | None,
    jwks_url: str | None,
    subject_claim: str,
    group_claims: tuple[str, ...],
) -> OidcActorResolver:
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


_exchange_demo_password_token = exchange_demo_password_token


def create_app(
    session_maker: sessionmaker[Session],
    *,
    admin_token: str,
    oidc_actor_resolver: OidcActorResolver | None = None,
    oidc_admin_group: str | None = None,
    ui_auth_config: Mapping[str, object] | None = None,
) -> FastAPI:
    app = FastAPI(title="dal-obscura control plane")
    install_ui(app)
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
