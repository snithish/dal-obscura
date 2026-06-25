from __future__ import annotations

from typing import cast

from fastapi import APIRouter, Depends, HTTPException

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import DemoLoginRequest
from dal_obscura.control_plane.interfaces.session_api import (
    actor_response,
    demo_login_config,
    public_ui_auth_config,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    api = APIRouter()

    @api.get("/v1/session")
    def get_session(actor: ControlPlaneActor = Depends(deps.require_actor)) -> object:  # noqa: B008
        return actor_response(actor)

    @api.get("/v1/ui-auth-config")
    def get_ui_auth_config() -> object:
        if deps.ui_auth_config is None:
            raise HTTPException(status_code=404, detail="UI auth is not configured")
        return public_ui_auth_config(deps.ui_auth_config)

    @api.post("/v1/demo-login")
    def demo_login(request: DemoLoginRequest) -> object:
        if deps.ui_auth_config is None:
            raise HTTPException(status_code=404, detail="Demo login is not configured")
        login_config = demo_login_config(deps.ui_auth_config)
        if not login_config:
            raise HTTPException(status_code=404, detail="Demo login is not configured")
        username = request.login_hint.strip()
        passwords = cast(dict[str, str], login_config["passwords"])
        if username not in passwords:
            raise HTTPException(status_code=404, detail="Demo persona is not configured")
        return {"access_token": deps.demo_token_exchange(login_config, username)}

    return api
