"""Session and UI-auth routes for the control-plane API.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

import secrets
from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Response

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import DemoLoginRequest
from dal_obscura.control_plane.interfaces.session_api import (
    actor_response,
    demo_login_config,
    public_ui_auth_config,
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds session, UI auth config, and demo-login routes.

    Example:
        ```python
        session_router = router(deps)
        ```
    """

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
    def demo_login(request: DemoLoginRequest, response: Response) -> object:
        if deps.ui_auth_config is None:
            raise HTTPException(status_code=404, detail="Demo login is not configured")
        login_config = demo_login_config(deps.ui_auth_config)
        if not login_config:
            raise HTTPException(status_code=404, detail="Demo login is not configured")
        username = request.login_hint.strip()
        passwords = cast(dict[str, str], login_config["passwords"])
        if username not in passwords:
            raise HTTPException(status_code=404, detail="Demo persona is not configured")
        token = deps.demo_token_exchange(login_config, username)
        secure = str(deps.ui_auth_config.get("redirect_uri", "")).startswith("https://")
        response.set_cookie(
            key="dal_obscura_session",
            value=token,
            httponly=True,
            secure=secure,
            samesite="lax",
            path="/",
        )
        response.set_cookie(
            key="dal_obscura_csrf",
            value=secrets.token_urlsafe(32),
            httponly=False,
            secure=secure,
            samesite="lax",
            path="/",
        )
        return {"authenticated": True}

    @api.post("/v1/logout")
    def logout(
        response: Response,
        _actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        """Expires the browser session after the shared CSRF check."""
        response.delete_cookie(key="dal_obscura_session", path="/")
        response.delete_cookie(key="dal_obscura_csrf", path="/")
        return {"authenticated": False}

    return api
