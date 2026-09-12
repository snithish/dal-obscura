"""Session and UI-auth routes for the control-plane API.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

import base64
import hashlib
import secrets
from typing import cast
from urllib.parse import urlencode, urlsplit, urlunsplit

from fastapi import APIRouter, Cookie, Depends, HTTPException, Response
from fastapi.responses import RedirectResponse

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import DemoLoginRequest
from dal_obscura.control_plane.interfaces.session_api import (
    actor_response,
    demo_login_config,
    public_ui_auth_config,
)


def router(deps: ControlPlaneDeps) -> APIRouter:  # noqa: C901
    """Builds session, UI auth config, and demo-login routes.

    Example:
        ```python
        session_router = router(deps)
        ```
    """

    api = APIRouter()

    @api.get("/auth/login")
    def auth_login() -> Response:
        config = _required_ui_config(deps)
        redirect_uri = _required_config_value(config, "redirect_uri")
        state = secrets.token_urlsafe(32)
        nonce = secrets.token_urlsafe(32)
        code_verifier = secrets.token_urlsafe(48)
        deps.issue_login_transaction(
            state=state,
            nonce=nonce,
            code_verifier=code_verifier,
            redirect_uri=redirect_uri,
        )
        authorization_url = _oidc_endpoint(
            config,
            "authorization_endpoint",
            "/protocol/openid-connect/auth",
        )
        query = urlencode(
            {
                "response_type": "code",
                "client_id": _required_config_value(config, "client_id"),
                "redirect_uri": redirect_uri,
                "scope": str(config.get("scope", "openid profile")),
                "state": state,
                "nonce": nonce,
                "code_challenge": _code_challenge(code_verifier),
                "code_challenge_method": "S256",
            }
        )
        response = RedirectResponse(f"{authorization_url}?{query}", status_code=303)
        response.headers["cache-control"] = "no-store"
        response.set_cookie(
            key="dal_obscura_auth_state",
            value=state,
            httponly=True,
            secure=_secure_cookie(config),
            samesite="lax",
            max_age=600,
            path="/",
        )
        return response

    @api.get("/auth/callback")
    def auth_callback(
        code: str | None = None,
        state: str | None = None,
        error: str | None = None,
        auth_state: str | None = Cookie(default=None, alias="dal_obscura_auth_state"),
    ) -> Response:
        config = _required_ui_config(deps)
        if error:
            raise HTTPException(status_code=401, detail="OIDC login was not completed")
        if not code or not state or not auth_state or not secrets.compare_digest(state, auth_state):
            raise HTTPException(status_code=400, detail="Invalid OIDC login state")
        transaction = deps.consume_login_transaction(state)
        if transaction is None:
            raise HTTPException(status_code=400, detail="Expired or already used OIDC login state")
        redirect_uri = _required_config_value(config, "redirect_uri")
        if transaction.redirect_uri != redirect_uri:
            raise HTTPException(status_code=400, detail="OIDC redirect URI changed during login")
        exchange = deps.authorization_code_exchange
        if exchange is None:
            raise HTTPException(status_code=503, detail="OIDC code exchange is not configured")
        tokens = exchange(config, code, transaction.code_verifier)
        id_token = str(tokens.get("id_token", "")).strip()
        actor = deps.resolve_nonce_token(id_token, transaction.nonce_hash)
        if actor is None:
            raise HTTPException(status_code=401, detail="OIDC ID token was rejected")
        session_token, csrf_token = deps.issue_browser_session_credentials(actor)
        result = RedirectResponse(_post_login_redirect(config, redirect_uri), status_code=303)
        result.headers["cache-control"] = "no-store"
        result.set_cookie(
            key="dal_obscura_session",
            value=session_token,
            httponly=True,
            secure=_secure_cookie(config),
            samesite="lax",
            path="/",
        )
        result.set_cookie(
            key="dal_obscura_csrf",
            value=csrf_token,
            httponly=False,
            secure=_secure_cookie(config),
            samesite="lax",
            path="/",
        )
        result.delete_cookie(key="dal_obscura_auth_state", path="/")
        return result

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
        provider_token = deps.demo_token_exchange(login_config, username)
        actor = deps.resolve_bearer_token(provider_token)
        if actor is None:
            raise HTTPException(status_code=401, detail="Demo identity provider token rejected")
        session_token, csrf_token = deps.issue_browser_session_credentials(actor)
        secure = str(deps.ui_auth_config.get("redirect_uri", "")).startswith("https://")
        response.set_cookie(
            key="dal_obscura_session",
            value=session_token,
            httponly=True,
            secure=secure,
            samesite="lax",
            path="/",
        )
        response.set_cookie(
            key="dal_obscura_csrf",
            value=csrf_token,
            httponly=False,
            secure=secure,
            samesite="lax",
            path="/",
        )
        return {"authenticated": True}

    @api.post("/v1/logout")
    def logout(
        response: Response,
        session_token: str | None = Cookie(default=None, alias="dal_obscura_session"),
        _actor: ControlPlaneActor = Depends(deps.require_actor),  # noqa: B008
    ) -> object:
        """Expires the browser session after the shared CSRF check."""
        if session_token:
            deps.revoke_browser_session(session_token)
        response.delete_cookie(key="dal_obscura_session", path="/")
        response.delete_cookie(key="dal_obscura_csrf", path="/")
        return {"authenticated": False}

    return api


def _required_ui_config(deps: ControlPlaneDeps) -> dict[str, object]:
    if deps.ui_auth_config is None:
        raise HTTPException(status_code=404, detail="UI auth is not configured")
    return dict(deps.ui_auth_config)


def _required_config_value(config: dict[str, object], key: str) -> str:
    value = str(config.get(key, "")).strip()
    if not value:
        raise HTTPException(status_code=503, detail=f"UI auth is missing {key}")
    return value


def _oidc_endpoint(config: dict[str, object], key: str, suffix: str) -> str:
    configured = str(config.get(key, "")).strip()
    if configured:
        return configured
    authority = _required_config_value(config, "authority").rstrip("/")
    return authority + suffix


def _code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _secure_cookie(config: dict[str, object]) -> bool:
    return str(config.get("redirect_uri", "")).startswith("https://")


def _post_login_redirect(config: dict[str, object], redirect_uri: str) -> str:
    configured = str(config.get("post_login_redirect_uri", "")).strip()
    if configured:
        return configured
    fallback = str(config.get("post_logout_redirect_uri", "")).strip()
    if fallback:
        return fallback
    parsed = urlsplit(redirect_uri)
    return urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))
