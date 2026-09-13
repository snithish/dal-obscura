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
from collections.abc import Mapping
from typing import NoReturn, cast
from urllib.parse import urlencode, urlsplit, urlunsplit

from fastapi import APIRouter, Cookie, Depends, Header, HTTPException, Request, Response
from fastapi.responses import RedirectResponse

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps
from dal_obscura.control_plane.interfaces.routes.schemas import DemoLoginRequest, SessionResponse
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
    def auth_login(request: Request) -> Response:
        config = _required_ui_config(deps)
        _enforce_login_rate_limit(deps, request)
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
        request: Request,
        code: str | None = None,
        state: str | None = None,
        error: str | None = None,
        auth_state: str | None = Cookie(default=None, alias="dal_obscura_auth_state"),
    ) -> Response:
        auth_state = _cookie_text(auth_state)
        config = _required_ui_config(deps)
        if error:
            _callback_failure(deps, request, status_code=401, detail="OIDC login was not completed")
        if not code or not state or not auth_state or not secrets.compare_digest(state, auth_state):
            _callback_failure(deps, request, status_code=400, detail="Invalid OIDC login state")
        transaction = deps.consume_login_transaction(state)
        if transaction is None:
            _callback_failure(
                deps,
                request,
                status_code=400,
                detail="Expired or already used OIDC login state",
            )
        redirect_uri = _required_config_value(config, "redirect_uri")
        if transaction.redirect_uri != redirect_uri:
            _callback_failure(
                deps,
                request,
                status_code=400,
                detail="OIDC redirect URI changed during login",
            )
        exchange = deps.authorization_code_exchange
        if exchange is None:
            raise HTTPException(status_code=503, detail="OIDC code exchange is not configured")
        try:
            tokens = exchange(config, code, transaction.code_verifier)
        except HTTPException:
            _callback_failure(deps, request, status_code=502, detail="OIDC code exchange failed")
        except Exception:
            _callback_failure(deps, request, status_code=502, detail="OIDC code exchange failed")
        id_token = str(tokens.get("id_token", "")).strip()
        actor = deps.resolve_nonce_token(id_token, transaction.nonce_hash)
        if actor is None:
            _callback_failure(deps, request, status_code=401, detail="OIDC ID token was rejected")
        session_token, csrf_token = deps.issue_browser_session_credentials(actor)
        deps.clear_login_rate_limit(_client_rate_key(request))
        result = RedirectResponse(_post_login_redirect(config, redirect_uri), status_code=303)
        result.headers["cache-control"] = "no-store"
        session_cookie, csrf_cookie = _browser_cookie_names(config)
        result.set_cookie(
            key=session_cookie,
            value=session_token,
            httponly=True,
            secure=_secure_cookie(config),
            samesite="lax",
            path="/",
        )
        result.set_cookie(
            key=csrf_cookie,
            value=csrf_token,
            httponly=False,
            secure=_secure_cookie(config),
            samesite="lax",
            path="/",
        )
        result.delete_cookie(key="dal_obscura_auth_state", path="/")
        return result

    @api.get("/v1/session", response_model=SessionResponse, response_model_exclude_none=True)
    def get_session(actor: ControlPlaneActor = Depends(deps.require_actor)) -> SessionResponse:  # noqa: B008
        return SessionResponse.model_validate(actor_response(actor))

    @api.get("/v1/session/options")
    def get_session_options() -> object:
        """Returns browser-safe login methods for the current deployment.

        The local bootstrap flag is deliberately exposed as capability metadata
        only. The credential itself is never returned to the browser and the
        production profile disables this method at startup.
        """

        return {
            "bootstrap_enabled": deps.bootstrap_enabled,
            "oidc": (
                public_ui_auth_config(deps.ui_auth_config)
                if deps.ui_auth_config is not None
                else None
            ),
        }

    @api.post("/v1/session/bootstrap")
    def bootstrap_session(
        request: Request,
        response: Response,
        authorization: str = Header(default=""),
    ) -> object:
        """Exchanges the local admin bearer secret for a browser session.

        This route is a local-development bridge only. It requires the exact
        configured bootstrap secret, is rate limited, and mints the same
        server-side HttpOnly session plus CSRF cookie used by OIDC callbacks.
        """

        if not deps.bootstrap_enabled:
            raise HTTPException(status_code=404, detail="Local bootstrap login is disabled")
        _enforce_login_rate_limit(deps, request)
        if authorization != f"Bearer {deps.admin_token}":
            decision = deps.record_login_failure(_client_rate_key(request))
            if not decision.allowed:
                raise HTTPException(
                    status_code=429,
                    detail="Login temporarily unavailable",
                    headers={"Retry-After": str(decision.retry_after_seconds)},
                )
            raise HTTPException(status_code=401, detail="Invalid bootstrap credential")
        actor = ControlPlaneActor.for_platform_admin("platform:admin")
        session_token, csrf_token = deps.issue_browser_session_credentials(actor)
        deps.clear_login_rate_limit(_client_rate_key(request))
        config = dict(deps.ui_auth_config or {})
        session_cookie, csrf_cookie = _browser_cookie_names(config)
        secure = _secure_cookie(config)
        response.set_cookie(
            key=session_cookie,
            value=session_token,
            httponly=True,
            secure=secure,
            samesite="lax",
            path="/",
        )
        response.set_cookie(
            key=csrf_cookie,
            value=csrf_token,
            httponly=False,
            secure=secure,
            samesite="lax",
            path="/",
        )
        return {"authenticated": True}

    @api.get("/v1/ui-auth-config")
    def get_ui_auth_config() -> object:
        if deps.ui_auth_config is None:
            raise HTTPException(status_code=404, detail="UI auth is not configured")
        return public_ui_auth_config(deps.ui_auth_config)

    @api.post("/v1/demo-login")
    def demo_login(request: DemoLoginRequest, response: Response, http_request: Request) -> object:
        if deps.ui_auth_config is None:
            raise HTTPException(status_code=404, detail="Demo login is not configured")
        _enforce_login_rate_limit(deps, http_request)
        login_config = demo_login_config(deps.ui_auth_config)
        if not login_config:
            raise HTTPException(status_code=404, detail="Demo login is not configured")
        username = request.login_hint.strip()
        passwords = cast(dict[str, str], login_config["passwords"])
        if username not in passwords:
            raise HTTPException(status_code=404, detail="Demo persona is not configured")
        try:
            provider_token = deps.demo_token_exchange(login_config, username)
        except HTTPException:
            _callback_failure(deps, http_request, status_code=502, detail="Demo login failed")
        except Exception:
            _callback_failure(deps, http_request, status_code=502, detail="Demo login failed")
        actor = deps.resolve_bearer_token(provider_token)
        if actor is None:
            _callback_failure(
                deps,
                http_request,
                status_code=401,
                detail="Demo identity provider token rejected",
            )
        session_token, csrf_token = deps.issue_browser_session_credentials(actor)
        deps.clear_login_rate_limit(_client_rate_key(http_request))
        secure = str(deps.ui_auth_config.get("redirect_uri", "")).startswith("https://")
        session_cookie, csrf_cookie = _browser_cookie_names(deps.ui_auth_config)
        response.set_cookie(
            key=session_cookie,
            value=session_token,
            httponly=True,
            secure=secure,
            samesite="lax",
            path="/",
        )
        response.set_cookie(
            key=csrf_cookie,
            value=csrf_token,
            httponly=False,
            secure=secure,
            samesite="lax",
            path="/",
        )
        return {"authenticated": True}

    @api.post("/v1/logout")
    def logout(
        request: Request,
        response: Response,
        session_token: str | None = Cookie(default=None, alias="dal_obscura_session"),
        csrf_cookie: str | None = Cookie(default=None, alias="dal_obscura_csrf"),
        host_session_token: str | None = Cookie(default=None, alias="__Host-dal_obscura_session"),
        host_csrf_cookie: str | None = Cookie(default=None, alias="__Host-dal_obscura_csrf"),
    ) -> object:
        """Expires browser credentials even when the server session is stale."""
        session_token = _cookie_text(host_session_token) or _cookie_text(session_token)
        csrf_cookie = _cookie_text(host_csrf_cookie) or _cookie_text(csrf_cookie)
        if session_token:
            deps.validate_browser_mutation(request, csrf_cookie)
            deps.revoke_browser_session(session_token)
        for cookie_name in ("dal_obscura_session", "__Host-dal_obscura_session"):
            response.delete_cookie(key=cookie_name, path="/")
        for cookie_name in ("dal_obscura_csrf", "__Host-dal_obscura_csrf"):
            response.delete_cookie(key=cookie_name, path="/")
        return {"authenticated": False}

    return api


def _required_ui_config(deps: ControlPlaneDeps) -> dict[str, object]:
    if deps.ui_auth_config is None:
        raise HTTPException(status_code=404, detail="UI auth is not configured")
    return dict(deps.ui_auth_config)


def _cookie_text(value: object) -> str | None:
    """Return a cookie value only when FastAPI supplied a real string."""

    if isinstance(value, str):
        return value or None
    return None


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


def _browser_cookie_names(config: Mapping[str, object]) -> tuple[str, str]:
    if _secure_cookie(dict(config)):
        return "__Host-dal_obscura_session", "__Host-dal_obscura_csrf"
    return "dal_obscura_session", "dal_obscura_csrf"


def _post_login_redirect(config: dict[str, object], redirect_uri: str) -> str:
    configured = str(config.get("post_login_redirect_uri", "")).strip()
    if configured:
        return configured
    parsed = urlsplit(redirect_uri)
    return urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))


def _client_rate_key(request: Request) -> str:
    """Returns a direct connection key; forwarded headers are never trusted."""

    client = request.client
    return client.host if client is not None and client.host else "unknown"


def _enforce_login_rate_limit(deps: ControlPlaneDeps, request: Request) -> None:
    decision = deps.check_login_rate_limit(_client_rate_key(request))
    if decision.allowed:
        return
    raise HTTPException(
        status_code=429,
        detail="Login temporarily unavailable",
        headers={"Retry-After": str(decision.retry_after_seconds)},
    )


def _callback_failure(
    deps: ControlPlaneDeps,
    request: Request,
    *,
    status_code: int,
    detail: str,
) -> NoReturn:
    decision = deps.record_login_failure(_client_rate_key(request))
    if not decision.allowed:
        raise HTTPException(
            status_code=429,
            detail="Login temporarily unavailable",
            headers={"Retry-After": str(decision.retry_after_seconds)},
        )
    raise HTTPException(status_code=status_code, detail=detail)
