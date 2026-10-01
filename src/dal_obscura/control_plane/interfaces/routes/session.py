"""Session and UI-auth routes for the control-plane API.

Example:
    ```python
    app.include_router(router(deps))
    ```
"""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import secrets
from typing import NoReturn
from urllib.parse import urlencode, urlsplit, urlunsplit

from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, Response
from fastapi.responses import RedirectResponse

from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps, bearer_authorization
from dal_obscura.control_plane.interfaces.routes.schemas import (
    AuthenticationMutationResponse,
    SessionOptionsResponse,
    SessionResponse,
    UiAuthConfigResponse,
)
from dal_obscura.control_plane.interfaces.session_api import (
    actor_response,
    public_ui_auth_config,
)


def router(deps: ControlPlaneDeps) -> APIRouter:  # noqa: C901
    """Builds session and UI authentication routes.

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
            key="__Host-dal_obscura_auth_state",
            value=state,
            httponly=True,
            secure=True,
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
        auth_state: str | None = Cookie(default=None, alias="__Host-dal_obscura_auth_state"),
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
        client_key, _aggregate_key = _login_rate_keys(deps, request)
        deps.clear_login_rate_limit(client_key)
        result = RedirectResponse(_post_login_redirect(config, redirect_uri), status_code=303)
        result.headers["cache-control"] = "no-store"
        result.set_cookie(
            key="__Host-dal_obscura_session",
            value=session_token,
            httponly=True,
            secure=True,
            samesite="lax",
            path="/",
        )
        result.set_cookie(
            key="__Host-dal_obscura_csrf",
            value=csrf_token,
            httponly=False,
            secure=True,
            samesite="lax",
            path="/",
        )
        result.delete_cookie(key="__Host-dal_obscura_auth_state", path="/", secure=True)
        return result

    @api.get("/v1/session", response_model=SessionResponse, response_model_exclude_none=True)
    def get_session(actor: ControlPlaneActor = Depends(deps.require_actor)) -> SessionResponse:  # noqa: B008
        return SessionResponse.model_validate(actor_response(actor))

    @api.get(
        "/v1/session/options",
        response_model=SessionOptionsResponse,
    )
    def get_session_options() -> SessionOptionsResponse:
        """Returns browser-safe login methods for the current deployment.

        The local bootstrap flag is deliberately exposed as capability metadata
        only. The credential itself is never returned to the browser and the
        production profile disables this method at startup.
        """

        return SessionOptionsResponse.model_validate(
            {
                "bootstrap_enabled": deps.bootstrap_enabled,
                "oidc": (
                    public_ui_auth_config(deps.ui_auth_config)
                    if deps.ui_auth_config is not None
                    else None
                ),
            }
        )

    @api.post("/v1/session/bootstrap", response_model=AuthenticationMutationResponse)
    def bootstrap_session(
        request: Request,
        response: Response,
        authorization: str = Depends(bearer_authorization),
    ) -> AuthenticationMutationResponse:
        """Exchanges the local admin bearer secret for a browser session.

        This route is a local-development bridge only. It requires the exact
        configured bootstrap secret, is rate limited, and mints the same
        server-side HttpOnly session plus CSRF cookie used by OIDC callbacks.
        """

        if not deps.bootstrap_enabled:
            raise HTTPException(status_code=404, detail="Local bootstrap login is disabled")
        _enforce_login_rate_limit(deps, request)
        if authorization != f"Bearer {deps.admin_token}":
            client_key, aggregate_key = _login_rate_keys(deps, request)
            decision = deps.record_login_failure(client_key, aggregate_key=aggregate_key)
            if not decision.allowed:
                raise HTTPException(
                    status_code=429,
                    detail="Login temporarily unavailable",
                    headers={"Retry-After": str(decision.retry_after_seconds)},
                )
            raise HTTPException(status_code=401, detail="Invalid bootstrap credential")
        actor = ControlPlaneActor.for_platform_admin("platform:admin")
        session_token, csrf_token = deps.issue_browser_session_credentials(actor)
        client_key, _aggregate_key = _login_rate_keys(deps, request)
        deps.clear_login_rate_limit(client_key)
        response.set_cookie(
            key="__Host-dal_obscura_session",
            value=session_token,
            httponly=True,
            secure=True,
            samesite="lax",
            path="/",
        )
        response.set_cookie(
            key="__Host-dal_obscura_csrf",
            value=csrf_token,
            httponly=False,
            secure=True,
            samesite="lax",
            path="/",
        )
        return AuthenticationMutationResponse(authenticated=True)

    @api.get(
        "/v1/ui-auth-config",
        response_model=UiAuthConfigResponse,
        response_model_exclude_none=True,
    )
    def get_ui_auth_config() -> UiAuthConfigResponse:
        if deps.ui_auth_config is None:
            raise HTTPException(status_code=404, detail="UI auth is not configured")
        return UiAuthConfigResponse.model_validate(public_ui_auth_config(deps.ui_auth_config))

    @api.post("/v1/logout", response_model=AuthenticationMutationResponse)
    def logout(
        request: Request,
        response: Response,
        session_token: str | None = Cookie(default=None, alias="__Host-dal_obscura_session"),
        csrf_cookie: str | None = Cookie(default=None, alias="__Host-dal_obscura_csrf"),
    ) -> AuthenticationMutationResponse:
        """Expires browser credentials even when the server session is stale."""
        if session_token:
            deps.validate_browser_mutation(request, csrf_cookie)
            deps.revoke_browser_session(session_token)
        response.delete_cookie(key="__Host-dal_obscura_session", path="/", secure=True)
        response.delete_cookie(key="__Host-dal_obscura_csrf", path="/", secure=True)
        return AuthenticationMutationResponse(authenticated=False)

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
    endpoint = configured
    if not endpoint:
        authority = _required_config_value(config, "authority").rstrip("/")
        endpoint = authority + suffix
    try:
        parsed = urlsplit(endpoint)
        hostname = parsed.hostname
        _ = parsed.port
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=f"OIDC {key} is invalid") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise HTTPException(status_code=503, detail=f"OIDC {key} is invalid")
    return endpoint


def _code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _post_login_redirect(config: dict[str, object], redirect_uri: str) -> str:
    configured = str(config.get("post_login_redirect_uri", "")).strip()
    try:
        callback = urlsplit(redirect_uri)
        callback_hostname = callback.hostname
        _ = callback.port
    except ValueError as exc:
        raise HTTPException(status_code=503, detail="UI redirect URI is invalid") from exc
    if not callback.scheme or not callback.netloc or not callback_hostname:
        raise HTTPException(status_code=503, detail="UI redirect URI is invalid")
    if configured:
        try:
            target = urlsplit(configured)
            target_hostname = target.hostname
            _ = target.port
        except ValueError as exc:
            raise HTTPException(
                status_code=503,
                detail="UI post-login redirect is invalid",
            ) from exc
        if (
            target.scheme.lower() != callback.scheme.lower()
            or target.netloc.lower() != callback.netloc.lower()
            or not target_hostname
            or target.username is not None
            or target.password is not None
            or target.query
            or target.fragment
            or not target.netloc
        ):
            raise HTTPException(status_code=503, detail="UI post-login redirect is invalid")
        return configured
    return urlunsplit((callback.scheme, callback.netloc, "/", "", ""))


def _rate_key_for_request(
    request: Request,
    trusted_proxy_peers: tuple[str, ...],
) -> tuple[str, str | None]:
    """Return client and optional aggregate keys under an explicit proxy contract."""

    client = request.client
    direct_peer = client.host if client is not None and client.host else "unknown"
    client_address = direct_peer
    aggregate_key = None
    if _peer_matches_networks(direct_peer, trusted_proxy_peers):
        forwarded = request.headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
        if _is_ip_address(forwarded):
            client_address = forwarded
            aggregate_key = f"aggregate:{direct_peer}"
    return f"client:{client_address}", aggregate_key


def _login_rate_keys(deps: ControlPlaneDeps, request: Request) -> tuple[str, str | None]:
    return _rate_key_for_request(request, deps.trusted_proxy_peers)


def _peer_matches_networks(peer: str, configured: tuple[str, ...]) -> bool:
    if not configured or not _is_ip_address(peer):
        return False
    address = ipaddress.ip_address(peer)
    for value in configured:
        try:
            network = ipaddress.ip_network(value, strict=False)
        except ValueError:
            continue
        if address in network:
            return True
    return False


def _is_ip_address(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


def _enforce_login_rate_limit(deps: ControlPlaneDeps, request: Request) -> None:
    client_key, aggregate_key = _login_rate_keys(deps, request)
    decision = deps.check_login_rate_limit(client_key, aggregate_key=aggregate_key)
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
    client_key, aggregate_key = _login_rate_keys(deps, request)
    decision = deps.record_login_failure(client_key, aggregate_key=aggregate_key)
    if not decision.allowed:
        raise HTTPException(
            status_code=429,
            detail="Login temporarily unavailable",
            headers={"Retry-After": str(decision.retry_after_seconds)},
        )
    raise HTTPException(status_code=status_code, detail=detail)
