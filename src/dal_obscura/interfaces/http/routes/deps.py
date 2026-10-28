"""Shared FastAPI dependencies for control-plane route modules."""

from __future__ import annotations

import ipaddress
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Annotated, cast
from urllib.parse import urlsplit

from fastapi import Cookie, Depends, HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from dal_obscura.application.governance.assets.service import AssetsCommands
from dal_obscura.application.governance.audit_service import AuditQueries
from dal_obscura.application.governance.catalogs.service import CatalogsCommands
from dal_obscura.application.governance.errors import (
    ConfigurationConflictError,
    RevisionPreconditionRequired,
)
from dal_obscura.application.governance.plugins import PluginQueries
from dal_obscura.application.governance.policies.service import PoliciesCommands
from dal_obscura.application.governance.sessions import SessionCommands
from dal_obscura.application.governance.workspace.service import WorkspaceCommands
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from dal_obscura.kernel.identity.session import OidcNonceActorResolver

AuthorizationCodeExchange = Callable[[Mapping[str, object], str, str], Mapping[str, object]]

_BEARER_AUTH = HTTPBearer(scheme_name="BearerAuth", auto_error=False)


def bearer_authorization(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(_BEARER_AUTH)],
) -> str:
    """Document bearer authentication while preserving cookie fallback semantics.

    A malformed but present Authorization header must not fall back to browser
    credentials. Authentication and error handling remain owned by require_actor.
    """

    del credentials
    return request.headers.get("authorization", "")


class RevisionConflictResponse(HTTPException):
    def __init__(self, failure: ConfigurationConflictError) -> None:
        super().__init__(
            status_code=428 if isinstance(failure, RevisionPreconditionRequired) else 409,
            detail=str(failure),
        )
        self.current_revision = failure.current_revision


@dataclass(frozen=True)
class ControlPlaneDeps:
    """Route dependency bundle for auth checks and service session handling."""

    sessions: SessionCommands
    admin_token: str
    actor_resolver: Callable[[str], ControlPlaneActor | None] | None
    oidc_admin_group: str | None
    ui_auth_config: Mapping[str, object] | None
    catalogs_commands: CatalogsCommands
    workspace_commands: WorkspaceCommands
    audit_queries: AuditQueries
    plugin_queries: PluginQueries
    assets_commands: AssetsCommands
    policies_commands: PoliciesCommands
    allowed_origins: tuple[str, ...] = ()
    oidc_nonce_actor_resolver: OidcNonceActorResolver | None = None
    authorization_code_exchange: AuthorizationCodeExchange | None = None
    bootstrap_enabled: bool = True
    trusted_proxy_peers: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for peer in self.trusted_proxy_peers:
            _parse_proxy_network(peer)

    def require_actor(
        self,
        request: Request,
        authorization: str = Depends(bearer_authorization),
        session_token: str | None = Cookie(default=None, alias="__Host-dal_obscura_session"),
        csrf_cookie: str | None = Cookie(default=None, alias="__Host-dal_obscura_csrf"),
    ) -> ControlPlaneActor:
        """Authenticates bearer clients or an HttpOnly browser session."""
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
                actor = self.sessions.resolve_browser_session(session_token, csrf_cookie)
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
        allowed_origins = self._allowed_browser_origins()
        allowed_hosts = {
            parsed.netloc.lower() for value in allowed_origins if (parsed := urlsplit(value)).netloc
        }
        request_host = _canonical_request_host(request.headers.get("host", ""))
        if allowed_hosts and (request_host is None or request_host not in allowed_hosts):
            raise HTTPException(status_code=403, detail="Origin validation failed")
        origin = request.headers.get("origin", "").strip()
        if origin and _canonical_origin(origin) not in allowed_origins:
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

        if self.actor_resolver is None:
            return None
        return self.actor_resolver(token)

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
        authorization: str = Depends(bearer_authorization),
        session_token: str | None = Cookie(default=None, alias="__Host-dal_obscura_session"),
        csrf_cookie: str | None = Cookie(default=None, alias="__Host-dal_obscura_csrf"),
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


def _bearer_value(authorization: str) -> str | None:
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None


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


def _canonical_request_host(value: str) -> str | None:
    """Normalize a Host header without trusting forwarded host metadata."""

    raw = value.strip()
    if not raw or "," in raw or any(character.isspace() for character in raw):
        return None
    try:
        parsed = urlsplit(f"https://{raw}")
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if not hostname or parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        return None
    host = hostname.lower()
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    default_port = port in (None, 443)
    return f"{host}{'' if default_port else f':{port}'}"


def _parse_proxy_network(value: str) -> ipaddress.IPv4Network | ipaddress.IPv6Network:
    """Parse an operator-configured proxy peer without DNS resolution."""

    text = value.strip()
    try:
        if "/" in text:
            return ipaddress.ip_network(text, strict=False)
        return ipaddress.ip_network(f"{text}/32" if ":" not in text else f"{text}/128")
    except ValueError as exc:
        raise ValueError(f"trusted proxy peer must be an IP address or CIDR: {value!r}") from exc
