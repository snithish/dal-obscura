"""HTTP session request and response contracts."""

from __future__ import annotations

from pydantic import (
    BaseModel,
)


class SessionResponse(BaseModel):
    """Safe authenticated actor metadata for browser and API clients."""

    principal: str
    groups: list[str]
    platform_admin: bool
    capabilities: list[str]
    issuer: str | None = None


class UiAuthConfigResponse(BaseModel):
    """Browser-safe OIDC settings; secrets never appear here."""

    authority: str | None = None
    client_id: str | None = None
    redirect_uri: str | None = None
    post_logout_redirect_uri: str | None = None
    scope: str | None = None


class SessionOptionsResponse(BaseModel):
    """Available browser authentication methods."""

    bootstrap_enabled: bool
    oidc: UiAuthConfigResponse | None = None


class AuthenticationMutationResponse(BaseModel):
    """Result of a browser session login or logout mutation."""

    authenticated: bool


class LogoutResponse(AuthenticationMutationResponse):
    """Local session revocation and optional provider sign-out destination."""

    logout_url: str | None = None
