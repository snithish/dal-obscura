"""HTTP settings request and response contracts."""

from __future__ import annotations

from typing import Any

from pydantic import (
    BaseModel,
    Field,
)

from dal_obscura.interfaces.http.routes.schemas import (
    StrictModel,
)


class RuntimeSettingsRequest(StrictModel):
    """Runtime ticket and scan fan-out settings request.

    Example:
        ```python
        RuntimeSettingsRequest(
            ticket_ttl_seconds=300,
            max_tickets=32,
            max_ticket_exchanges=1,
        )
        ```
    """

    ticket_ttl_seconds: int = Field(gt=0)
    max_tickets: int = Field(gt=0)
    max_ticket_exchanges: int = Field(gt=0)
    path_rules: list[dict[str, object]] = Field(default_factory=list, max_length=256)
    expected_revision: int | None = Field(default=None, ge=0)


class RuntimeSettingsResponse(BaseModel):
    """Authoritative runtime settings returned to management clients."""

    ticket_ttl_seconds: int
    max_tickets: int
    max_ticket_exchanges: int
    path_rules: list[dict[str, str]]
    revision: int


class AuthProviderResponse(BaseModel):
    """Redacted authentication-provider record returned to management clients."""

    id: str
    ordinal: int
    module: str
    args: dict[str, Any]
    enabled: bool
    revision: int


class AuthProviderRevisionResponse(BaseModel):
    revision: int


class IdentityAttributeResponse(BaseModel):
    key: str
    claim_path: str
    label: str
    description: str
    allowed_values: list[str]


class AttributeProviderResponse(BaseModel):
    ordinal: int
    issuer: str
    revision: int
    attributes: list[IdentityAttributeResponse]


class AttributeMappingPreviewRequest(StrictModel):
    claims: dict[str, object] = Field(default_factory=dict)
    provider_args: dict[str, Any] | None = None


class AttributeMappingPreviewResponse(BaseModel):
    attributes: dict[str, str]


class AuthProvidersRequest(StrictModel):
    """Authentication provider-chain replacement request.

    Example:
        ```python
        AuthProvidersRequest(providers=[{"module": "example.Provider", "args": {}}])
        ```
    """

    providers: list[dict[str, Any]] = Field(max_length=16)
    expected_revision: int | None = Field(default=None, ge=0)
