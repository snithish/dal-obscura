from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from dal_obscura.common.access_control.models import Principal


@dataclass(frozen=True)
class AuthenticationRequest:
    """Credential-bearing request context passed to identity providers."""

    headers: dict[str, str] = field(default_factory=dict)
    peer_identity: str = ""
    peer: str = ""
    method: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "headers",
            {
                str(key).lower(): str(value)
                for key, value in dict(self.headers).items()
                if str(key).strip()
            },
        )

    def header(self, name: str) -> str | None:
        return self.headers.get(str(name).lower())


class AuthenticationError(PermissionError):
    """Base class for authentication failures."""


class MissingCredentialsError(AuthenticationError):
    """No credential for this provider was present on the request."""


class InvalidCredentialsError(AuthenticationError):
    """A credential for this provider was present but failed validation."""


class AuthenticationUnavailableError(AuthenticationError):
    """The provider could not validate credentials because a dependency failed."""


class IdentityPort(Protocol):
    """Authenticates a caller from normalized request context."""

    def authenticate(self, request: AuthenticationRequest) -> Principal: ...
