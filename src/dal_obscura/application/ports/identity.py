from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Protocol

from dal_obscura.domain.access_control.models import Principal


@dataclass(frozen=True)
class AuthenticationRequest(Mapping[str, str]):
    """Credential-bearing request context passed to identity providers.

    The class behaves like a normalized header mapping for backwards
    compatibility with existing providers while exposing transport metadata for
    mTLS and gateway-aware authentication.
    """

    headers: Mapping[str, str] = field(default_factory=dict)
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

    def __getitem__(self, key: str) -> str:
        return self.headers[str(key).lower()]

    def __iter__(self) -> Iterator[str]:
        return iter(self.headers)

    def __len__(self) -> int:
        return len(self.headers)


AuthenticationInput = AuthenticationRequest | Mapping[str, str]


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

    def authenticate(self, request: AuthenticationInput) -> Principal: ...


def coerce_authentication_request(
    request: AuthenticationInput,
    *,
    peer_identity: str = "",
    peer: str = "",
    method: str = "",
) -> AuthenticationRequest:
    """Returns an `AuthenticationRequest` while preserving existing context."""
    if isinstance(request, AuthenticationRequest):
        if not peer_identity and not peer and not method:
            return request
        return AuthenticationRequest(
            headers=request.headers,
            peer_identity=peer_identity or request.peer_identity,
            peer=peer or request.peer,
            method=method or request.method,
        )
    return AuthenticationRequest(
        headers=request,
        peer_identity=peer_identity,
        peer=peer,
        method=method,
    )
