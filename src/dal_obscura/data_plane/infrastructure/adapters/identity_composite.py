from __future__ import annotations

from collections.abc import Sequence

from dal_obscura.common.access_control.models import Principal
from dal_obscura.data_plane.application.ports.identity import (
    AuthenticationRequest,
    IdentityPort,
    MissingCredentialsError,
)


class CompositeIdentityProvider:
    """Tries providers in order, falling through only when credentials are absent."""

    def __init__(self, providers: Sequence[IdentityPort]) -> None:
        if not providers:
            raise ValueError("CompositeIdentityProvider requires at least one provider")
        self.providers = list(providers)

    def authenticate(self, request: AuthenticationRequest) -> Principal:
        missing_messages: list[str] = []
        for provider in self.providers:
            try:
                return provider.authenticate(request)
            except MissingCredentialsError as exc:
                missing_messages.append(str(exc))
                continue
        detail = "; ".join(message for message in missing_messages if message)
        raise MissingCredentialsError(detail or "Missing credentials")
