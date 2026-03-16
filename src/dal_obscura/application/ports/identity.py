from __future__ import annotations

from typing import Mapping, Protocol

from dal_obscura.domain.access_control import Principal


class IdentityPort(Protocol):
    """Authenticates a caller from transport-level headers."""

    def authenticate(self, headers: Mapping[str, str]) -> Principal: ...
