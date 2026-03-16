from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from dal_obscura.domain.access_control.models import Principal


class IdentityPort(Protocol):
    """Authenticates a caller from transport-level headers."""

    def authenticate(self, headers: Mapping[str, str]) -> Principal: ...
