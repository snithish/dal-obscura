from __future__ import annotations

from typing import Mapping, Protocol

from dal_obscura.domain.access_control import Principal


class IdentityPort(Protocol):
    def authenticate(self, headers: Mapping[str, str]) -> Principal: ...
