from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from dal_obscura.policy.models import AccessDecision, Principal


class AuthorizationPort(Protocol):
    """Resolves which columns, masks, and row filters a principal may use."""

    def authorize(
        self,
        principal: Principal,
        target: str,
        catalog: str | None,
        requested_columns: Iterable[str],
    ) -> AccessDecision: ...
