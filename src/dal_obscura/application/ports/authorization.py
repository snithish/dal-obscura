from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from dal_obscura.domain.access_control.models import AccessDecision, Principal


class AuthorizationPort(Protocol):
    """Resolves which columns, masks, and row filters a principal may use."""

    def authorize(
        self,
        principal: Principal,
        target: str,
        catalog: str | None,
        requested_columns: Iterable[str],
    ) -> AccessDecision: ...

    def current_policy_version(
        self,
        target: str,
        catalog: str | None,
        *,
        tenant_id: str,
    ) -> int | None: ...
