from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from dal_obscura.domain.access_control.models import AccessDecision, Principal
from dal_obscura.domain.query_planning.models import DatasetSelector


class AuthorizationPort(Protocol):
    """Resolves which columns, masks, and row filters a principal may use."""

    def authorize(
        self,
        principal: Principal,
        dataset: DatasetSelector,
        requested_columns: Iterable[str],
    ) -> AccessDecision: ...

    def current_policy_version(self, dataset: DatasetSelector) -> int | None: ...
