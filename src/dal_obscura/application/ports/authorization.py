from __future__ import annotations

from typing import Iterable, Protocol

from dal_obscura.domain.access_control import AccessDecision, Principal
from dal_obscura.domain.query_planning import DatasetSelector


class AuthorizationPort(Protocol):
    """Resolves which columns, masks, and row filters a principal may use."""

    def authorize(
        self,
        principal: Principal,
        dataset: DatasetSelector,
        requested_columns: Iterable[str],
    ) -> AccessDecision: ...

    def current_policy_version(self, dataset: DatasetSelector) -> int | None: ...
