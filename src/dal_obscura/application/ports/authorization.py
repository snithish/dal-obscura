from __future__ import annotations

from typing import Iterable, Protocol

from dal_obscura.domain.access_control import AccessDecision, Principal


class AuthorizationPort(Protocol):
    def authorize(
        self,
        principal: Principal,
        table_identifier: str,
        requested_columns: Iterable[str],
    ) -> AccessDecision: ...

    def current_policy_version(self, table_identifier: str) -> int | None: ...
