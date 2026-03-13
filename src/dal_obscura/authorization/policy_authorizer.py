from __future__ import annotations

from pathlib import Path
from typing import Iterable

from dal_obscura.policy.loader import load_policy
from dal_obscura.policy.models import Policy, Principal
from dal_obscura.policy.resolver import resolve_access

from .base import Authorizer
from .types import AccessDecision


class PolicyAuthorizer(Authorizer):
    def __init__(self, policy_path: str | Path) -> None:
        self._policy_path = Path(policy_path)

    def _load_policy(self) -> Policy:
        return load_policy(self._policy_path)

    def authorize(
        self,
        principal: Principal,
        table_identifier: str,
        requested_columns: Iterable[str],
    ) -> AccessDecision:
        policy = self._load_policy()
        allowed_columns, masks, row_filter = resolve_access(
            policy,
            principal,
            table_identifier,
            requested_columns,
        )
        return AccessDecision(
            allowed_columns=allowed_columns,
            masks=masks,
            row_filter=row_filter,
            policy_version=policy.version,
        )

    def current_policy_version(self) -> int:
        return self._load_policy().version
