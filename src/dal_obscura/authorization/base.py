from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Optional

from dal_obscura.policy.models import Principal

from .types import AccessDecision


class Authorizer(ABC):
    @abstractmethod
    def authorize(
        self,
        principal: Principal,
        table_identifier: str,
        requested_columns: Iterable[str],
    ) -> AccessDecision: ...

    def current_policy_version(self, table_identifier: str) -> Optional[int]:
        return None
