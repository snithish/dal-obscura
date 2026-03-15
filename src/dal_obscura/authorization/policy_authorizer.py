from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable

from dal_obscura.policy.loader import load_policy
from dal_obscura.policy.models import DatasetPolicy, Policy, Principal
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
        dataset = policy.match_dataset(table_identifier)
        if not dataset:
            raise PermissionError("No policy for requested table")
        return AccessDecision(
            allowed_columns=allowed_columns,
            masks=masks,
            row_filter=row_filter,
            policy_version=_dataset_version(dataset),
        )

    def current_policy_version(self, table_identifier: str) -> int | None:
        policy = self._load_policy()
        dataset = policy.match_dataset(table_identifier)
        if not dataset:
            return None
        return _dataset_version(dataset)


def _dataset_version(dataset: DatasetPolicy) -> int:
    payload = {
        "table": dataset.table,
        "rules": [
            {
                "principals": rule.principals,
                "columns": rule.columns,
                "masks": {
                    key: {"type": value.type, "value": value.value}
                    for key, value in rule.masks.items()
                },
                "row_filter": rule.row_filter,
            }
            for rule in dataset.rules
        ],
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(raw).digest()
    return int.from_bytes(digest[:8], "big")
