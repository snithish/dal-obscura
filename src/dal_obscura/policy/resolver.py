from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from .models import MaskRule, Policy, Principal


def resolve_access(
    policy: Policy,
    principal: Principal,
    table_identifier: str,
    requested_columns: Iterable[str],
) -> tuple[List[str], Dict[str, MaskRule], Optional[str]]:
    dataset = policy.match_dataset(table_identifier)
    if not dataset:
        raise PermissionError("No policy for requested table")

    principal_tokens = set(principal.tokens())
    allowed_columns: List[str] = []
    masks: Dict[str, MaskRule] = {}
    row_filters: List[str] = []

    for rule in dataset.rules:
        if not principal_tokens.intersection(rule.principals):
            continue
        if "*" in rule.columns:
            allowed_columns = list(requested_columns)
        else:
            allowed_columns = [c for c in requested_columns if c in rule.columns]
        for col, mask in rule.masks.items():
            masks[col] = mask
        if rule.row_filter:
            row_filters.append(rule.row_filter)

    if not allowed_columns:
        raise PermissionError("No allowed columns for principal")

    combined_filter = " AND ".join(f"({f})" for f in row_filters) if row_filters else None
    return allowed_columns, masks, combined_filter
