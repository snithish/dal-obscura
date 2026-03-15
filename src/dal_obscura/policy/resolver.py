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
    allowed_set: set[str] = set()
    masks: Dict[str, MaskRule] = {}
    row_filters: List[str] = []

    for rule in dataset.rules:
        if not principal_tokens.intersection(rule.principals):
            continue
        if "*" in rule.columns:
            allowed_columns = list(requested_columns)
        else:
            allowed_columns = [c for c in requested_columns if c in rule.columns]
        allowed_set.update(allowed_columns)
        for col, mask in rule.masks.items():
            existing = masks.get(col)
            masks[col] = _choose_mask(existing, mask)
        if rule.row_filter:
            row_filters.append(rule.row_filter)

    if not allowed_set:
        raise PermissionError("No allowed columns for principal")

    combined_filter = " AND ".join(f"({f})" for f in row_filters) if row_filters else None
    ordered_allowed = [c for c in requested_columns if c in allowed_set]
    return ordered_allowed, masks, combined_filter


def _choose_mask(existing: MaskRule | None, candidate: MaskRule) -> MaskRule:
    if existing is None:
        return candidate
    if _mask_precedence(candidate) > _mask_precedence(existing):
        return candidate
    return existing


def _mask_precedence(mask: MaskRule) -> int:
    mask_type = mask.type.lower()
    if mask_type == "null":
        return 4
    if mask_type == "redact":
        return 3
    if mask_type == "hash":
        return 2
    if mask_type == "default":
        return 1
    return 0
