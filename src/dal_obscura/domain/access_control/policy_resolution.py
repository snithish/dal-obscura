from __future__ import annotations

import hashlib
import json
from typing import Iterable

from dal_obscura.domain.query_planning import DatasetSelector

from .models import DatasetPolicy, MaskRule, Policy, Principal


def resolve_access(
    policy: Policy,
    principal: Principal,
    dataset: DatasetSelector,
    requested_columns: Iterable[str],
) -> tuple[list[str], dict[str, MaskRule], str | None]:
    matched_dataset = policy.match_dataset(dataset)
    if not matched_dataset:
        raise PermissionError("No policy for requested table")

    principal_tokens = set(principal.tokens())
    allowed_set: set[str] = set()
    masks: dict[str, MaskRule] = {}
    row_filters: list[str] = []

    requested = list(requested_columns)
    for rule in matched_dataset.rules:
        if not principal_tokens.intersection(rule.principals):
            continue

        allowed_columns = (
            requested if "*" in rule.columns else [c for c in requested if c in rule.columns]
        )
        allowed_set.update(allowed_columns)
        for column, mask in rule.masks.items():
            existing = masks.get(column)
            masks[column] = _choose_mask(existing, mask)
        if rule.row_filter:
            row_filters.append(rule.row_filter)

    if not allowed_set:
        raise PermissionError("No allowed columns for principal")

    combined_filter = " AND ".join(f"({part})" for part in row_filters) if row_filters else None
    ordered_allowed = [column for column in requested if column in allowed_set]
    return ordered_allowed, masks, combined_filter


def dataset_version(dataset: DatasetPolicy) -> int:
    payload = {
        "catalog": dataset.catalog,
        "target": dataset.target,
        "rules": [
            {
                "principals": rule.principals,
                "columns": rule.columns,
                "masks": {
                    name: {"type": mask.type, "value": mask.value}
                    for name, mask in rule.masks.items()
                },
                "row_filter": rule.row_filter,
            }
            for rule in dataset.rules
        ],
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(raw).digest()
    return int.from_bytes(digest[:8], "big")


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
