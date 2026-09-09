from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable

from dal_obscura.common.access_control.models import (
    DatasetPolicy,
    MaskRule,
    Policy,
    Principal,
    PrincipalConditionValue,
)


def resolve_access(
    policy: Policy,
    principal: Principal,
    target: str,
    catalog: str | None,
    requested_columns: Iterable[str],
) -> tuple[list[str], dict[str, MaskRule], str | None]:
    """Combines all matching rules into a single access decision for the dataset."""
    matched_dataset = policy.match_dataset(target, catalog)
    if not matched_dataset:
        raise PermissionError("No policy for requested table")

    principal_tokens = set(principal.tokens())
    allowed_set: set[str] = set()
    masks: dict[str, MaskRule] = {}
    row_filters: list[str] = []

    requested = list(requested_columns)
    for rule in matched_dataset.rules:
        if rule.effect != "allow":
            continue
        if not principal_tokens.intersection(rule.principals):
            continue
        if not _matches_conditions(principal, rule.when):
            continue

        matching_columns = (
            requested if "*" in rule.columns else [c for c in requested if c in rule.columns]
        )
        # Rule matches are unioned so multiple roles can widen the projection while
        # still allowing the stricter mask precedence rules below to win.
        allowed_set.update(matching_columns)
        for column, mask in rule.masks.items():
            existing = masks.get(column)
            masks[column] = _choose_mask(existing, mask)
        if rule.row_filter:
            row_filters.append(rule.row_filter)

    effective_allowed = [column for column in requested if column in allowed_set]
    if not effective_allowed:
        raise PermissionError("No allowed columns for principal")

    combined_filter = " AND ".join(f"({part})" for part in row_filters) if row_filters else None
    return effective_allowed, masks, combined_filter


def dataset_version(dataset: DatasetPolicy) -> int:
    """Hashes the effective dataset policy so tickets can detect stale policy state."""
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
                "effect": rule.effect,
                "when": rule.when,
            }
            for rule in dataset.rules
        ],
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(raw).digest()
    return int.from_bytes(digest[:8], "big")


def _choose_mask(existing: MaskRule | None, candidate: MaskRule) -> MaskRule:
    """Combines compatible masks and rejects ambiguous policy composition."""
    if existing is None:
        return candidate
    if existing == candidate:
        return existing
    if existing.type.lower() == "null" or candidate.type.lower() == "null":
        return MaskRule(type="null")
    if existing.type.lower() == candidate.type.lower() == "keep_last":
        existing_value = _keep_last_value(existing)
        candidate_value = _keep_last_value(candidate)
        return MaskRule(type="keep_last", value=min(existing_value, candidate_value))
    raise PermissionError("Conflicting masks for the same field")


def _keep_last_value(mask: MaskRule) -> int:
    value = mask.value
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PermissionError("Invalid keep_last mask")
    return value


def _matches_conditions(
    principal: Principal,
    when: dict[str, PrincipalConditionValue] | None,
) -> bool:
    if not when:
        return True

    for key, expected in when.items():
        actual = principal.attributes.get(key)
        if actual is None:
            return False
        if isinstance(expected, list):
            if actual not in {str(item) for item in expected}:
                return False
            continue
        if actual != expected:
            return False
    return True
