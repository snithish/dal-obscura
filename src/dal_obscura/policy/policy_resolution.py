from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from dal_obscura.policy.attribute_conditions import attributes_match
from dal_obscura.policy.models import (
    MaskRule,
    Policy,
    Principal,
    PrincipalConditionValue,
)
from dal_obscura.policy.paths import path_covers


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

    requested = list(dict.fromkeys(requested_columns))
    # This is an explicit, authenticated policy bypass, not an ordinary grant.
    if any(
        rule.effect == "allow_all"
        and tuple(rule.principals) == ("*",)
        and tuple(rule.columns) == ("*",)
        and not rule.when
        and not rule.row_filter
        and not rule.masks
        for rule in matched_dataset.rules
    ):
        return requested, {}, None

    principal_tokens = set(principal.tokens())
    granted: set[str] = set()
    candidates: dict[str, list[MaskRule]] = {}
    row_filters: list[str] = []
    for rule in matched_dataset.rules:
        if rule.effect != "allow":
            continue
        if "*" not in rule.principals and not principal_tokens.intersection(rule.principals):
            continue
        if not _matches_conditions(principal, rule.when):
            continue
        matching_columns = _matching_columns(requested, rule.columns)
        granted.update(matching_columns)
        _collect_masks(candidates, matching_columns, rule.masks, principal_tokens)
        # Row rules are independent of projection and mask exemptions.
        if rule.row_filter:
            row_filters.append(rule.row_filter)

    masks = {column: _combine_masks(items) for column, items in candidates.items()}
    for column in requested:
        if column not in granted:
            masks[column] = MaskRule(type="null")
    combined_filter = " AND ".join(f"({part})" for part in row_filters) if row_filters else None
    return requested, masks, combined_filter


def _collect_masks(
    candidates: dict[str, list[MaskRule]],
    columns: list[str],
    masks: Mapping[str, MaskRule],
    principal_tokens: set[str],
) -> None:
    for column in columns:
        for path, mask in masks.items():
            if path_covers(path, column) and not principal_tokens.intersection(
                mask.exempt_principals
            ):
                candidates.setdefault(column, []).append(MaskRule(type=mask.type, value=mask.value))


def _combine_masks(candidates: list[MaskRule]) -> MaskRule:
    # Resolve NULL before comparing other masks, so rule order never changes
    # whether an otherwise incompatible set is safely suppressed.
    if any(mask.type.lower() == "null" for mask in candidates):
        return MaskRule(type="null")
    result = candidates[0]
    for candidate in candidates[1:]:
        result = _choose_mask(result, candidate)
    return result


def _matching_columns(requested: list[str], grants: Sequence[str]) -> list[str]:
    """Returns the exact authorized leaves for requested nested paths.

    A grant on a parent covers a requested descendant. A request for a parent
    is pruned to each granted descendant, preventing sibling disclosure.
    """
    if "*" in grants:
        return list(requested)

    matched: list[str] = []
    for requested_path in requested:
        for grant_path in grants:
            if path_covers(grant_path, requested_path):
                candidate = requested_path
            elif path_covers(requested_path, grant_path):
                candidate = grant_path
            else:
                continue
            if candidate not in matched:
                matched.append(candidate)
    return matched


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
    when: Mapping[str, PrincipalConditionValue] | None,
) -> bool:
    return attributes_match(principal.attributes, when)
