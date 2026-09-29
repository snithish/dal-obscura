"""Validate and normalize directly edited asset policies."""

from __future__ import annotations

from dataclasses import dataclass, replace
from math import isfinite
from typing import Any, Literal, cast

from dal_obscura.common.access_control.compiled_policy import (
    CompiledMaskRule,
    CompiledPolicyRule,
)
from dal_obscura.common.access_control.filters import deserialize_row_filter
from dal_obscura.common.access_control.mask_types import SUPPORTED_MASK_TYPES
from dal_obscura.common.query_planning.field_paths import (
    FieldPath,
    FieldPathSegment,
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    MapValueSegment,
    parse_field_path,
)
from dal_obscura.control_plane.application.errors import PolicyValidationFailure, ValidationFailure

_MASK_TYPES = frozenset(SUPPORTED_MASK_TYPES)
_RULE_FIELDS = {
    "ordinal",
    "effect",
    "principals",
    "columns",
    "masks",
    "row_filter",
    "when",
    "name",
    "description",
}


@dataclass(frozen=True)
class _PolicyRule:
    ordinal: int
    principals: list[str]
    columns: list[str]
    masks: dict[str, object]
    row_filter: str | None
    when: dict[str, str | list[str]]
    effect: Literal["allow", "allow_all"] = "allow"
    name: str = ""
    description: str = ""


def validate_policy_rule_payloads(rules: list[dict[str, Any]]) -> None:
    """Validates rules before they are stored in the canonical policy rows."""

    compile_policy_rule_payloads(rules, [])


def compile_policy_rule_payloads(
    rules: list[dict[str, Any]],
    schema_fields: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Normalize rules and freeze field selections against admitted schema paths."""

    compiled: list[dict[str, object]] = []
    for index, raw in enumerate(rules):
        try:
            rule = _rule_from_payload(index, raw)
            expanded = _expand_schema_bound_rule(rule, schema_fields)
            compiled.append(
                CompiledPolicyRule(
                    ordinal=expanded.ordinal,
                    principals=expanded.principals,
                    columns=expanded.columns,
                    effect=expanded.effect,
                    name=expanded.name,
                    description=expanded.description,
                    when=expanded.when,
                    masks={
                        column: _compile_mask_rule(column, mask)
                        for column, mask in expanded.masks.items()
                    },
                    row_filter=_normalize_row_filter(expanded.row_filter),
                ).to_json()
            )
        except (ValidationFailure, ValueError, TypeError) as exc:
            raise PolicyValidationFailure(index, str(exc)) from exc
    return compiled


def _rule_from_payload(index: int, raw: dict[str, Any]) -> _PolicyRule:
    _reject_unknown_rule_fields(raw)
    try:
        ordinal_value = _rule_ordinal(raw, index)
        effect = raw.get("effect", "allow")
        if effect not in {"allow", "allow_all"}:
            raise ValidationFailure("Policy rule effect must be 'allow' or 'allow_all'.")
        principals = _string_list(raw.get("principals", []), "principals")
        columns = _string_list(raw.get("columns", []), "columns")
        normalized_when = _conditions(raw.get("when", {}))
        masks = _masks(raw.get("masks", {}))
        row_filter = raw.get("row_filter")
        if row_filter is not None and not isinstance(row_filter, str):
            raise ValueError("row_filter must be text or null")
        name = raw.get("name", "")
        description = raw.get("description", "")
        if (
            not isinstance(name, str)
            or len(name) > 160
            or not isinstance(description, str)
            or len(description) > 2000
        ):
            raise ValueError("Rule name or description is invalid")
        if effect == "allow_all" and (
            principals != ["*"] or columns != ["*"] or masks or row_filter or normalized_when
        ):
            raise ValueError("Allow all must target all users and columns without restrictions")
        return _PolicyRule(
            effect=cast(Literal["allow", "allow_all"], effect),
            name=name.strip(),
            description=description,
            ordinal=ordinal_value,
            principals=principals,
            columns=columns,
            masks=dict(masks),
            row_filter=row_filter,
            when=normalized_when,
        )
    except ValidationFailure:
        raise
    except (TypeError, ValueError) as exc:
        raise ValidationFailure(f"Invalid policy rule {index + 1}: {exc}") from exc


def _reject_unknown_rule_fields(raw: dict[str, Any]) -> None:
    unknown = set(raw) - _RULE_FIELDS
    if unknown:
        raise ValidationFailure(f"Policy rule has unsupported fields: {', '.join(sorted(unknown))}")


def _rule_ordinal(raw: dict[str, Any], index: int) -> int:
    ordinal = raw.get("ordinal", index)
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 0:
        raise ValueError("ordinal must be a non-negative integer")
    return ordinal


def _conditions(value: object) -> dict[str, str | list[str]]:
    if not isinstance(value, dict):
        raise ValueError("when must be an object")
    result: dict[str, str | list[str]] = {}
    for key, expected in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("condition names must be non-empty strings")
        if isinstance(expected, str):
            result[key.strip()] = expected
        elif isinstance(expected, list):
            normalized = [item for item in expected if isinstance(item, str)]
            if len(normalized) == len(expected):
                result[key.strip()] = normalized
            else:
                raise ValueError(f"condition {key!r} must be text or a list of text")
        else:
            raise ValueError(f"condition {key!r} must be text or a list of text")
    return result


def _masks(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("masks must be an object keyed by field path")
    result: dict[str, object] = {}
    for key, mask in value.items():
        if not isinstance(key, str):
            raise ValueError("masks must be an object keyed by field path")
        result[key] = mask
    return result


def _string_list(value: object, label: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list of strings")
    normalized = [item for item in value if isinstance(item, str)]
    if len(normalized) != len(value):
        raise ValueError(f"{label} must be a list of strings")
    if any(not item.strip() for item in normalized):
        raise ValueError(f"{label} entries must be non-empty")
    return [item.strip() for item in normalized]


def _normalize_row_filter(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    normalized = value.strip()
    try:
        deserialize_row_filter(normalized)
    except Exception as exc:
        raise ValidationFailure(f"Invalid row_filter SQL: {normalized}") from exc
    return normalized


def _expand_schema_bound_rule(
    rule: _PolicyRule,
    schema_fields: list[dict[str, object]],
) -> _PolicyRule:
    if not schema_fields or rule.effect == "allow_all":
        return rule
    columns = _expand_schema_bound_paths(rule.columns, schema_fields)
    masks: dict[str, object] = {}
    for path, mask in rule.masks.items():
        for candidate in _expand_schema_bound_paths([path], schema_fields):
            if candidate in masks and _compile_mask_rule(
                candidate, masks[candidate]
            ) != _compile_mask_rule(candidate, mask):
                raise ValidationFailure(
                    f"Overlapping masks for column {candidate!r}; select one mask per field"
                )
            masks[candidate] = mask
    return replace(rule, columns=columns, masks=masks)


def _expand_schema_bound_paths(
    requested: list[str],
    schema_fields: list[dict[str, object]],
) -> list[str]:
    admitted: list[tuple[str, tuple[FieldPathSegment, ...], str]] = []
    for field in schema_fields:
        raw_path = field.get("path")
        if not isinstance(raw_path, list) or not raw_path:
            continue
        path = tuple(_schema_path_segments(cast(list[object], raw_path)))
        canonical = FieldPath(path).to_human()
        raw_name = str(field.get("name", "")).strip()
        admitted.append((canonical, path, raw_name))

    expanded: list[str] = []
    seen: set[str] = set()
    for value in requested:
        if value == "*":
            candidates = [item[0] for item in admitted]
        else:
            try:
                parsed = parse_field_path(value)
            except ValueError as exc:
                raise ValidationFailure(f"Invalid column path: {value}") from exc
            else:
                candidates = [
                    canonical
                    for canonical, path, _ in admitted
                    if len(parsed.segments) <= len(path)
                    and tuple(parsed.segments) == path[: len(parsed.segments)]
                ]
                if not candidates:
                    raise ValidationFailure(f"Unknown column path: {value}")
        for candidate in candidates:
            if candidate not in seen:
                expanded.append(candidate)
                seen.add(candidate)
    return expanded


def _schema_path_segments(path: list[object]) -> list[FieldPathSegment]:
    segments: list[FieldPathSegment] = []
    for segment in path:
        if not isinstance(segment, str) or not segment.strip():
            raise ValidationFailure("Schema field paths must contain non-empty strings")
        if segment == "$element":
            segments.append(ListElementSegment())
        elif segment == "$key":
            segments.append(MapKeySegment())
        elif segment == "$value":
            segments.append(MapValueSegment())
        else:
            segments.append(FieldSegment(segment))
    if not segments or not isinstance(segments[0], FieldSegment):
        raise ValidationFailure("Schema field paths must begin with a field segment")
    return segments


def _compile_mask_rule(column: str, raw_mask: object) -> CompiledMaskRule:
    if not isinstance(raw_mask, dict) or set(raw_mask) - {"type", "value", "exempt_principals"}:
        raise ValidationFailure(f"Invalid mask for column {column!r}")
    normalized_mask = cast(dict[str, object], raw_mask)
    mask_type = normalized_mask.get("type")
    if not isinstance(mask_type, str) or mask_type.lower() not in _MASK_TYPES:
        raise ValidationFailure(f"Invalid mask for column {column!r}")
    normalized_type = mask_type.lower()
    if (
        isinstance(parse_field_path(column).segments[-1], MapKeySegment)
        and normalized_type != "null"
    ):
        raise ValidationFailure("Map keys support only the null mask; mask map values instead")
    value = normalized_mask.get("value")
    if normalized_type in {"null", "hash", "email"} and value is not None:
        raise ValidationFailure(f"Mask {normalized_type!r} does not accept a value")
    if normalized_type == "redact" and not isinstance(value, str):
        raise ValidationFailure(f"Invalid mask for column {column!r}")
    if normalized_type == "keep_last" and (
        isinstance(value, bool) or not isinstance(value, int) or value < 0
    ):
        raise ValidationFailure(f"Invalid mask for column {column!r}")
    if normalized_type == "default" and isinstance(value, (dict, list, tuple, set)):
        raise ValidationFailure(f"Invalid mask for column {column!r}")
    if normalized_type == "default" and isinstance(value, float) and not isfinite(value):
        raise ValidationFailure(f"Invalid mask for column {column!r}")
    exemptions_raw = normalized_mask.get("exempt_principals", [])
    try:
        exemptions = _string_list(exemptions_raw, "exempt_principals")
    except (TypeError, ValueError) as exc:
        raise ValidationFailure(f"Invalid mask exemptions for column {column!r}") from exc
    if any(
        not token.strip()
        or token != token.strip()
        or token == "*"
        or token.casefold() == "everyone"
        or (
            token.startswith("group:")
            and (
                not token.removeprefix("group:").strip()
                or token.removeprefix("group:").casefold() in {"*", "everyone"}
                or any(char.isspace() for char in token)
            )
        )
        for token in exemptions
    ):
        raise ValidationFailure(f"Invalid mask exemptions for column {column!r}")
    normalized_exemptions = tuple(sorted(set(exemptions)))
    return CompiledMaskRule(
        type=normalized_type, value=value, exempt_principals=normalized_exemptions
    )
