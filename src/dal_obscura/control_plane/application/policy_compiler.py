"""Validate and normalize directly edited asset policies."""

from __future__ import annotations

from dataclasses import dataclass, replace
from math import isfinite
from typing import Any, cast

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
from dal_obscura.control_plane.application.errors import ValidationFailure

_MASK_TYPES = frozenset(SUPPORTED_MASK_TYPES)
_RULE_FIELDS = {"ordinal", "effect", "principals", "columns", "masks", "row_filter", "when"}


@dataclass(frozen=True)
class _PolicyRule:
    ordinal: int
    principals: list[str]
    columns: list[str]
    masks: dict[str, object]
    row_filter: str | None
    when: dict[str, str | list[str]]


def validate_policy_rule_payloads(rules: list[dict[str, Any]]) -> None:
    """Validates rules before they are stored in the canonical policy rows."""

    compile_policy_rule_payloads(rules, [])


def compile_policy_rule_payloads(
    rules: list[dict[str, Any]],
    schema_fields: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Normalize rules and freeze field aliases against admitted schema paths."""

    compiled: list[dict[str, object]] = []
    for index, raw in enumerate(rules):
        rule = _rule_from_payload(index, raw)
        expanded = _expand_schema_bound_rule(rule, schema_fields)
        compiled.append(
            CompiledPolicyRule(
                ordinal=expanded.ordinal,
                principals=expanded.principals,
                columns=expanded.columns,
                effect="allow",
                when=expanded.when,
                masks={
                    column: _compile_mask_rule(column, mask)
                    for column, mask in expanded.masks.items()
                },
                row_filter=_normalize_row_filter(expanded.row_filter),
            ).to_json()
        )
    return compiled


def _rule_from_payload(index: int, raw: dict[str, Any]) -> _PolicyRule:
    _reject_unknown_rule_fields(raw)
    try:
        ordinal_value = _rule_ordinal(raw, index)
        effect = raw.get("effect", "allow")
        if effect != "allow":
            raise ValidationFailure("Policy rules are explicit grants; effect must be 'allow'.")
        principals = _string_list(raw.get("principals", []), "principals")
        columns = _string_list(raw.get("columns", []), "columns")
        normalized_when = _conditions(raw.get("when", {}))
        masks = _masks(raw.get("masks", {}))
        row_filter = raw.get("row_filter")
        if row_filter is not None and not isinstance(row_filter, str):
            raise ValueError("row_filter must be text or null")
        return _PolicyRule(
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
    if not schema_fields:
        return rule
    columns = _expand_schema_bound_paths(rule.columns, schema_fields)
    masks: dict[str, object] = {}
    for path, mask in rule.masks.items():
        for candidate in _expand_schema_bound_paths([path], schema_fields):
            masks[candidate] = mask
    return replace(rule, columns=columns, masks=masks)


def _expand_schema_bound_paths(
    requested: list[str],
    schema_fields: list[dict[str, object]],
) -> list[str]:
    admitted: list[tuple[str, tuple[FieldPathSegment, ...], str]] = []
    aliases: dict[str, str] = {}
    for field in schema_fields:
        raw_path = field.get("path")
        if not isinstance(raw_path, list) or not raw_path:
            continue
        path = tuple(_schema_path_segments(cast(list[object], raw_path)))
        canonical = FieldPath(path).to_human()
        raw_name = str(field.get("name", "")).strip()
        admitted.append((canonical, path, raw_name))
        if raw_name:
            aliases[raw_name] = canonical

    expanded: list[str] = []
    seen: set[str] = set()
    for value in requested:
        if value == "*":
            candidates = [item[0] for item in admitted]
        elif value in aliases:
            candidates = [aliases[value]]
        else:
            try:
                parsed = parse_field_path(value)
            except ValueError:
                candidates = [value]
            else:
                candidates = [
                    canonical
                    for canonical, path, _ in admitted
                    if len(parsed.segments) <= len(path)
                    and tuple(parsed.segments) == path[: len(parsed.segments)]
                ] or [value]
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
    if not isinstance(raw_mask, dict) or set(raw_mask) - {"type", "value"}:
        raise ValidationFailure(f"Invalid mask for column {column!r}")
    normalized_mask = cast(dict[str, object], raw_mask)
    mask_type = normalized_mask.get("type")
    if not isinstance(mask_type, str) or mask_type.lower() not in _MASK_TYPES:
        raise ValidationFailure(f"Invalid mask for column {column!r}")
    normalized_type = mask_type.lower()
    value = normalized_mask.get("value")
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
    return CompiledMaskRule(type=normalized_type, value=value)
