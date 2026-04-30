from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from dal_obscura.common.access_control.models import Principal

JsonObject = Mapping[str, Any]


class PrincipalClaimMapper:
    """Maps validated token or gateway claims into the service principal model."""

    def __init__(
        self,
        *,
        subject_claim: str = "sub",
        group_claims: Sequence[str] | None = None,
        attribute_claims: Mapping[str, str] | None = None,
    ) -> None:
        self._subject_claim = subject_claim
        self._group_claims = tuple(group_claims or ())
        self._attribute_claims = dict(attribute_claims or {})

    def map_claims(self, claims: JsonObject) -> Principal:
        subject = _string_claim(claims, self._subject_claim)
        if not subject:
            raise PermissionError("Missing subject")
        return Principal(
            id=subject,
            groups=_groups_from_claims(claims, self._group_claims),
            attributes=_attributes_from_claims(claims, self._attribute_claims),
        )


def _string_claim(payload: JsonObject, path: str) -> str:
    value = _value_at_path(payload, path)
    if value is None:
        return ""
    if isinstance(value, Mapping | list):
        return ""
    return str(value).strip()


def _groups_from_claims(payload: JsonObject, claim_paths: Sequence[str]) -> list[str]:
    groups: list[str] = []
    seen: set[str] = set()
    for path in claim_paths:
        for group in _flatten_group_values(_value_at_path(payload, path)):
            normalized = str(group).strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                groups.append(normalized)
    return groups


def _attributes_from_claims(payload: JsonObject, claim_paths: Mapping[str, str]) -> dict[str, str]:
    attributes: dict[str, str] = {}
    for attribute_name, path in claim_paths.items():
        value = _value_at_path(payload, path)
        if value is None:
            continue
        if isinstance(value, Mapping | list):
            raise PermissionError("Invalid attribute claim")
        normalized = str(value).strip()
        if normalized:
            attributes[str(attribute_name)] = normalized
    return attributes


def _flatten_group_values(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        groups: list[str] = []
        for nested in value.values():
            groups.extend(_flatten_group_values(nested))
        return groups
    if isinstance(value, list):
        groups: list[str] = []
        for item in value:
            groups.extend(_flatten_group_values(item))
        return groups
    return [str(value)]


def _value_at_path(payload: JsonObject, path: str) -> object:
    current: object = payload
    for raw_part in path.split("."):
        part = raw_part.strip()
        if not part:
            return None
        if not isinstance(current, Mapping):
            return None
        mapping = cast(Mapping[object, object], current)
        if part not in mapping:
            return None
        current = mapping[part]
    return current
