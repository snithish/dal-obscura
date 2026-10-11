from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Literal, TypedDict, cast

from dal_obscura.policy.models import Principal


class AttributeDefinition(TypedDict):
    label: str
    description: str
    allowed_values: list[str]


class PrincipalClaimMapper:
    """Maps validated token or gateway claims into the service principal model."""

    def __init__(
        self,
        *,
        subject_claim: str = "sub",
        group_claims: Sequence[str] | None = None,
        attribute_claims: Mapping[str, str] | None = None,
        attribute_definitions: Mapping[str, object] | None = None,
    ) -> None:
        self._subject_claim = subject_claim
        self._group_claims = tuple(group_claims or ())
        self._attribute_claims = dict(attribute_claims or {})
        self._definitions = validate_attribute_definitions(
            attribute_definitions, self._attribute_claims
        )

    def map_claims(self, claims: Mapping[str, object]) -> Principal:
        subject = _string_claim(claims, self._subject_claim)
        if not subject:
            raise PermissionError("Missing subject")
        return Principal(
            id=subject,
            groups=_groups_from_claims(claims, self._group_claims),
            attributes=self.map_attributes(claims),
        )

    def map_attributes(self, claims: Mapping[str, object]) -> dict[str, str]:
        """Apply the same mapping and value domains in runtime and synthetic previews."""
        attributes = _attributes_from_claims(claims, self._attribute_claims)
        for key, value in attributes.items():
            domain = self._definitions.get(key, {}).get("allowed_values", [])
            if domain and value not in domain:
                raise PermissionError(f"Attribute {key!r} is outside its allowed values")
        return attributes


def validate_attribute_definitions(
    definitions: Mapping[str, object] | None,
    mappings: Mapping[str, str],
) -> dict[str, AttributeDefinition]:
    """Validate bounded descriptive definitions for mapped scalar attributes."""
    if definitions is None:
        return {}
    if not isinstance(definitions, Mapping) or len(definitions) > 32:
        raise ValueError("Attribute definitions must be a bounded mapping")
    result: dict[str, AttributeDefinition] = {}
    for key, definition in definitions.items():
        if key not in mappings or not isinstance(definition, Mapping):
            raise ValueError("Every attribute definition must reference a mapped attribute")
        if set(definition) - {"label", "description", "allowed_values"}:
            raise ValueError("Unsupported attribute definition field")
        definition = cast(Mapping[str, object], definition)
        normalized: AttributeDefinition = {"label": "", "description": "", "allowed_values": []}
        text_fields: tuple[tuple[Literal["label", "description"], int], ...] = (
            ("label", 120),
            ("description", 500),
        )
        for field, limit in text_fields:
            value = definition.get(field, "")
            if not isinstance(value, str) or len(value) > limit:
                raise ValueError(f"Attribute {field} must be bounded text")
            normalized[field] = value
        domain = definition.get("allowed_values", [])
        if not isinstance(domain, list) or len(domain) > 100:
            raise ValueError("Allowed values must be at most 100 non-empty trimmed text values")
        allowed: list[str] = []
        for value in domain:
            if (
                not isinstance(value, str)
                or not value.strip()
                or value != value.strip()
                or len(value) > 256
                or any(ord(char) < 32 for char in value)
            ):
                raise ValueError("Allowed values must be at most 100 non-empty trimmed text values")
            allowed.append(value)
        if len(set(allowed)) != len(allowed):
            raise ValueError("Allowed values must be unique")
        normalized["allowed_values"] = allowed
        result[key] = normalized
    return result


def _string_claim(payload: Mapping[str, object], path: str) -> str:
    value = _value_at_path(payload, path)
    if value is None:
        return ""
    if isinstance(value, Mapping | list):
        return ""
    return str(value).strip()


def _groups_from_claims(payload: Mapping[str, object], claim_paths: Sequence[str]) -> list[str]:
    groups: list[str] = []
    seen: set[str] = set()
    for path in claim_paths:
        for group in _flatten_group_values(_value_at_path(payload, path)):
            normalized = str(group).strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                groups.append(normalized)
    return groups


def _attributes_from_claims(
    payload: Mapping[str, object], claim_paths: Mapping[str, str]
) -> dict[str, str]:
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


def _value_at_path(payload: Mapping[str, object], path: str) -> object:
    current: object = payload
    for raw_part in path.split("."):
        part = raw_part.strip()
        if not part:
            return None
        if not isinstance(current, Mapping):
            return None
        current = cast(Mapping[str, object], current).get(part)
    return current
