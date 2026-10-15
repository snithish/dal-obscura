"""Bounded descriptive definitions for scalar identity attributes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, TypedDict, cast


class AttributeDefinition(TypedDict):
    label: str
    description: str
    allowed_values: list[str]


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
