"""Attribute condition matching shared by runtime and policy-test evidence."""

from collections.abc import Mapping

from dal_obscura.common.access_control.models import PrincipalConditionValue


def attributes_match(
    attributes: Mapping[str, str], conditions: Mapping[str, PrincipalConditionValue] | None
) -> bool:
    """Match all scalar text conditions, failing closed for missing attributes."""
    return all(
        key in attributes
        and (
            attributes[key] in {str(item) for item in expected}
            if isinstance(expected, list)
            else attributes[key] == expected
        )
        for key, expected in (conditions or {}).items()
    )
