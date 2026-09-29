"""JSON-serializable compiled policy records.

Example:
    ```python
    compiled = CompiledPolicy.from_json(payload)
    policy = compiled.to_policy()
    ```
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, cast

from dal_obscura.common.access_control.models import (
    AccessRule,
    DatasetPolicy,
    MaskRule,
    Policy,
    PrincipalConditionValue,
)


@dataclass(frozen=True)
class CompiledMaskRule:
    """Compiled mask rule stored in live asset policy JSON.

    Example:
        ```python
        mask = CompiledMaskRule(type="hash")
        rule = mask.to_mask_rule()
        ```
    """

    type: str
    value: object | None = None
    exempt_principals: tuple[str, ...] = ()

    def to_mask_rule(self) -> MaskRule:
        return MaskRule(type=self.type, value=self.value, exempt_principals=self.exempt_principals)

    def to_json(self) -> dict[str, object]:
        payload: dict[str, object] = {"type": self.type}
        if self.value is not None:
            payload["value"] = self.value
        if self.exempt_principals:
            payload["exempt_principals"] = list(self.exempt_principals)
        return payload

    @classmethod
    def from_json(cls, raw: object) -> CompiledMaskRule:
        data = _required_mapping(raw, "mask")
        _reject_unknown(data, {"type", "value", "exempt_principals"}, "mask")
        mask_type = _required_text(data.get("type"), "mask.type")
        exemptions = tuple(_text_list(data.get("exempt_principals", []), "mask.exempt_principals"))
        return cls(type=mask_type, value=data.get("value"), exempt_principals=exemptions)


@dataclass(frozen=True)
class CompiledPolicyRule:
    """Compiled access rule stored in live asset policy JSON.

    Example:
        ```python
        rule = CompiledPolicyRule(
            ordinal=0,
            effect="allow",
            principals=["analyst"],
            columns=["id"],
            masks={},
        )
        ```
    """

    ordinal: int
    effect: Literal["allow", "allow_all"]
    principals: list[str]
    columns: list[str]
    masks: dict[str, CompiledMaskRule]
    row_filter: str | None = None
    when: dict[str, PrincipalConditionValue] = field(default_factory=dict)
    name: str = ""
    description: str = ""

    def to_access_rule(self) -> AccessRule:
        return AccessRule(
            principals=list(self.principals),
            columns=list(self.columns),
            masks={column: mask.to_mask_rule() for column, mask in self.masks.items()},
            row_filter=self.row_filter,
            effect=self.effect,
            when=dict(self.when),
            name=self.name,
            description=self.description,
        )

    def to_json(self) -> dict[str, object]:
        return {
            "ordinal": self.ordinal,
            "name": self.name,
            "description": self.description,
            "principals": list(self.principals),
            "columns": list(self.columns),
            "effect": self.effect,
            "when": dict(self.when),
            "masks": {column: mask.to_json() for column, mask in self.masks.items()},
            "row_filter": self.row_filter,
        }

    @classmethod
    def from_json(cls, raw: object) -> CompiledPolicyRule:
        data = _required_mapping(raw, "policy rule")
        _reject_unknown(
            data,
            {
                "ordinal",
                "effect",
                "principals",
                "columns",
                "masks",
                "row_filter",
                "when",
                "name",
                "description",
            },
            "policy rule",
        )
        masks = _required_mapping(data.get("masks"), "policy rule.masks")
        return cls(
            ordinal=_non_negative_int(data.get("ordinal", 0), "policy rule.ordinal"),
            effect=_effect(data.get("effect")),
            principals=_text_list(data.get("principals"), "policy rule.principals"),
            columns=_text_list(data.get("columns"), "policy rule.columns"),
            masks={
                _required_text(column, "policy rule mask path"): CompiledMaskRule.from_json(mask)
                for column, mask in masks.items()
            },
            row_filter=_optional_str(data.get("row_filter")),
            when=_conditions(data.get("when")),
            name=str(data.get("name", "")),
            description=str(data.get("description", "")),
        )


@dataclass(frozen=True)
class CompiledPolicy:
    """Compiled policy snapshot for one governed asset.

    Example:
        ```python
        compiled = CompiledPolicy(version=1, catalog="analytics", target="orders", rules=[])
        policy = compiled.to_policy()
        ```
    """

    version: int
    catalog: str
    target: str
    rules: list[CompiledPolicyRule]

    def to_policy(self) -> Policy:
        return Policy(
            version=self.version,
            datasets=[
                DatasetPolicy(
                    catalog=self.catalog,
                    target=self.target,
                    rules=[rule.to_access_rule() for rule in self.rules],
                )
            ],
        )

    def to_json(self) -> dict[str, object]:
        return {
            "version": self.version,
            "catalog": self.catalog,
            "target": self.target,
            "rules": [rule.to_json() for rule in self.rules],
        }

    @classmethod
    def from_json(cls, raw: object) -> CompiledPolicy:
        data = _required_mapping(raw, "policy")
        _reject_unknown(data, {"version", "catalog", "target", "rules"}, "policy")
        return cls(
            version=_non_negative_int(data.get("version"), "policy.version"),
            catalog=_required_text(data.get("catalog"), "policy.catalog"),
            target=_required_text(data.get("target"), "policy.target"),
            rules=[
                CompiledPolicyRule.from_json(item)
                for item in _required_list(data.get("rules"), "policy.rules")
            ],
        )


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return _required_text(value, "policy rule.row_filter")


def _required_mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return cast(dict[str, Any], value).copy()


def _required_list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    return list(cast(list[object], value))


def _required_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be non-empty text")
    return value


def _non_negative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _effect(value: object) -> Literal["allow", "allow_all"]:
    if value not in {"allow", "allow_all"}:
        raise ValueError("policy rule.effect must be allow or allow_all")
    return cast(Literal["allow", "allow_all"], value)


def _text_list(value: object, label: str) -> list[str]:
    return [_required_text(item, f"{label}[]") for item in _required_list(value, label)]


def _conditions(value: object) -> dict[str, PrincipalConditionValue]:
    conditions = _required_mapping(value, "policy rule.when")
    result: dict[str, PrincipalConditionValue] = {}
    for key, expected in conditions.items():
        name = _required_text(key, "policy rule.when key")
        if isinstance(expected, str):
            result[name] = expected
        elif isinstance(expected, list) and all(isinstance(item, str) for item in expected):
            result[name] = cast(list[str], expected)
        else:
            raise ValueError(f"policy rule.when[{name!r}] must be text or text list")
    return result


def _reject_unknown(data: dict[str, Any], allowed: set[str], label: str) -> None:
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise ValueError(f"{label} has unsupported keys: {', '.join(unknown)}")
