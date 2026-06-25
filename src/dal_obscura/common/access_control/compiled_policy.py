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
    type: str
    value: object | None = None

    def to_mask_rule(self) -> MaskRule:
        return MaskRule(type=self.type, value=self.value)

    def to_json(self) -> dict[str, object]:
        payload: dict[str, object] = {"type": self.type}
        if self.value is not None:
            payload["value"] = self.value
        return payload

    @classmethod
    def from_json(cls, raw: object) -> CompiledMaskRule:
        data = _mapping(raw)
        return cls(type=str(data["type"]), value=data.get("value"))


@dataclass(frozen=True)
class CompiledPolicyRule:
    ordinal: int
    effect: Literal["allow", "deny"]
    principals: list[str]
    columns: list[str]
    masks: dict[str, CompiledMaskRule]
    row_filter: str | None = None
    when: dict[str, PrincipalConditionValue] = field(default_factory=dict)

    def to_access_rule(self) -> AccessRule:
        return AccessRule(
            principals=list(self.principals),
            columns=list(self.columns),
            masks={column: mask.to_mask_rule() for column, mask in self.masks.items()},
            row_filter=self.row_filter,
            effect=self.effect,
            when=dict(self.when),
        )

    def to_json(self) -> dict[str, object]:
        return {
            "ordinal": self.ordinal,
            "principals": list(self.principals),
            "columns": list(self.columns),
            "effect": self.effect,
            "when": dict(self.when),
            "masks": {column: mask.to_json() for column, mask in self.masks.items()},
            "row_filter": self.row_filter,
        }

    @classmethod
    def from_json(cls, raw: object) -> CompiledPolicyRule:
        data = _mapping(raw)
        return cls(
            ordinal=int(data.get("ordinal", 0)),
            effect=cast(Literal["allow", "deny"], str(data.get("effect", "allow"))),
            principals=[str(item) for item in _list(data.get("principals"))],
            columns=[str(item) for item in _list(data.get("columns"))],
            masks={
                str(column): CompiledMaskRule.from_json(mask)
                for column, mask in _mapping(data.get("masks")).items()
                if isinstance(column, str) and isinstance(mask, dict) and "type" in mask
            },
            row_filter=_optional_str(data.get("row_filter")),
            when=cast(dict[str, PrincipalConditionValue], _mapping(data.get("when"))),
        )


@dataclass(frozen=True)
class CompiledPolicy:
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
    def from_json(
        cls,
        raw: object,
        *,
        version: int | None = None,
        catalog: str | None = None,
        target: str | None = None,
    ) -> CompiledPolicy:
        data = _mapping(raw)
        return cls(
            version=int(data.get("version", version or 0)),
            catalog=str(data.get("catalog", catalog or "")),
            target=str(data.get("target", target or "")),
            rules=[CompiledPolicyRule.from_json(item) for item in _list(data.get("rules"))],
        )


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value).copy()
    return {}


def _list(value: object) -> list[object]:
    if isinstance(value, list):
        return list(cast(list[object], value))
    return []


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
