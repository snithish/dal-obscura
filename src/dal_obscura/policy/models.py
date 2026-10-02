from __future__ import annotations

import fnmatch
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, cast

PrincipalConditionValue = str | Sequence[str]


@dataclass(frozen=True)
class MaskRule:
    """Mask definition attached to a column or nested field."""

    type: str
    value: object | None = None
    exempt_principals: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "exempt_principals", tuple(self.exempt_principals))

    def to_json(self) -> dict[str, object]:
        payload: dict[str, object] = {"type": self.type}
        if self.value is not None:
            payload["value"] = self.value
        if self.exempt_principals:
            payload["exempt_principals"] = list(self.exempt_principals)
        return payload

    @classmethod
    def from_json(cls, raw: object) -> MaskRule:
        data = _required_mapping(raw, "mask")
        _reject_unknown(data, {"type", "value", "exempt_principals"}, "mask")
        mask_type = _required_text(data.get("type"), "mask.type")
        exemptions = tuple(_text_list(data.get("exempt_principals", []), "mask.exempt_principals"))
        return cls(type=mask_type, value=data.get("value"), exempt_principals=exemptions)


@dataclass(frozen=True)
class AccessRule:
    """Single explicit grant evaluated for a matching principal token."""

    principals: Sequence[str]
    columns: Sequence[str]
    masks: Mapping[str, MaskRule]
    row_filter: str | None = None
    effect: Literal["allow", "allow_all"] = "allow"
    when: Mapping[str, PrincipalConditionValue] | None = None
    name: str = ""
    description: str = ""
    ordinal: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "principals", tuple(self.principals))
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "masks", MappingProxyType(dict(self.masks)))
        object.__setattr__(self, "when", freeze_conditions(self.when))

    def to_json(self) -> dict[str, object]:
        return {
            "ordinal": self.ordinal,
            "name": self.name,
            "description": self.description,
            "principals": list(self.principals),
            "columns": list(self.columns),
            "effect": self.effect,
            "when": conditions_to_json(self.when),
            "masks": {column: mask.to_json() for column, mask in self.masks.items()},
            "row_filter": self.row_filter,
        }

    @classmethod
    def from_json(cls, raw: object) -> AccessRule:
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
                _required_text(column, "policy rule mask path"): MaskRule.from_json(mask)
                for column, mask in masks.items()
            },
            row_filter=_optional_str(data.get("row_filter")),
            when=_conditions(data.get("when")),
            name=str(data.get("name", "")),
            description=str(data.get("description", "")),
        )


@dataclass(frozen=True)
class DatasetPolicy:
    """Policy bundle for one catalog/target pair or wildcard target."""

    target: str
    catalog: str | None
    rules: Sequence[AccessRule]

    def __post_init__(self) -> None:
        object.__setattr__(self, "rules", tuple(self.rules))


@dataclass(frozen=True)
class Policy:
    """In-memory representation of the full authorization document."""

    version: int
    datasets: Sequence[DatasetPolicy]

    def __post_init__(self) -> None:
        object.__setattr__(self, "datasets", tuple(self.datasets))

    def match_dataset(self, target: str, catalog: str | None) -> DatasetPolicy | None:
        """Returns the first dataset policy whose catalog and target glob match."""
        for dataset in self.datasets:
            if dataset.catalog != catalog:
                continue
            if fnmatch.fnmatch(target, dataset.target):
                return dataset
        return None


@dataclass(frozen=True)
class Principal:
    """Authenticated caller plus any groups and free-form identity attributes."""

    id: str
    groups: Sequence[str]
    attributes: Mapping[str, str]
    issuer: str = ""
    expires_at: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "groups", tuple(self.groups))
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))

    def tokens(self) -> list[str]:
        """Returns tokens used by policy matching, including `group:` prefixes."""
        return [self.id, *[f"group:{group}" for group in self.groups]]


@dataclass(frozen=True)
class AccessDecision:
    """Concrete authorization outcome consumed by planning and fetch paths."""

    allowed_columns: Sequence[str]
    masks: Mapping[str, MaskRule]
    row_filter: str | None
    policy_version: int
    asset_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "allowed_columns", tuple(self.allowed_columns))
        object.__setattr__(self, "masks", MappingProxyType(dict(self.masks)))


def freeze_conditions(
    conditions: Mapping[str, PrincipalConditionValue] | None,
) -> Mapping[str, PrincipalConditionValue] | None:
    """Detach condition lists and expose only immutable policy values."""
    if conditions is None:
        return None
    return MappingProxyType(
        {
            key: value if isinstance(value, str) else tuple(value)
            for key, value in conditions.items()
        }
    )


def conditions_to_json(
    conditions: Mapping[str, PrincipalConditionValue] | None,
) -> dict[str, str | list[str]]:
    """Write explicit JSON arrays rather than leaking internal immutable values."""
    return {
        key: value if isinstance(value, str) else list(value)
        for key, value in (conditions or {}).items()
    }


@dataclass(frozen=True)
class AssetPolicy:
    """Compiled policy snapshot for one governed asset.

    Example:
        ```python
        compiled = AssetPolicy(version=1, catalog="analytics", target="orders", rules=[])
        policy = compiled.to_policy()
        ```
    """

    version: int
    catalog: str
    target: str
    rules: Sequence[AccessRule]

    def __post_init__(self) -> None:
        object.__setattr__(self, "rules", tuple(self.rules))

    def to_policy(self) -> Policy:
        return Policy(
            version=self.version,
            datasets=[
                DatasetPolicy(
                    catalog=self.catalog,
                    target=self.target,
                    rules=self.rules,
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
    def from_json(cls, raw: object) -> AssetPolicy:
        data = _required_mapping(raw, "policy")
        _reject_unknown(data, {"version", "catalog", "target", "rules"}, "policy")
        return cls(
            version=_non_negative_int(data.get("version"), "policy.version"),
            catalog=_required_text(data.get("catalog"), "policy.catalog"),
            target=_required_text(data.get("target"), "policy.target"),
            rules=[
                AccessRule.from_json(item)
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
