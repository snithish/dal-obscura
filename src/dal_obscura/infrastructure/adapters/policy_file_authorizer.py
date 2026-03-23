from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from dal_obscura.domain.access_control.models import (
    AccessDecision,
    AccessRule,
    DatasetPolicy,
    MaskRule,
    Policy,
    Principal,
)
from dal_obscura.domain.access_control.policy_resolution import dataset_version, resolve_access


class _StrictModel(BaseModel):
    """Base model used by config schemas to reject unknown keys."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class _MaskObjectModel(_StrictModel):
    type: str = Field(min_length=1)
    value: object | None = None


class _AccessRuleModel(_StrictModel):
    principals: list[str] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    masks: dict[str, str | _MaskObjectModel] = Field(default_factory=dict)
    row_filter: str | None = None

    @field_validator("row_filter")
    @classmethod
    def _normalize_row_filter(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class _TargetPolicyModel(_StrictModel):
    rules: list[_AccessRuleModel] = Field(default_factory=list)


class _CatalogPolicyModel(_StrictModel):
    targets: dict[str, _TargetPolicyModel] = Field(default_factory=dict)


class _PathDatasetModel(_StrictModel):
    target: str = Field(min_length=1)
    rules: list[_AccessRuleModel] = Field(default_factory=list)


class _PolicyDocument(_StrictModel):
    version: int = 1
    catalogs: dict[str, _CatalogPolicyModel] = Field(default_factory=dict)
    paths: list[_PathDatasetModel] = Field(default_factory=list)


class PolicyFileAuthorizer:
    """Authorization adapter backed by a YAML policy document on disk."""

    def __init__(self, policy_path: str | Path) -> None:
        self._policy_path = Path(policy_path)

    def authorize(
        self,
        principal: Principal,
        target: str,
        catalog: str | None,
        requested_columns: Iterable[str],
    ) -> AccessDecision:
        """Reloads the policy file and resolves the effective access decision."""
        policy = load_policy_config(self._policy_path)
        allowed_columns, masks, row_filter = resolve_access(
            policy,
            principal,
            target,
            catalog,
            requested_columns,
        )
        matched_dataset = policy.match_dataset(target, catalog)
        if not matched_dataset:
            raise PermissionError("No policy for requested table")
        return AccessDecision(
            allowed_columns=allowed_columns,
            masks=masks,
            row_filter=row_filter,
            policy_version=dataset_version(matched_dataset),
        )

    def current_policy_version(self, target: str, catalog: str | None) -> int | None:
        """Returns the current dataset version hash used to invalidate old tickets."""
        policy = load_policy_config(self._policy_path)
        matched_dataset = policy.match_dataset(target, catalog)
        if not matched_dataset:
            return None
        return dataset_version(matched_dataset)


def load_policy_config(path: str | Path) -> Policy:
    """Loads the policy file and normalizes both catalog and raw-path rules."""
    policy_path = Path(path)
    raw = _load_yaml_object(policy_path, "Policy config")
    try:
        document = _PolicyDocument.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Invalid policy config: {exc}") from exc

    datasets = _merge_datasets(
        [
            *_catalog_datasets_from_model(document.catalogs),
            *_path_datasets_from_model(document.paths),
        ]
    )
    return Policy(version=document.version, datasets=datasets)


def _load_yaml_object(path: Path, label: str) -> dict[str, object]:
    """Loads a YAML object document from disk."""
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() not in {".yaml", ".yml"}:
        raise ValueError(f"{label} must be .yaml or .yml")
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must contain a YAML object")
    return raw


def _catalog_datasets_from_model(raw: dict[str, _CatalogPolicyModel]) -> list[DatasetPolicy]:
    """Extracts dataset policies scoped under named catalogs."""
    datasets: list[DatasetPolicy] = []
    for catalog_name, catalog_data in raw.items():
        for target_name, target_data in catalog_data.targets.items():
            rules = [_rule_from_model(item) for item in target_data.rules]
            datasets.append(DatasetPolicy(target=target_name, catalog=catalog_name, rules=rules))
    return datasets


def _path_datasets_from_model(raw: list[_PathDatasetModel]) -> list[DatasetPolicy]:
    """Extracts dataset policies that apply to direct file path targets."""
    datasets: list[DatasetPolicy] = []
    for item in raw:
        rules = [_rule_from_model(rule) for rule in item.rules]
        datasets.append(DatasetPolicy(target=item.target, catalog=None, rules=rules))
    return datasets


def _merge_datasets(datasets: list[DatasetPolicy]) -> list[DatasetPolicy]:
    """Merges duplicate dataset entries so later files can append more rules."""
    merged: dict[tuple[str | None, str], DatasetPolicy] = {}
    ordered_keys: list[tuple[str | None, str]] = []
    for dataset in datasets:
        key = (dataset.catalog, dataset.target)
        existing = merged.get(key)
        if existing is None:
            merged[key] = dataset
            ordered_keys.append(key)
            continue
        merged[key] = DatasetPolicy(
            target=dataset.target,
            catalog=dataset.catalog,
            rules=[*existing.rules, *dataset.rules],
        )
    return [merged[key] for key in ordered_keys]


def _rule_from_model(raw: _AccessRuleModel) -> AccessRule:
    """Converts one validated access rule into a domain object."""
    principals = [str(item) for item in raw.principals]
    columns = [str(item) for item in raw.columns]
    masks = {str(key): _parse_mask(value) for key, value in raw.masks.items()}
    return AccessRule(
        principals=principals,
        columns=columns,
        masks=masks,
        row_filter=raw.row_filter,
    )


def _parse_mask(value: object) -> MaskRule:
    """Supports shorthand string masks as well as object-shaped mask rules."""
    if isinstance(value, str):
        return MaskRule(type=value)
    if isinstance(value, _MaskObjectModel):
        return MaskRule(type=value.type, value=value.value)
    raise ValueError("Mask rule must be a string or object")
