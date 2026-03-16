from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml

from dal_obscura.domain.access_control.models import (
    AccessDecision,
    AccessRule,
    DatasetPolicy,
    MaskRule,
    Policy,
    Principal,
)
from dal_obscura.domain.access_control.policy_resolution import dataset_version, resolve_access
from dal_obscura.domain.query_planning.models import DatasetSelector


class PolicyFileAuthorizer:
    """Authorization adapter backed by a YAML or JSON policy document on disk."""

    def __init__(self, policy_path: str | Path) -> None:
        self._policy_path = Path(policy_path)

    def authorize(
        self,
        principal: Principal,
        dataset: DatasetSelector,
        requested_columns: Iterable[str],
    ) -> AccessDecision:
        """Reloads the policy file and resolves the effective access decision."""
        policy = load_policy_file(self._policy_path)
        allowed_columns, masks, row_filter = resolve_access(
            policy,
            principal,
            dataset,
            requested_columns,
        )
        matched_dataset = policy.match_dataset(dataset)
        if not matched_dataset:
            raise PermissionError("No policy for requested table")
        return AccessDecision(
            allowed_columns=allowed_columns,
            masks=masks,
            row_filter=row_filter,
            policy_version=dataset_version(matched_dataset),
        )

    def current_policy_version(self, dataset: DatasetSelector) -> int | None:
        """Returns the current dataset version hash used to invalidate old tickets."""
        policy = load_policy_file(self._policy_path)
        matched_dataset = policy.match_dataset(dataset)
        if not matched_dataset:
            return None
        return dataset_version(matched_dataset)


def load_policy_file(path: str | Path) -> Policy:
    """Loads the policy file and normalizes both catalog and raw-path rules."""
    policy_path = Path(path)
    if not policy_path.exists():
        raise FileNotFoundError(policy_path)

    suffix = policy_path.suffix.lower()
    if suffix in {".yml", ".yaml"}:
        raw = yaml.safe_load(policy_path.read_text())
    elif suffix == ".json":
        raw = json.loads(policy_path.read_text())
    else:
        raise ValueError("Policy file must be .yaml, .yml, or .json")

    if not isinstance(raw, dict):
        raise ValueError("Policy file must contain a JSON/YAML object")
    version = int(raw.get("version", 1))
    datasets = _merge_datasets(
        [
            *_parse_catalog_datasets(raw.get("catalogs", {}) or {}),
            *_parse_path_datasets(raw.get("paths", []) or []),
        ]
    )
    return Policy(version=version, datasets=datasets)


def _parse_catalog_datasets(raw: dict[str, Any]) -> list[DatasetPolicy]:
    """Extracts dataset policies scoped under named catalogs."""
    datasets: list[DatasetPolicy] = []
    for catalog_name, catalog_data in dict(raw).items():
        if not isinstance(catalog_data, dict):
            raise ValueError(f"Catalog policy {catalog_name!r} must be an object")
        targets = dict(catalog_data.get("targets", {}) or {})
        for target_name, target_data in targets.items():
            if not isinstance(target_data, dict):
                raise ValueError(f"Target policy {target_name!r} must be an object")
            rules = [_parse_rule(item) for item in target_data.get("rules", [])]
            datasets.append(
                DatasetPolicy(target=str(target_name), catalog=str(catalog_name), rules=rules)
            )
    return datasets


def _parse_path_datasets(raw: list[Any]) -> list[DatasetPolicy]:
    """Extracts dataset policies that apply to direct file path targets."""
    datasets: list[DatasetPolicy] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("Path policy entries must be objects")
        target = item.get("target")
        if not target:
            raise ValueError("Path policy missing 'target'")
        rules = [_parse_rule(rule) for rule in item.get("rules", [])]
        datasets.append(DatasetPolicy(target=str(target), catalog=None, rules=rules))
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


def _parse_rule(raw: dict[str, Any]) -> AccessRule:
    """Normalizes one raw access rule from the policy document."""
    principals = [str(item) for item in raw.get("principals", [])]
    columns = [str(item) for item in raw.get("columns", [])]
    masks_raw = raw.get("masks", {}) or {}
    masks = {str(key): _parse_mask(value) for key, value in masks_raw.items()}
    row_filter = raw.get("row_filter")
    return AccessRule(
        principals=principals,
        columns=columns,
        masks=masks,
        row_filter=str(row_filter) if row_filter else None,
    )


def _parse_mask(value: Any) -> MaskRule:
    """Supports shorthand string masks as well as object-shaped mask rules."""
    if isinstance(value, str):
        return MaskRule(type=value)
    if not isinstance(value, dict):
        raise ValueError("Mask rule must be a string or object")
    mask_type = value.get("type")
    if not mask_type:
        raise ValueError("Mask rule missing 'type'")
    return MaskRule(type=str(mask_type), value=value.get("value"))
