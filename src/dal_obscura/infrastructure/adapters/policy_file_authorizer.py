from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import yaml

from dal_obscura.domain.access_control import (
    AccessDecision,
    AccessRule,
    DatasetPolicy,
    MaskRule,
    Policy,
    Principal,
    dataset_version,
    resolve_access,
)


class PolicyFileAuthorizer:
    def __init__(self, policy_path: str | Path) -> None:
        self._policy_path = Path(policy_path)

    def authorize(
        self,
        principal: Principal,
        table_identifier: str,
        requested_columns: Iterable[str],
    ) -> AccessDecision:
        policy = load_policy_file(self._policy_path)
        allowed_columns, masks, row_filter = resolve_access(
            policy,
            principal,
            table_identifier,
            requested_columns,
        )
        dataset = policy.match_dataset(table_identifier)
        if not dataset:
            raise PermissionError("No policy for requested table")
        return AccessDecision(
            allowed_columns=allowed_columns,
            masks=masks,
            row_filter=row_filter,
            policy_version=dataset_version(dataset),
        )

    def current_policy_version(self, table_identifier: str) -> int | None:
        policy = load_policy_file(self._policy_path)
        dataset = policy.match_dataset(table_identifier)
        if not dataset:
            return None
        return dataset_version(dataset)


def load_policy_file(path: str | Path) -> Policy:
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
    datasets = [_parse_dataset(item) for item in raw.get("datasets", [])]
    return Policy(version=version, datasets=datasets)


def _parse_dataset(raw: dict[str, Any]) -> DatasetPolicy:
    table = raw.get("table")
    if not table:
        raise ValueError("Dataset missing 'table'")
    rules = [_parse_rule(item) for item in raw.get("rules", [])]
    return DatasetPolicy(table=str(table), rules=rules)


def _parse_rule(raw: dict[str, Any]) -> AccessRule:
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
    if isinstance(value, str):
        return MaskRule(type=value)
    if not isinstance(value, dict):
        raise ValueError("Mask rule must be a string or object")
    mask_type = value.get("type")
    if not mask_type:
        raise ValueError("Mask rule missing 'type'")
    return MaskRule(type=str(mask_type), value=value.get("value"))
