from __future__ import annotations

import fnmatch
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml


@dataclass(frozen=True)
class MaskRule:
    type: str
    value: Optional[str] = None


@dataclass(frozen=True)
class AccessRule:
    principals: List[str]
    columns: List[str]
    masks: Dict[str, MaskRule]
    row_filter: Optional[str]


@dataclass(frozen=True)
class DatasetPolicy:
    table: str
    rules: List[AccessRule]


@dataclass(frozen=True)
class Policy:
    version: int
    datasets: List[DatasetPolicy]

    def match_dataset(self, table_identifier: str) -> Optional[DatasetPolicy]:
        for dataset in self.datasets:
            if fnmatch.fnmatch(table_identifier, dataset.table):
                return dataset
        return None


@dataclass(frozen=True)
class Principal:
    id: str
    groups: List[str]
    attributes: Dict[str, str]

    def tokens(self) -> List[str]:
        return [self.id, *[f"group:{g}" for g in self.groups]]


def _parse_mask(value: Any) -> MaskRule:
    if isinstance(value, str):
        return MaskRule(type=value)
    if not isinstance(value, dict):
        raise ValueError("Mask rule must be a string or object")
    mask_type = value.get("type")
    if not mask_type:
        raise ValueError("Mask rule missing 'type'")
    return MaskRule(type=str(mask_type), value=value.get("value"))


def _parse_rule(raw: Dict[str, Any]) -> AccessRule:
    principals = [str(p) for p in raw.get("principals", [])]
    columns = [str(c) for c in raw.get("columns", [])]
    masks_raw = raw.get("masks", {}) or {}
    masks = {str(k): _parse_mask(v) for k, v in masks_raw.items()}
    row_filter = raw.get("row_filter")
    return AccessRule(
        principals=principals,
        columns=columns,
        masks=masks,
        row_filter=str(row_filter) if row_filter else None,
    )


def _parse_dataset(raw: Dict[str, Any]) -> DatasetPolicy:
    table = raw.get("table")
    if not table:
        raise ValueError("Dataset missing 'table'")
    rules_raw = raw.get("rules", [])
    rules = [_parse_rule(r) for r in rules_raw]
    return DatasetPolicy(table=str(table), rules=rules)


def load_policy(path: str | Path) -> Policy:
    policy_path = Path(path)
    if not policy_path.exists():
        raise FileNotFoundError(policy_path)
    if policy_path.suffix.lower() in {".yml", ".yaml"}:
        raw = yaml.safe_load(policy_path.read_text())
    elif policy_path.suffix.lower() == ".json":
        raw = json.loads(policy_path.read_text())
    else:
        raise ValueError("Policy file must be .yaml, .yml, or .json")
    if not isinstance(raw, dict):
        raise ValueError("Policy file must contain a JSON/YAML object")
    version = int(raw.get("version", 1))
    datasets_raw = raw.get("datasets", [])
    datasets = [_parse_dataset(d) for d in datasets_raw]
    return Policy(version=version, datasets=datasets)


def resolve_access(
    policy: Policy,
    principal: Principal,
    table_identifier: str,
    requested_columns: Iterable[str],
) -> tuple[List[str], Dict[str, MaskRule], Optional[str]]:
    dataset = policy.match_dataset(table_identifier)
    if not dataset:
        raise PermissionError("No policy for requested table")

    principal_tokens = set(principal.tokens())
    allowed_columns: List[str] = []
    masks: Dict[str, MaskRule] = {}
    row_filters: List[str] = []

    for rule in dataset.rules:
        if not principal_tokens.intersection(rule.principals):
            continue
        if "*" in rule.columns:
            allowed_columns = list(requested_columns)
        else:
            allowed_columns = [c for c in requested_columns if c in rule.columns]
        for col, mask in rule.masks.items():
            masks[col] = mask
        if rule.row_filter:
            row_filters.append(rule.row_filter)

    if not allowed_columns:
        raise PermissionError("No allowed columns for principal")

    combined_filter = " AND ".join(f"({f})" for f in row_filters) if row_filters else None
    return allowed_columns, masks, combined_filter
