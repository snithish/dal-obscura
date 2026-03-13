from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from .models import AccessRule, DatasetPolicy, MaskRule, Policy


def _parse_mask(value: Any) -> MaskRule:
    if isinstance(value, str):
        return MaskRule(type=value)
    if not isinstance(value, dict):
        raise ValueError("Mask rule must be a string or object")
    mask_type = value.get("type")
    if not mask_type:
        raise ValueError("Mask rule missing 'type'")
    return MaskRule(type=str(mask_type), value=value.get("value"))


def _parse_rule(raw: dict[str, Any]) -> AccessRule:
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


def _parse_dataset(raw: dict[str, Any]) -> DatasetPolicy:
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
    datasets_raw = raw.get("datasets", [])
    datasets = [_parse_dataset(d) for d in datasets_raw]
    return Policy(version=version, datasets=datasets)
