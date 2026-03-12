from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List

from dal_obscura.policy import MaskRule


@dataclass(frozen=True)
class MaskedSelection:
    select_list: List[str]
    masked_columns: List[str]


def _mask_expression(column: str, mask: MaskRule) -> str:
    mask_type = mask.type.lower()
    if mask_type == "null":
        return "NULL"
    if mask_type == "redact":
        value = mask.value or "***"
        return f"'{value}'"
    if mask_type == "hash":
        return f"sha256(CAST({column} AS VARCHAR))"
    if mask_type == "default":
        if mask.value is None:
            raise ValueError("default mask requires a value")
        return f"{mask.value}"
    raise ValueError(f"Unsupported mask type: {mask.type}")


def _apply_nested_mask(path: str, expr: str) -> str:
    parts = path.split(".")
    if len(parts) == 1:
        return expr
    # Build nested struct_update calls from deepest to top.
    # struct_update(parent, 'field', new_value)
    updated = expr
    for depth in range(len(parts) - 1, 0, -1):
        parent = ".".join(parts[:depth])
        field = parts[depth]
        updated = f"struct_update({parent}, '{field}', {updated})"
    return updated


def build_select_list(
    columns: Iterable[str],
    masks: Dict[str, MaskRule],
) -> MaskedSelection:
    select_list: List[str] = []
    masked_columns: List[str] = []

    for column in columns:
        if column in masks:
            expr = _mask_expression(column, masks[column])
            expr = _apply_nested_mask(column, expr)
            select_list.append(f'{expr} AS "{column}"')
            masked_columns.append(column)
        else:
            select_list.append(f'{column} AS "{column}"')

    return MaskedSelection(select_list=select_list, masked_columns=masked_columns)
