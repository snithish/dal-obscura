from __future__ import annotations

from typing import Iterable, List, Mapping

import pyarrow as pa

from dal_obscura.policy.models import MaskRule

from .base import MaskApplier, MaskedSelection


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
    updated = expr
    for depth in range(len(parts) - 1, 0, -1):
        parent = ".".join(parts[:depth])
        field = parts[depth]
        updated = f'struct_update({parent}, "{field}" := {updated})'
    return updated


def build_select_list(columns: Iterable[str], masks: Mapping[str, MaskRule]) -> MaskedSelection:
    select_list: List[str] = []
    masked_columns: List[str] = []

    for column in columns:
        if column in masks:
            expr = _mask_expression(column, masks[column])
            expr = _apply_nested_mask(column, expr)
            select_list.append(f'{expr} AS "{column}"')
            masked_columns.append(column)
            continue

        nested_masks = {k: v for k, v in masks.items() if k.startswith(f"{column}.")}
        if nested_masks:
            expr = column
            for path, mask in nested_masks.items():
                masked_expr = _mask_expression(path, mask)
                expr = _apply_nested_mask_on_root(expr, path, masked_expr)
                masked_columns.append(path)
            select_list.append(f'{expr} AS "{column}"')
        else:
            select_list.append(f'{column} AS "{column}"')

    return MaskedSelection(select_list=select_list, masked_columns=masked_columns)


class DefaultMaskApplier(MaskApplier):
    def apply(self, columns: Iterable[str], masks: Mapping[str, MaskRule]) -> MaskedSelection:
        return build_select_list(columns, masks)

    def masked_schema(
        self, base_schema: pa.Schema, columns: Iterable[str], masks: Mapping[str, MaskRule]
    ) -> pa.Schema:
        selected_fields: List[pa.Field] = []
        seen: set[str] = set()

        for column in columns:
            if column == "*":
                for field in base_schema:
                    if field.name not in seen:
                        selected_fields.append(_masked_field(field, masks.get(field.name)))
                        seen.add(field.name)
                continue

            top_level = column.split(".")[0]
            if top_level in seen:
                continue
            field = base_schema.field(top_level)
            selected_fields.append(_masked_field(field, masks.get(top_level)))
            seen.add(top_level)

        return pa.schema(selected_fields)


def _masked_field(field: pa.Field, mask: MaskRule | None) -> pa.Field:
    if not mask:
        return field
    mask_type = mask.type.lower()
    if mask_type in {"hash", "redact"}:
        return pa.field(field.name, pa.string(), nullable=True)
    return field


def _apply_nested_mask_on_root(root_expr: str, path: str, expr: str) -> str:
    updated = _apply_nested_mask(path, expr)
    root = path.split(".")[0]
    if root_expr != root:
        updated = updated.replace(root, f"({root_expr})")
    return updated
