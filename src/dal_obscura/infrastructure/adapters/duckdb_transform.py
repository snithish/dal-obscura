from __future__ import annotations

from typing import Iterable, Mapping

import duckdb
import pyarrow as pa

from dal_obscura.application.ports import MaskedSelection
from dal_obscura.domain.access_control import MaskRule


class DefaultMaskingAdapter:
    def apply(self, columns: Iterable[str], masks: Mapping[str, MaskRule]) -> MaskedSelection:
        return _build_select_list(columns, masks)

    def masked_schema(
        self, base_schema: pa.Schema, columns: Iterable[str], masks: Mapping[str, MaskRule]
    ) -> pa.Schema:
        selected_fields: list[pa.Field] = []
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


class DuckDBRowTransformAdapter:
    def __init__(self, masking: DefaultMaskingAdapter) -> None:
        self._masking = masking

    def apply_filters_and_masks_stream(
        self,
        batches: Iterable[pa.RecordBatch],
        columns: Iterable[str],
        row_filter: str | None,
        masks: Mapping[str, MaskRule],
    ) -> Iterable[pa.RecordBatch]:
        query = _build_query(columns, row_filter, masks, self._masking)
        con = duckdb.connect()
        try:
            for batch in batches:
                con.register("input", batch)
                try:
                    reader = con.execute(query).to_arrow_reader()
                    yield from reader
                finally:
                    con.unregister("input")
        finally:
            con.close()


def _build_query(
    columns: Iterable[str],
    row_filter: str | None,
    masks: Mapping[str, MaskRule],
    masking: DefaultMaskingAdapter,
) -> str:
    selection = masking.apply(columns, masks)
    query = f"SELECT {', '.join(selection.select_list)} FROM input"
    if row_filter:
        query += f" WHERE {row_filter}"
    return query


def _build_select_list(columns: Iterable[str], masks: Mapping[str, MaskRule]) -> MaskedSelection:
    select_list: list[str] = []
    masked_columns: list[str] = []

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


def _mask_expression(column: str, mask: MaskRule) -> str:
    mask_type = mask.type.lower()
    if mask_type == "null":
        return "NULL"
    if mask_type == "redact":
        return _sql_literal(str(mask.value or "***"))
    if mask_type == "hash":
        return f"sha256(CAST({column} AS VARCHAR))"
    if mask_type == "default":
        if mask.value is None:
            raise ValueError("default mask requires a value")
        return _sql_literal(mask.value)
    raise ValueError(f"Unsupported mask type: {mask.type}")


def _apply_nested_mask(path: str, expr: str) -> str:
    parts = path.split(".")
    if len(parts) == 1:
        return expr
    return _apply_nested_mask_on_root(parts[0], path, expr)


def _apply_nested_mask_on_root(root_expr: str, path: str, expr: str) -> str:
    parts = path.split(".")
    if len(parts) == 1:
        return expr
    updated = expr
    for depth in range(len(parts) - 1, 0, -1):
        field = parts[depth]
        if depth == 1:
            base = f"({root_expr})"
        else:
            base = f"({root_expr}).{'.'.join(parts[1:depth])}"
        updated = f'struct_update({base}, "{field}" := {updated})'
    return updated


def _masked_field(field: pa.Field, mask: MaskRule | None) -> pa.Field:
    if not mask:
        return field
    mask_type = mask.type.lower()
    if mask_type in {"hash", "redact"}:
        return pa.field(field.name, pa.string(), nullable=True)
    return field


def _sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    raise ValueError("default mask requires a scalar literal")
