from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from itertools import chain

import duckdb
import pyarrow as pa

from dal_obscura.application.ports.masking import MaskedSelection
from dal_obscura.domain.access_control.filters import RowFilter, row_filter_to_sql
from dal_obscura.domain.access_control.models import MaskRule

_DUCKDB_ARROW_OUTPUT_BATCH_SIZE = 8_192


class DefaultMaskingAdapter:
    """Builds DuckDB projection expressions and the schema they imply."""

    def apply(self, columns: Iterable[str], masks: Mapping[str, MaskRule]) -> MaskedSelection:
        """Returns the DuckDB SELECT list for the requested columns and masks."""
        return _build_select_list(columns, masks)

    def masked_schema(
        self, base_schema: pa.Schema, columns: Iterable[str], masks: Mapping[str, MaskRule]
    ) -> pa.Schema:
        """Projects the schema visible to clients after masking is applied."""
        selected_fields: list[pa.Field] = []
        seen: set[str] = set()

        for column in columns:
            if column == "*":
                for field in base_schema:
                    if field.name not in seen:
                        selected_fields.append(_masked_field(field, field.name, masks))
                        seen.add(field.name)
                continue

            if column in seen:
                continue
            selected_fields.append(_selected_field(base_schema, column, masks))
            seen.add(column)

        return pa.schema(selected_fields)


class DuckDBRowTransformAdapter:
    """Applies row filters and masks to streamed Arrow batches via DuckDB SQL."""

    def __init__(self, masking: DefaultMaskingAdapter) -> None:
        self._masking = masking

    def apply_filters_and_masks_stream(
        self,
        batches: Iterable[pa.RecordBatch],
        columns: Iterable[str],
        row_filter: RowFilter | None,
        masks: Mapping[str, MaskRule],
    ) -> Iterable[pa.RecordBatch]:
        """Builds a transient DuckDB query and streams transformed record batches."""
        query = _build_query(columns, row_filter, masks, self._masking)

        batch_iter = iter(batches)
        try:
            first_batch = next(batch_iter)
        except StopIteration:
            return iter(())

        reader = pa.RecordBatchReader.from_batches(
            first_batch.schema,
            chain((first_batch,), batch_iter),
        )
        return _stream_query_results(reader, query)


def _stream_query_results(
    reader: pa.RecordBatchReader,
    query: str,
) -> Iterator[pa.RecordBatch]:
    """Executes the generated SQL over the incoming Arrow reader."""
    # DuckDB 1.5.0 removes the Python-side per-batch loop here, but the input side
    # does not appear observably lazy enough to assert callback-order streaming.
    con = duckdb.connect()
    try:
        result_reader = (
            con.from_arrow(reader)
            .query("input", query)
            .to_arrow_reader(batch_size=_DUCKDB_ARROW_OUTPUT_BATCH_SIZE)
        )
        yield from result_reader
    finally:
        con.close()


def _build_query(
    columns: Iterable[str],
    row_filter: RowFilter | None,
    masks: Mapping[str, MaskRule],
    masking: DefaultMaskingAdapter,
) -> str:
    """Builds the SQL statement used to apply projection, masks, and filters."""
    selection = masking.apply(columns, masks)
    query = f"SELECT {', '.join(selection.select_list)} FROM input"
    if row_filter:
        query += f" WHERE {row_filter_to_sql(row_filter)}"
    return query


def _build_select_list(columns: Iterable[str], masks: Mapping[str, MaskRule]) -> MaskedSelection:
    """Builds projection expressions for both top-level and nested masked fields."""
    select_list: list[str] = []
    masked_columns: list[str] = []

    for column in columns:
        if column in masks:
            expr = _mask_expression(column, masks[column])
            select_list.append(f'{expr} AS "{column}"')
            masked_columns.append(column)
            continue

        nested_masks = {k: v for k, v in masks.items() if _is_descendant_path(k, column)}
        if nested_masks:
            expr = column
            for path, mask in nested_masks.items():
                masked_expr = _mask_expression(path, mask)
                # Nested masks are applied from the leaf upward so each struct update
                # wraps the previous expression in the correct order.
                expr = _apply_nested_mask_on_root(expr, column, path, masked_expr)
                masked_columns.append(path)
            select_list.append(f'{expr} AS "{column}"')
        else:
            select_list.append(f'{column} AS "{column}"')

    return MaskedSelection(select_list=select_list, masked_columns=masked_columns)


def _mask_expression(column: str, mask: MaskRule) -> str:
    """Returns the DuckDB SQL fragment for a single mask rule."""
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


def _is_descendant_path(path: str, parent: str) -> bool:
    """Returns whether `path` is nested underneath `parent`."""
    return path.startswith(f"{parent}.")


def _apply_nested_mask_on_root(root_expr: str, root_path: str, path: str, expr: str) -> str:
    """Applies a nested field replacement on top of a selected struct expression."""
    relative_parts = path.split(".")[len(root_path.split(".")) :]
    if not relative_parts:
        return expr

    updated = expr
    for depth in range(len(relative_parts) - 1, -1, -1):
        field = relative_parts[depth]
        base = (
            f"({root_expr})" if depth == 0 else f"({root_expr}).{'.'.join(relative_parts[:depth])}"
        )
        updated = f'struct_update({base}, "{field}" := {updated})'
    return updated


def _selected_field(base_schema: pa.Schema, column: str, masks: Mapping[str, MaskRule]) -> pa.Field:
    """Returns the visible field for a requested top-level or nested column path."""
    field = _field_for_path(base_schema, column)
    masked = _masked_field(field, column, masks)
    return pa.field(column, masked.type, nullable=masked.nullable, metadata=masked.metadata)


def _field_for_path(schema: pa.Schema, path: str) -> pa.Field:
    """Resolves a top-level or nested field path from the Arrow schema."""
    parts = path.split(".")
    field = schema.field(parts[0])
    for part in parts[1:]:
        field = field.type.field(part)
    return field


def _masked_field(field: pa.Field, path: str, masks: Mapping[str, MaskRule]) -> pa.Field:
    """Adjusts the visible field type when masking changes the value representation."""
    mask = masks.get(path)
    if mask is not None:
        return _masked_leaf_field(field, mask)

    if not pa.types.is_struct(field.type):
        return field

    nested_fields = [_masked_field(child, f"{path}.{child.name}", masks) for child in field.type]
    if all(
        original.equals(updated)
        for original, updated in zip(field.type, nested_fields, strict=False)
    ):
        return field
    return pa.field(
        field.name,
        pa.struct(nested_fields),
        nullable=field.nullable,
        metadata=field.metadata,
    )


def _masked_leaf_field(field: pa.Field, mask: MaskRule) -> pa.Field:
    """Adjusts the field type for a direct mask attached to the selected path."""
    mask_type = mask.type.lower()
    if mask_type in {"hash", "redact"}:
        return pa.field(field.name, pa.string(), nullable=True)
    return field


def _sql_literal(value: object) -> str:
    """Serializes a Python scalar into a DuckDB SQL literal."""
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
