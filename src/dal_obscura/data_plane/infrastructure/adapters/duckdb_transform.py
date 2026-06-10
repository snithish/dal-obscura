from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from functools import lru_cache
from itertools import chain

import duckdb
import pyarrow as pa

from dal_obscura.common.access_control.filters import RowFilter, row_filter_to_sql
from dal_obscura.common.access_control.models import MaskRule
from dal_obscura.data_plane.application.ports.masking import MaskedSelection

_DUCKDB_ARROW_OUTPUT_BATCH_SIZE = 8_192
_DUCKDB_TRANSFORM_CONFIG: dict[str, str | bool | int | float | list[str]] = {
    "enable_external_access": "false",
    "autoload_known_extensions": "false",
    "autoinstall_known_extensions": "false",
}


class DefaultMaskingAdapter:
    """Builds DuckDB projection expressions and the schema they imply."""

    def apply(
        self,
        base_schema: pa.Schema,
        columns: Iterable[str],
        masks: Mapping[str, MaskRule],
    ) -> MaskedSelection:
        """Returns the DuckDB SELECT list for the requested columns and masks."""
        return _build_select_list(base_schema, columns, masks)

    def masked_schema(
        self, base_schema: pa.Schema, columns: Iterable[str], masks: Mapping[str, MaskRule]
    ) -> pa.Schema:
        """Projects the schema visible to clients after masking is applied."""
        selected_fields: list[pa.Field] = []
        seen: set[str] = set()
        projection = _build_projection(columns)

        for column, nested_projection in projection:
            if column == "*":
                for field in base_schema:
                    if field.name not in seen:
                        selected_fields.append(_masked_field(field, field.name, masks))
                        seen.add(field.name)
                continue

            if column in seen:
                continue
            if nested_projection is None:
                selected_fields.append(_selected_field(base_schema, column, masks))
            else:
                selected_fields.append(
                    _projected_nested_field(
                        base_schema.field(column),
                        column,
                        nested_projection,
                        masks,
                    )
                )
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
        batch_iter = iter(batches)
        try:
            first_batch = next(batch_iter)
        except StopIteration:
            return iter(())
        query = _build_query(first_batch.schema, columns, row_filter, masks, self._masking)

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
    con = _connect()
    try:
        result_reader = (
            con.from_arrow(reader)
            .query("input", query)
            .to_arrow_reader(batch_size=_DUCKDB_ARROW_OUTPUT_BATCH_SIZE)
        )
        yield from result_reader
    finally:
        con.close()


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config=_DUCKDB_TRANSFORM_CONFIG.copy())
    con.execute("SET enable_progress_bar = false")
    return con


def _build_query(
    base_schema: pa.Schema,
    columns: Iterable[str],
    row_filter: RowFilter | None,
    masks: Mapping[str, MaskRule],
    masking: DefaultMaskingAdapter,
) -> str:
    """Builds the SQL statement used to apply projection, masks, and filters."""
    selection = masking.apply(base_schema, columns, masks)
    query = f"SELECT {', '.join(selection.select_list)} FROM input"
    if row_filter:
        query += f" WHERE {row_filter_to_sql(row_filter)}"
    return query


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _column_reference(path: str) -> str:
    return ".".join(_quote_identifier(part) for part in path.split("."))


def _build_select_list(
    base_schema: pa.Schema,
    columns: Iterable[str],
    masks: Mapping[str, MaskRule],
) -> MaskedSelection:
    """Builds projection expressions for both top-level and nested masked fields."""
    select_list: list[str] = []
    masked_columns: list[str] = []
    projection = _build_projection(columns)

    for column, nested_projection in projection:
        if nested_projection is not None:
            field = base_schema.field(column)
            expr = _nested_projection_expression(
                _quote_identifier(column),
                column,
                field.type,
                nested_projection,
                masks,
            )
            select_list.append(f"{expr} AS {_quote_identifier(column)}")
            masked_columns.extend(
                sorted(
                    mask_path
                    for mask_path in masks
                    if _projection_contains(column, nested_projection, mask_path)
                )
            )
            continue

        nested_masks = {k: v for k, v in masks.items() if _is_descendant_path(k, column)}
        if column in masks:
            expr = _mask_expression(_column_reference(column), masks[column])
            select_list.append(f"{expr} AS {_quote_identifier(column)}")
            masked_columns.append(column)
            continue

        if nested_masks:
            expr = _apply_nested_masks(
                _column_reference(column),
                column,
                _field_for_path(base_schema, column).type,
                masks,
            )
            masked_columns.extend(sorted(nested_masks))
            select_list.append(f"{expr} AS {_quote_identifier(column)}")
        else:
            select_list.append(f"{_column_reference(column)} AS {_quote_identifier(column)}")

    return MaskedSelection(select_list=select_list, masked_columns=masked_columns)


ProjectionTree = dict[str, "ProjectionTree"]


def _build_projection(columns: Iterable[str]) -> list[tuple[str, ProjectionTree | None]]:
    """Groups dotted requested paths into top-level Arrow fields."""
    projection: list[tuple[str, ProjectionTree | None]] = []
    by_top_level: dict[str, ProjectionTree | None] = {}

    for column in columns:
        top_level, *nested = column.split(".")
        if top_level not in by_top_level:
            tree: ProjectionTree | None = {} if nested else None
            by_top_level[top_level] = tree
            projection.append((top_level, tree))

        tree = by_top_level[top_level]
        if tree is None:
            continue
        if not nested:
            by_top_level[top_level] = None
            for index, (name, _existing) in enumerate(projection):
                if name == top_level:
                    projection[index] = (top_level, None)
                    break
            continue
        _insert_projection_path(tree, nested)

    return projection


def _insert_projection_path(tree: ProjectionTree, parts: list[str]) -> None:
    current = tree
    for part in parts:
        current = current.setdefault(part, {})


def _projection_contains(top_level: str, tree: ProjectionTree, path: str) -> bool:
    parts = path.split(".")
    if not parts or parts[0] != top_level:
        return False
    current = tree
    for part in parts[1:]:
        next_tree = current.get(part)
        if next_tree is None:
            return False
        current = next_tree
    return True


def _nested_projection_expression(
    expr: str,
    path: str,
    data_type: pa.DataType,
    projection: ProjectionTree,
    masks: Mapping[str, MaskRule],
    *,
    item_var: str = "_item",
) -> str:
    if pa.types.is_struct(data_type):
        fields: list[str] = []
        for child_name, child_projection in projection.items():
            child = data_type.field(child_name)
            child_path = f"{path}.{child.name}"
            child_expr = f"({expr}).{_quote_identifier(child.name)}"
            projected_child = _nested_projection_leaf_or_struct(
                child_expr,
                child_path,
                child.type,
                child_projection,
                masks,
            )
            fields.append(f"{_quote_identifier(child.name)} := {projected_child}")
        packed = f"struct_pack({', '.join(fields)})"
        return f"CASE WHEN {expr} IS NULL THEN NULL ELSE {packed} END"

    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        child_var = f"{item_var}_{len(path.split('.'))}"
        value_field = data_type.value_field
        transformed = _nested_projection_leaf_or_struct(
            child_var,
            path,
            value_field.type,
            projection,
            masks,
            item_var=child_var,
        )
        return f"list_transform({expr}, {child_var} -> {transformed})"

    direct_mask = masks.get(path)
    if direct_mask is not None:
        return _mask_expression(expr, direct_mask)
    if _has_descendant_mask(path, masks):
        return _apply_nested_masks(expr, path, data_type, masks)
    return expr


def _nested_projection_leaf_or_struct(
    expr: str,
    path: str,
    data_type: pa.DataType,
    projection: ProjectionTree,
    masks: Mapping[str, MaskRule],
    *,
    item_var: str = "_item",
) -> str:
    if projection:
        return _nested_projection_expression(
            expr,
            path,
            data_type,
            projection,
            masks,
            item_var=item_var,
        )
    direct_mask = masks.get(path)
    if direct_mask is not None:
        return _mask_expression(expr, direct_mask)
    if _has_descendant_mask(path, masks):
        return _apply_nested_masks(expr, path, data_type, masks)
    return expr


def _mask_expression(expr: str, mask: MaskRule) -> str:
    """Returns the DuckDB SQL fragment for a single mask rule."""
    mask_type = mask.type.lower()
    if mask_type == "null":
        return "NULL"
    if mask_type == "redact":
        return _sql_literal(str(mask.value or "***"))
    if mask_type == "hash":
        return f"sha256(CAST({expr} AS VARCHAR))"
    if mask_type == "email":
        return f"regexp_replace(CAST({expr} AS VARCHAR), '(^.).*(@.*$)', '\\1***\\2')"
    if mask_type == "keep_last":
        if not isinstance(mask.value, int) or mask.value < 0:
            raise ValueError("keep_last mask requires a non-negative integer value")
        text_column = f"CAST({expr} AS VARCHAR)"
        keep = mask.value
        return (
            "CASE "
            f"WHEN length({text_column}) <= {keep} THEN {text_column} "
            "ELSE "
            f"repeat('*', greatest(length({text_column}) - {keep}, 0)) "
            f"|| right({text_column}, {keep}) "
            "END"
        )
    if mask_type == "default":
        if mask.value is None:
            raise ValueError("default mask requires a value")
        return _sql_literal(mask.value)
    raise ValueError(f"Unsupported mask type: {mask.type}")


def _is_descendant_path(path: str, parent: str) -> bool:
    """Returns whether `path` is nested underneath `parent`."""
    return path.startswith(f"{parent}.")


def _apply_nested_masks(
    expr: str,
    path: str,
    data_type: pa.DataType,
    masks: Mapping[str, MaskRule],
    *,
    item_var: str = "_item",
) -> str:
    direct_mask = masks.get(path)
    if direct_mask is not None:
        return _mask_expression(expr, direct_mask)

    if pa.types.is_struct(data_type):
        updated_expr = expr
        for child in data_type:
            child_path = f"{path}.{child.name}"
            if not _has_mask_for_path(child_path, masks):
                continue
            child_expr = _apply_nested_masks(
                f"({updated_expr}).{_quote_identifier(child.name)}",
                child_path,
                child.type,
                masks,
                item_var=item_var,
            )
            updated_expr = (
                f"struct_update({updated_expr}, {_quote_identifier(child.name)} := {child_expr})"
            )
        return updated_expr

    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        value_field = data_type.value_field
        if not _has_descendant_mask(path, masks):
            return expr
        child_var = f"{item_var}_{len(path.split('.'))}"
        transformed = _apply_nested_masks(
            child_var,
            path,
            value_field.type,
            masks,
            item_var=child_var,
        )
        return f"list_transform({expr}, {child_var} -> {transformed})"

    return expr


def _selected_field(base_schema: pa.Schema, column: str, masks: Mapping[str, MaskRule]) -> pa.Field:
    """Returns the visible field for a requested top-level or nested column path."""
    field = _field_for_path(base_schema, column)
    masked = _masked_field(field, column, masks)
    return pa.field(column, masked.type, nullable=masked.nullable, metadata=masked.metadata)


def _projected_nested_field(
    field: pa.Field,
    path: str,
    projection: ProjectionTree,
    masks: Mapping[str, MaskRule],
) -> pa.Field:
    """Returns a pruned nested field for requested dotted column paths."""
    if pa.types.is_struct(field.type):
        child_fields: list[pa.Field] = []
        for child_name, child_projection in projection.items():
            child = field.type.field(child_name)
            child_path = f"{path}.{child.name}"
            if child_projection:
                child_fields.append(
                    _projected_nested_field(child, child_path, child_projection, masks)
                )
            else:
                child_fields.append(_masked_field(child, child_path, masks))
        return pa.field(
            field.name,
            pa.struct(child_fields),
            nullable=field.nullable,
            metadata=field.metadata,
        )

    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        value_field = field.type.value_field
        projected_value_field = _projected_nested_field(value_field, path, projection, masks)
        list_type = (
            pa.list_(projected_value_field)
            if pa.types.is_list(field.type)
            else pa.large_list(projected_value_field)
        )
        return pa.field(
            field.name,
            list_type,
            nullable=field.nullable,
            metadata=field.metadata,
        )

    return _masked_field(field, path, masks)


def _field_for_path(schema: pa.Schema, path: str) -> pa.Field:
    """Resolves a top-level or nested field path from the Arrow schema."""
    parts = path.split(".")
    field = schema.field(parts[0])
    for part in parts[1:]:
        field_type = field.type
        if pa.types.is_list(field_type) or pa.types.is_large_list(field_type):
            field = field_type.value_field
            field_type = field.type
        field = field_type.field(part)
    return field


def _masked_field(field: pa.Field, path: str, masks: Mapping[str, MaskRule]) -> pa.Field:
    """Adjusts the visible field type when masking changes the value representation."""
    mask = masks.get(path)
    if mask is not None:
        return _masked_leaf_field(field, mask)

    if not pa.types.is_struct(field.type):
        if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
            value_field = field.type.value_field
            nested_value_field = _masked_field(value_field, path, masks)
            if nested_value_field.equals(value_field):
                return field
            list_type = (
                pa.list_(nested_value_field)
                if pa.types.is_list(field.type)
                else pa.large_list(nested_value_field)
            )
            return pa.field(
                field.name,
                list_type,
                nullable=field.nullable,
                metadata=field.metadata,
            )
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
    if mask_type in {"hash", "redact", "email", "keep_last"}:
        return pa.field(field.name, pa.string(), nullable=True)
    if mask_type == "default":
        if mask.value is None:
            raise ValueError("default mask requires a value")
        return pa.field(field.name, _default_mask_type(mask.value), nullable=field.nullable)
    return field


def _has_mask_for_path(path: str, masks: Mapping[str, MaskRule]) -> bool:
    return path in masks or _has_descendant_mask(path, masks)


def _has_descendant_mask(path: str, masks: Mapping[str, MaskRule]) -> bool:
    return any(_is_descendant_path(mask_path, path) for mask_path in masks)


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


def _default_mask_type(value: object) -> pa.DataType:
    """Matches the Arrow type DuckDB emits for the configured default literal."""
    return _duckdb_literal_arrow_type(_sql_literal(value))


@lru_cache(maxsize=128)
def _duckdb_literal_arrow_type(literal: str) -> pa.DataType:
    con = _connect()
    try:
        return con.sql(f"SELECT {literal} AS value").arrow().read_all().schema.field("value").type
    finally:
        con.close()
