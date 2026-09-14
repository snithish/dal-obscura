from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from functools import lru_cache
from itertools import chain
from threading import BoundedSemaphore

import duckdb
import pyarrow as pa

from dal_obscura.common.access_control.filters import RowFilter, row_filter_to_sql
from dal_obscura.common.access_control.models import MaskRule
from dal_obscura.common.query_planning.field_paths import (
    FieldPath,
    FieldPathSegment,
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    MapValueSegment,
    parse_field_path,
)
from dal_obscura.data_plane.application.ports.masking import MaskedSelection

_DUCKDB_ARROW_OUTPUT_BATCH_SIZE = 8_192
_DUCKDB_TRANSFORM_CONFIG: dict[str, str | bool | int | float | list[str]] = {
    "enable_external_access": "false",
    "autoload_known_extensions": "false",
    "autoinstall_known_extensions": "false",
    "threads": 1,
}
_DEFAULT_DUCKDB_MEMORY_LIMIT = "512MB"
_DEFAULT_MAX_ACTIVE_STREAMS = 16
_DEFAULT_MAX_INPUT_BATCH_BYTES = 64 * 1024 * 1024
_DEFAULT_MAX_OUTPUT_BATCH_BYTES = 64 * 1024 * 1024


class StreamAdmissionError(RuntimeError):
    """Raised when the configured governed-stream capacity is exhausted."""


class InputBatchLimitError(ValueError):
    """Raised when an Arrow input batch exceeds the configured byte budget."""


class OutputBatchLimitError(ValueError):
    """Raised when an Arrow output batch exceeds the configured byte budget."""


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
            elif column in masks:
                # A mask on a compound parent governs every descendant.  Returning
                # a pruned child expression here would otherwise bypass it.
                selected_fields.append(_masked_field(base_schema.field(column), column, masks))
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

        return pa.schema(_duckdb_output_field(field) for field in selected_fields)


class DuckDBRowTransformAdapter:
    """Applies row filters and masks to streamed Arrow batches via DuckDB SQL."""

    def __init__(
        self,
        masking: DefaultMaskingAdapter,
        *,
        max_active_streams: int = _DEFAULT_MAX_ACTIVE_STREAMS,
        duckdb_memory_limit: str = _DEFAULT_DUCKDB_MEMORY_LIMIT,
        max_input_batch_bytes: int = _DEFAULT_MAX_INPUT_BATCH_BYTES,
        max_output_batch_bytes: int = _DEFAULT_MAX_OUTPUT_BATCH_BYTES,
    ) -> None:
        if max_active_streams < 1:
            raise ValueError("max_active_streams must be positive")
        if not isinstance(duckdb_memory_limit, str) or not duckdb_memory_limit.strip():
            raise ValueError("duckdb_memory_limit must be non-empty text")
        if max_input_batch_bytes < 1:
            raise ValueError("max_input_batch_bytes must be positive")
        if max_output_batch_bytes < 1:
            raise ValueError("max_output_batch_bytes must be positive")
        self._masking = masking
        self._stream_slots = BoundedSemaphore(max_active_streams)
        self._duckdb_config = {
            **_DUCKDB_TRANSFORM_CONFIG,
            "memory_limit": duckdb_memory_limit,
        }
        self._max_input_batch_bytes = max_input_batch_bytes
        self._max_output_batch_bytes = max_output_batch_bytes

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
        _require_batch_size(first_batch, self._max_input_batch_bytes)
        query = _build_query(first_batch.schema, columns, row_filter, masks, self._masking)

        reader = pa.RecordBatchReader.from_batches(
            first_batch.schema,
            _bounded_batches(
                chain((first_batch,), batch_iter),
                max_input_batch_bytes=self._max_input_batch_bytes,
            ),
        )
        return _stream_query_results(
            reader,
            query,
            self._stream_slots,
            self._duckdb_config,
            max_output_batch_bytes=self._max_output_batch_bytes,
        )


def _stream_query_results(
    reader: pa.RecordBatchReader,
    query: str,
    stream_slots: BoundedSemaphore,
    duckdb_config: Mapping[str, str | bool | int | float | list[str]],
    *,
    max_output_batch_bytes: int,
) -> Iterator[pa.RecordBatch]:
    """Executes the generated SQL over the incoming Arrow reader."""
    # DuckDB 1.5.0 removes the Python-side per-batch loop here, but the input side
    # does not appear observably lazy enough to assert callback-order streaming.
    if not stream_slots.acquire(blocking=False):
        raise StreamAdmissionError("Governed stream capacity is exhausted")
    con: duckdb.DuckDBPyConnection | None = None
    try:
        con = _connect(duckdb_config)
        result_reader = (
            con.from_arrow(reader)
            .query("input", query)
            .to_arrow_reader(batch_size=_DUCKDB_ARROW_OUTPUT_BATCH_SIZE)
        )
        yield from _bounded_output_batches(
            result_reader,
            max_output_batch_bytes=max_output_batch_bytes,
        )
    finally:
        try:
            if con is not None:
                con.close()
        finally:
            stream_slots.release()


def _connect(
    config: Mapping[str, str | bool | int | float | list[str]] | None = None,
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config=dict(config or _DUCKDB_TRANSFORM_CONFIG))
    con.execute("SET enable_progress_bar = false")
    return con


def _bounded_batches(
    batches: Iterable[pa.RecordBatch], *, max_input_batch_bytes: int
) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        _require_batch_size(batch, max_input_batch_bytes)
        yield batch


def _require_batch_size(batch: pa.RecordBatch, maximum: int) -> None:
    if batch.nbytes > maximum:
        raise InputBatchLimitError(
            f"Arrow input batch is {batch.nbytes} bytes; limit is {maximum} bytes"
        )


def _bounded_output_batches(
    batches: Iterable[pa.RecordBatch], *, max_output_batch_bytes: int
) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        if batch.nbytes > max_output_batch_bytes:
            raise OutputBatchLimitError(
                "Arrow output batch is "
                f"{batch.nbytes} bytes; limit is {max_output_batch_bytes} bytes"
            )
        yield batch


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


def _path_segments(path: str) -> tuple[FieldPathSegment, ...]:
    """Parses canonical paths while retaining legacy special-character field names."""
    try:
        return parse_field_path(path).segments
    except ValueError:
        if "." not in path:
            return (FieldSegment(path),)
        raise


def _path_field_names(path: str) -> tuple[str, ...]:
    """Returns stable tokens for field and collection segments."""
    tokens: list[str] = []
    for segment in _path_segments(path):
        if isinstance(segment, FieldSegment):
            tokens.append(segment.name)
        elif isinstance(segment, ListElementSegment):
            tokens.append("$element")
        elif isinstance(segment, MapKeySegment):
            tokens.append("$key")
        else:
            tokens.append("$value")
    return tuple(tokens)


def _top_level_path(source: str, field_name: str) -> str:
    """Keeps legacy unquoted special-character top-level names compatible."""
    try:
        parse_field_path(source)
    except ValueError:
        return source
    return _field_path((field_name,))


def _field_path(parts: Iterable[str]) -> str:
    return FieldPath(tuple(FieldSegment(part) for part in parts)).to_human()


def _append_field_path(path: str, field_name: str) -> str:
    return FieldPath((*_path_segments(path), FieldSegment(field_name))).to_human()


def _append_collection_path(path: str, segment: FieldPathSegment) -> str:
    return FieldPath((*_path_segments(path), segment)).to_human()


def _column_reference(path: str) -> str:
    return ".".join(_quote_identifier(part) for part in _path_field_names(path))


def _output_field_name(path: str) -> str:
    return _path_field_names(path)[0]


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
            field = base_schema.field(_path_field_names(column)[0])
            direct_mask = masks.get(column)
            if direct_mask is not None:
                expr = _mask_expression(_quote_identifier(column), direct_mask)
            else:
                expr = _nested_projection_expression(
                    _quote_identifier(column),
                    column,
                    field.type,
                    nested_projection,
                    masks,
                )
            select_list.append(f"{expr} AS {_quote_identifier(_output_field_name(column))}")
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
            select_list.append(f"{expr} AS {_quote_identifier(_output_field_name(column))}")
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
            select_list.append(f"{expr} AS {_quote_identifier(_output_field_name(column))}")
        else:
            select_list.append(
                f"{_column_reference(column)} AS {_quote_identifier(_output_field_name(column))}"
            )

    return MaskedSelection(select_list=select_list, masked_columns=masked_columns)


ProjectionTree = dict[str, "ProjectionTree"]


def _build_projection(columns: Iterable[str]) -> list[tuple[str, ProjectionTree | None]]:
    """Groups canonical field paths into top-level Arrow fields."""
    projection: list[tuple[str, ProjectionTree | None]] = []
    by_top_level: dict[str, ProjectionTree | None] = {}

    for column in columns:
        top_level, *nested = _path_field_names(column)
        top_level_path = _top_level_path(column, top_level)
        if top_level_path not in by_top_level:
            tree: ProjectionTree | None = {} if nested else None
            by_top_level[top_level_path] = tree
            projection.append((top_level_path, tree))

        tree = by_top_level[top_level_path]
        if tree is None:
            continue
        if not nested:
            by_top_level[top_level_path] = None
            for index, (name, _existing) in enumerate(projection):
                if name == top_level_path:
                    projection[index] = (top_level_path, None)
                    break
            continue
        _insert_projection_path(tree, nested)

    return projection


def _insert_projection_path(tree: ProjectionTree, parts: list[str]) -> None:
    current = tree
    for part in parts:
        current = current.setdefault(part, {})


def _projection_contains(top_level: str, tree: ProjectionTree, path: str) -> bool:
    parts = _path_field_names(path)
    if not parts or _field_path((parts[0],)) != top_level:
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
            child_path = _append_field_path(path, child.name)
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

    if pa.types.is_map(data_type):
        if "$key" not in projection or "$value" not in projection:
            raise ValueError("Map projection requires explicit key and value paths")
        value_path = _append_collection_path(path, MapValueSegment())
        value_expr = _nested_projection_leaf_or_struct(
            f"{item_var}_entry.value",
            value_path,
            data_type.item_field.type,
            projection["$value"],
            masks,
            item_var=f"{item_var}_entry",
        )
        entry_var = f"{item_var}_entry"
        return (
            "map_from_entries(list_transform(map_entries("
            f"{expr}), {entry_var} -> struct_pack(key := {entry_var}.key, value := {value_expr})))"
        )

    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        child_var = f"{item_var}_{len(_path_field_names(path))}"
        value_field = data_type.value_field
        canonical_projection = projection.get("$element")
        value_projection = projection if canonical_projection is None else canonical_projection
        value_path = (
            path
            if canonical_projection is None
            else _append_collection_path(path, ListElementSegment())
        )
        transformed = _nested_projection_leaf_or_struct(
            child_var,
            value_path,
            value_field.type,
            value_projection,
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
        return f"cast_to_type(NULL, {expr})"
    if mask_type == "redact":
        if not isinstance(mask.value, str):
            raise ValueError("redact mask requires a string value")
        return f"CASE WHEN {expr} IS NULL THEN NULL ELSE {_sql_literal(mask.value)} END"
    if mask_type == "hash":
        return f"sha256(CAST({expr} AS VARCHAR))"
    if mask_type == "email":
        text_column = f"CAST({expr} AS VARCHAR)"
        return (
            "CASE "
            f"WHEN regexp_full_match({text_column}, '^[^@]+@[^@]+$') "
            f"THEN regexp_replace({text_column}, '(^.)[^@]*(@[^@]+)$', '\\1***\\2') "
            "ELSE NULL END"
        )
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
            return f"cast_to_type(NULL, {expr})"
        return _sql_literal(mask.value)
    raise ValueError(f"Unsupported mask type: {mask.type}")


def _is_descendant_path(path: str, parent: str) -> bool:
    """Returns whether `path` is nested underneath `parent`."""
    path_parts = _path_field_names(path)
    parent_parts = _path_field_names(parent)
    return len(path_parts) > len(parent_parts) and path_parts[: len(parent_parts)] == parent_parts


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
            child_path = _append_field_path(path, child.name)
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
        child_var = f"{item_var}_{len(_path_field_names(path))}"
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
    return pa.field(field.name, masked.type, nullable=masked.nullable, metadata=masked.metadata)


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
            child_path = _append_field_path(path, child.name)
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

    if pa.types.is_map(field.type):
        if "$key" not in projection or "$value" not in projection:
            raise ValueError("Map projection requires explicit key and value paths")
        value_field = field.type.item_field
        projected_value_field = _projected_nested_field(
            value_field,
            _append_collection_path(path, MapValueSegment()),
            projection["$value"],
            masks,
        )
        return pa.field(
            field.name,
            pa.map_(field.type.key_field.type, projected_value_field),
            nullable=field.nullable,
            metadata=field.metadata,
        )

    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        value_field = field.type.value_field
        canonical_projection = projection.get("$element")
        value_projection = projection if canonical_projection is None else canonical_projection
        value_path = (
            path
            if canonical_projection is None
            else _append_collection_path(path, ListElementSegment())
        )
        projected_value_field = _projected_nested_field(
            value_field, value_path, value_projection, masks
        )
        return pa.field(
            field.name,
            pa.list_(projected_value_field),
            nullable=field.nullable,
            metadata=field.metadata,
        )

    return _masked_field(field, path, masks)


def _field_for_path(schema: pa.Schema, path: str) -> pa.Field:
    """Resolves a canonical field path from the Arrow schema."""
    parts = _path_field_names(path)
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
        if pa.types.is_map(field.type):
            value_field = field.type.item_field
            nested_value_field = _masked_field(
                value_field,
                _append_collection_path(path, MapValueSegment()),
                masks,
            )
            if nested_value_field.equals(value_field):
                return field
            return pa.field(
                field.name,
                pa.map_(field.type.key_field.type, nested_value_field),
                nullable=field.nullable,
                metadata=field.metadata,
            )
        if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
            value_field = field.type.value_field
            canonical_value_path = _append_collection_path(path, ListElementSegment())
            value_path = (
                canonical_value_path if _has_mask_for_path(canonical_value_path, masks) else path
            )
            nested_value_field = _masked_field(value_field, value_path, masks)
            if nested_value_field.equals(value_field) and pa.types.is_list(field.type):
                return field
            return pa.field(
                field.name,
                pa.list_(nested_value_field),
                nullable=field.nullable,
                metadata=field.metadata,
            )
        return field

    nested_fields = [
        _masked_field(child, _append_field_path(path, child.name), masks) for child in field.type
    ]
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
    if mask_type == "null":
        return pa.field(field.name, field.type, nullable=True, metadata=field.metadata)
    if mask_type in {"hash", "redact", "email", "keep_last"}:
        return pa.field(field.name, pa.string(), nullable=True)
    if mask_type == "default":
        if mask.value is None:
            return pa.field(field.name, field.type, nullable=True, metadata=field.metadata)
        return pa.field(field.name, _default_mask_type(mask.value), nullable=field.nullable)
    return field


def _duckdb_output_field(field: pa.Field) -> pa.Field:
    """Models the Arrow field shape emitted by DuckDB projection queries."""
    return pa.field(field.name, _duckdb_output_type(field.type), nullable=True)


def _duckdb_output_type(data_type: pa.DataType) -> pa.DataType:
    # Flight streams must declare the exact schema DuckDB emits. DuckDB strips
    # field metadata, marks projected fields nullable, and narrows large string
    # and large list Arrow types during projection.
    if pa.types.is_large_string(data_type):
        return pa.string()
    if pa.types.is_large_binary(data_type):
        return pa.binary()
    if pa.types.is_struct(data_type):
        return pa.struct(_duckdb_output_field(child) for child in data_type)
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return pa.list_(_duckdb_output_field(data_type.value_field))
    return data_type


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
