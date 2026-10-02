from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import lru_cache

import pyarrow as pa

from dal_obscura.policy.models import MaskRule
from dal_obscura.policy.paths import (
    FieldPath,
    FieldPathSegment,
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    MapValueSegment,
    parse_field_path,
    resolve_schema_path,
)
from dal_obscura.read.duckdb_connection import connect


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _path_segments(path: str) -> tuple[FieldPathSegment, ...]:
    """Parses the canonical field-path syntax."""
    return parse_field_path(path).segments


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


def _field_path(parts: Iterable[str]) -> str:
    return FieldPath(tuple(FieldSegment(part) for part in parts)).to_human()


def _append_field_path(path: str, field_name: str) -> str:
    return FieldPath((*_path_segments(path), FieldSegment(field_name))).to_human()


def _append_collection_path(path: str, segment: FieldPathSegment) -> str:
    return FieldPath((*_path_segments(path), segment)).to_human()


ProjectionTree = dict[str, "ProjectionTree"]


def _build_projection(columns: Iterable[str]) -> list[tuple[str, ProjectionTree | None]]:
    """Groups canonical field paths into top-level Arrow fields."""
    projection: list[tuple[str, ProjectionTree | None]] = []
    by_top_level: dict[str, ProjectionTree | None] = {}

    for column in columns:
        top_level, *nested = _path_field_names(column)
        top_level_path = _field_path((top_level,))
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


def _mask_expression(expr: str, mask: MaskRule) -> str:
    """Returns the DuckDB SQL fragment for a single mask rule."""
    mask_type = mask.type.lower()
    if mask_type in {"null", "hash", "email"} and mask.value is not None:
        raise ValueError(f"Mask {mask_type!r} does not accept a value")
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
        if isinstance(mask.value, bool) or not isinstance(mask.value, int) or mask.value < 0:
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
    if pa.types.is_map(data_type):
        return pa.map_(
            _duckdb_output_type(data_type.key_type),
            _duckdb_output_field(data_type.item_field),
        )
    return data_type


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
    con = connect()
    try:
        return con.sql(f"SELECT {literal} AS value").arrow().read_all().schema.field("value").type
    finally:
        con.close()


@dataclass(frozen=True)
class ProjectionPlan:
    """SQL and advertised Arrow shape compiled from the same field traversal."""

    select_list: tuple[str, ...]
    output_schema: pa.Schema
    masked_columns: tuple[str, ...]


@dataclass(frozen=True)
class _ProjectedField:
    expression: str
    field: pa.Field
    masked_paths: tuple[str, ...] = ()


def compile_projection(
    schema: pa.Schema, columns: Iterable[str], masks: Mapping[str, MaskRule]
) -> ProjectionPlan:
    for path, mask in masks.items():
        resolve_schema_path(schema, parse_field_path(path))
        _mask_expression("NULL", mask)
    requested = list(columns)
    if requested == ["*"]:
        requested = [_field_path((field.name,)) for field in schema]
    for path in requested:
        resolve_schema_path(schema, parse_field_path(path))
    projected = [
        _compile_field(
            _quote_identifier(_path_field_names(path)[0]),
            path,
            schema.field(_path_field_names(path)[0]),
            tree,
            masks,
        )
        for path, tree in _build_projection(requested)
    ]
    return ProjectionPlan(
        select_list=tuple(
            f"{item.expression} AS {_quote_identifier(item.field.name)}" for item in projected
        ),
        output_schema=pa.schema(_duckdb_output_field(item.field) for item in projected),
        masked_columns=tuple(path for item in projected for path in item.masked_paths),
    )


def _compile_field(
    expr: str,
    path: str,
    field: pa.Field,
    projection: ProjectionTree | None,
    masks: Mapping[str, MaskRule],
    *,
    item_var: str = "_item",
) -> _ProjectedField:
    mask = masks.get(path)
    if mask is not None:
        return _ProjectedField(
            _mask_expression(expr, mask), _masked_leaf_field(field, mask), (path,)
        )
    if not projection and not _has_descendant_mask(path, masks):
        return _ProjectedField(expr, field)
    data_type = field.type
    if pa.types.is_struct(data_type):
        children = [
            _compile_field(
                f"({expr}).{_quote_identifier(name)}",
                _append_field_path(path, name),
                data_type.field(name),
                tree,
                masks,
                item_var=item_var,
            )
            for name, tree in (
                projection.items() if projection else ((child.name, None) for child in data_type)
            )
        ]
        packed = (
            "struct_pack("
            + ", ".join(
                f"{_quote_identifier(child.field.name)} := {child.expression}" for child in children
            )
            + ")"
        )
        return _ProjectedField(
            f"CASE WHEN {expr} IS NULL THEN NULL ELSE {packed} END",
            field.with_type(pa.struct(child.field for child in children)),
            tuple(path for child in children for path in child.masked_paths),
        )
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        if projection and set(projection) != {"$element"}:
            raise ValueError("List projection requires an explicit $element path")
        child_var = f"{item_var}_{len(_path_field_names(path))}"
        child = _compile_field(
            child_var,
            _append_collection_path(path, ListElementSegment()),
            data_type.value_field,
            projection.get("$element") if projection else None,
            masks,
            item_var=child_var,
        )
        return _ProjectedField(
            f"list_transform({expr}, {child_var} -> {child.expression})",
            field.with_type(pa.list_(child.field)),
            child.masked_paths,
        )
    if pa.types.is_map(data_type):
        return _compile_map(expr, path, field, projection, masks, item_var=item_var)
    return _ProjectedField(expr, field)


def _compile_map(
    expr: str,
    path: str,
    field: pa.Field,
    projection: ProjectionTree | None,
    masks: Mapping[str, MaskRule],
    *,
    item_var: str,
) -> _ProjectedField:
    if projection and set(projection) != {"$key", "$value"}:
        raise ValueError("Map projection requires explicit key and value paths")
    key_path = _append_collection_path(path, MapKeySegment())
    key_mask = masks.get(key_path)
    if key_mask is not None and key_mask.type.lower() != "null":
        raise ValueError("Map keys support only the null mask; mask map values instead")
    entry_var = f"{item_var}_entry"
    value = _compile_field(
        f"{entry_var}.value",
        _append_collection_path(path, MapValueSegment()),
        field.type.item_field,
        projection.get("$value") if projection else None,
        masks,
        item_var=entry_var,
    )
    projected = (
        f"map_from_entries(list_transform(map_entries({expr}), {entry_var} -> "
        f"struct_pack(key := {entry_var}.key, value := {value.expression})))"
    )
    return _ProjectedField(
        f"cast_to_type(NULL, {projected})" if key_mask else projected,
        field.with_type(pa.map_(field.type.key_type, value.field)),
        ((key_path,) if key_mask else ()) + value.masked_paths,
    )
