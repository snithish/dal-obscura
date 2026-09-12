"""Shared bounds for materializing and traversing Arrow schemas."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa

MAX_SCHEMA_NODES = 10_000
MAX_SCHEMA_DEPTH = 64
MAX_SCHEMA_ENCODING_BYTES = 2 * 1024 * 1024


def validate_arrow_schema_bounds(
    schema: pa.Schema,
    *,
    max_nodes: int = MAX_SCHEMA_NODES,
    max_depth: int = MAX_SCHEMA_DEPTH,
    max_encoding_bytes: int = MAX_SCHEMA_ENCODING_BYTES,
) -> None:
    """Reject schemas that exceed traversal or serialized-size budgets."""

    if not isinstance(schema, pa.Schema):
        raise TypeError("schema must be an Arrow schema")
    if max_nodes <= 0 or max_depth < 0 or max_encoding_bytes <= 0:
        raise ValueError("schema limits must be positive")
    for nodes, (_field, depth) in enumerate(_walk_fields(schema), 1):
        if nodes > max_nodes:
            raise ValueError(f"Arrow schema exceeds the {max_nodes} field-node limit")
        if depth > max_depth:
            raise ValueError(f"Arrow schema exceeds the {max_depth} nesting-depth limit")
    if schema.serialize().size > max_encoding_bytes:
        raise ValueError(f"Schema encoding exceeds the {max_encoding_bytes}-byte limit")


def _walk_fields(schema: pa.Schema) -> Iterator[tuple[pa.Field, int]]:
    stack = [(field, 1) for field in reversed(schema)]
    while stack:
        field, depth = stack.pop()
        yield field, depth
        children = list(_children(field))
        stack.extend((child, depth + 1) for child in reversed(children))


def _children(field: pa.Field) -> Iterator[pa.Field]:
    if pa.types.is_struct(field.type):
        yield from field.type
    elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        yield field.type.value_field
    elif pa.types.is_map(field.type):
        yield field.type.key_field
        yield field.type.item_field
    elif pa.types.is_fixed_size_list(field.type):
        yield field.type.value_field
