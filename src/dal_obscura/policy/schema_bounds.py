"""Shared bounds for materializing and traversing Arrow schemas."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa

from dal_obscura.policy.schema_index import field_children

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
    _unique_names(schema)
    stack = [(field, 1) for field in reversed(schema)]
    while stack:
        field, depth = stack.pop()
        yield field, depth
        children = [child for child, _segment in field_children(field)]
        if pa.types.is_struct(field.type):
            _unique_names(children)
        stack.extend((child, depth + 1) for child in reversed(children))


def _unique_names(fields) -> None:
    names: set[str] = set()
    for field in fields:
        if field.name in names:
            raise ValueError(f"Duplicate Arrow field name: {field.name}")
        names.add(field.name)
