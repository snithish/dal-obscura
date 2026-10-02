"""One typed traversal and immutable lookup for admitted Arrow schemas."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

import pyarrow as pa

from dal_obscura.policy.paths import (
    FieldPath,
    FieldPathSegment,
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    MapValueSegment,
    parse_field_path,
)


def field_children(field: pa.Field) -> Iterator[tuple[pa.Field, FieldPathSegment]]:
    """Container edges carry explicit kinds, never inferred field-name tokens."""
    data_type = field.type
    if pa.types.is_struct(data_type):
        for child in data_type:
            yield child, FieldSegment(child.name)
    elif (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_fixed_size_list(data_type)
    ):
        yield data_type.value_field, ListElementSegment()
    elif pa.types.is_map(data_type):
        yield data_type.key_field, MapKeySegment()
        yield data_type.item_field, MapValueSegment()


def walk_schema_fields(schema: pa.Schema) -> Iterator[tuple[pa.Field, FieldPath]]:
    stack = [(field, FieldPath((FieldSegment(field.name),))) for field in reversed(schema)]
    while stack:
        field, path = stack.pop()
        yield field, path
        stack.extend(
            (child, FieldPath((*path.segments, segment)))
            for child, segment in reversed(list(field_children(field)))
        )


@dataclass(frozen=True, init=False)
class SchemaIndex:
    """Expand projections once per request without repeatedly walking containers."""

    roots: tuple[FieldPath, ...]
    fields: Mapping[FieldPath, pa.Field]
    leaves: Mapping[FieldPath, tuple[str, ...]]

    def __init__(self, schema: pa.Schema) -> None:
        fields = {path: field for field, path in walk_schema_fields(schema)}
        leaves: dict[FieldPath, tuple[str, ...]] = {}
        for path, field in reversed(fields.items()):
            children = tuple(field_children(field))
            leaves[path] = (
                tuple(
                    leaf
                    for _child, segment in children
                    for leaf in leaves[FieldPath((*path.segments, segment))]
                )
                if children
                else (path.to_human(),)
            )
        object.__setattr__(self, "roots", tuple(path for path in fields if len(path.segments) == 1))
        object.__setattr__(self, "fields", MappingProxyType(fields))
        object.__setattr__(self, "leaves", MappingProxyType(leaves))

    def expand(self, columns: Sequence[str]) -> list[str]:
        paths = self.roots if tuple(columns) == ("*",) else tuple(map(parse_field_path, columns))
        missing = [path.to_human() for path in paths if path not in self.fields]
        if missing:
            raise ValueError(f"Unknown columns requested: {', '.join(missing)}")
        expanded = dict.fromkeys(leaf for path in paths for leaf in self.leaves[path])
        for column in tuple(expanded):
            path = parse_field_path(column)
            for index, segment in enumerate(path.segments):
                if isinstance(segment, MapValueSegment):
                    key = FieldPath((*path.segments[:index], MapKeySegment()))
                    expanded[key.to_human()] = None
        return list(expanded)
