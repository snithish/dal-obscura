"""Canonical, versioned field paths for governed nested Arrow schemas.

The path wire form is a JSON-compatible sequence of typed segments.  The
human form keeps simple field paths readable (``profile.name``), while field
names that need quoting use JSON brackets (``[\"profile.name\"]``).  Collection
segments are explicit: ``$element``, ``$key``, and ``$value``.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Final, TypeAlias, cast

import pyarrow as pa

_SIMPLE_FIELD_NAME: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]*$")
_COLLECTION_SEGMENTS: Final = {"$element", "$key", "$value"}


@dataclass(frozen=True)
class FieldSegment:
    """A named struct or top-level field, optionally bound to a field ID."""

    name: str
    field_id: int | None = None


@dataclass(frozen=True)
class ListElementSegment:
    """The element node of an Arrow list."""


@dataclass(frozen=True)
class MapKeySegment:
    """The key node of an Arrow map."""


@dataclass(frozen=True)
class MapValueSegment:
    """The value node of an Arrow map."""


FieldPathSegment: TypeAlias = FieldSegment | ListElementSegment | MapKeySegment | MapValueSegment


@dataclass(frozen=True)
class FieldPath:
    """An immutable field path that cannot confuse names and nesting."""

    segments: tuple[FieldPathSegment, ...]
    version: int = 1

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError("Unsupported field path version")
        if not self.segments:
            raise ValueError("Field path must contain at least one segment")
        if not isinstance(self.segments[0], FieldSegment):
            raise ValueError("Field paths must begin with a field segment")
        for segment in self.segments:
            if isinstance(segment, FieldSegment) and not segment.name:
                raise ValueError("Field segment names must be non-empty")

    def to_wire(self) -> dict[str, object]:
        """Returns the stable typed representation used by protocol payloads."""
        encoded: list[dict[str, object]] = []
        for segment in self.segments:
            if isinstance(segment, FieldSegment):
                value: dict[str, object] = {"kind": "field", "name": segment.name}
                if segment.field_id is not None:
                    value["field_id"] = segment.field_id
                encoded.append(value)
            elif isinstance(segment, ListElementSegment):
                encoded.append({"kind": "list_element"})
            elif isinstance(segment, MapKeySegment):
                encoded.append({"kind": "map_key"})
            else:
                encoded.append({"kind": "map_value"})
        return {"version": self.version, "segments": encoded}

    def to_human(self) -> str:
        """Renders an unambiguous human input form."""
        parts: list[str] = []
        for segment in self.segments:
            if isinstance(segment, FieldSegment):
                rendered = (
                    segment.name
                    if _SIMPLE_FIELD_NAME.fullmatch(segment.name)
                    else f"[{json.dumps(segment.name, ensure_ascii=False)}]"
                )
            elif isinstance(segment, ListElementSegment):
                rendered = "$element"
            elif isinstance(segment, MapKeySegment):
                rendered = "$key"
            else:
                rendered = "$value"
            parts.append(rendered)
        return ".".join(parts)

    @classmethod
    def from_wire(cls, value: object) -> FieldPath:
        """Decodes only the supported versioned typed wire representation."""
        if not isinstance(value, dict) or set(value) != {"version", "segments"}:
            raise ValueError("Invalid field path wire representation")
        wire = cast(dict[str, object], value)
        version = wire["version"]
        raw_segments = wire["segments"]
        if not isinstance(version, int) or isinstance(version, bool):
            raise ValueError("Field path version must be an integer")
        if not isinstance(raw_segments, list):
            raise ValueError("Field path segments must be a list")
        segments = tuple(_segment_from_wire(segment) for segment in raw_segments)
        return cls(segments=segments, version=version)


def parse_field_path(value: str) -> FieldPath:
    """Parses the canonical human form without guessing dotted field names."""
    if not isinstance(value, str) or not value:
        raise ValueError("Field path must be non-empty text")

    tokens = _split_path(value)
    segments: list[FieldPathSegment] = []
    for token, quoted in tokens:
        if not quoted and token == "$element":
            segments.append(ListElementSegment())
        elif not quoted and token == "$key":
            segments.append(MapKeySegment())
        elif not quoted and token == "$value":
            segments.append(MapValueSegment())
        elif not quoted and token in _COLLECTION_SEGMENTS:
            raise ValueError(f"Invalid collection path segment: {token}")
        else:
            segments.append(FieldSegment(token))
    return FieldPath(tuple(segments))


def _segment_from_wire(value: object) -> FieldPathSegment:
    if not isinstance(value, dict):
        raise ValueError("Invalid field path segment")
    wire = cast(dict[str, object], value)
    kind = wire["kind"]
    if not isinstance(kind, str):
        raise ValueError("Invalid field path segment")
    if kind == "field":
        allowed = {"kind", "name", "field_id"}
        name = wire.get("name")
        if set(wire) - allowed or not isinstance(name, str) or not name:
            raise ValueError("Invalid field path field segment")
        field_id = wire.get("field_id")
        if field_id is not None and (not isinstance(field_id, int) or isinstance(field_id, bool)):
            raise ValueError("Field path field IDs must be integers")
        return FieldSegment(name=name, field_id=field_id)
    if set(wire) != {"kind"}:
        raise ValueError("Collection path segments cannot carry additional fields")
    if kind == "list_element":
        return ListElementSegment()
    if kind == "map_key":
        return MapKeySegment()
    if kind == "map_value":
        return MapValueSegment()
    raise ValueError(f"Unknown field path segment kind: {kind}")


def resolve_schema_path(schema: pa.Schema, path: FieldPath) -> pa.Field:
    """Resolves a typed path against an Arrow schema or raises a precise error."""
    current: pa.Field | None = None
    for index, segment in enumerate(path.segments):
        if isinstance(segment, FieldSegment):
            current = _resolve_field_segment(schema, current, segment, index, path)
        else:
            current = _resolve_collection_segment(current, segment, path)
    assert current is not None
    return current


def _resolve_field_segment(
    schema: pa.Schema,
    current: pa.Field | None,
    segment: FieldSegment,
    index: int,
    path: FieldPath,
) -> pa.Field:
    try:
        if index == 0:
            resolved = schema.field(segment.name)
        else:
            if current is None or not pa.types.is_struct(current.type):
                raise ValueError(f"Field path does not contain a struct at: {path.to_human()}")
            resolved = current.type.field(segment.name)
    except KeyError as exc:
        raise ValueError(f"Unknown field path: {path.to_human()}") from exc
    _require_field_id(resolved, segment, path)
    return resolved


def _require_field_id(field: pa.Field, segment: FieldSegment, path: FieldPath) -> None:
    if segment.field_id is None:
        return
    metadata = field.metadata or {}
    raw_id = metadata.get(b"PARQUET:field_id") or metadata.get(b"iceberg.field.id")
    if raw_id is None:
        raise ValueError(f"Field path has no bound field ID: {path.to_human()}")
    try:
        actual_id = int(raw_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Field path has invalid field ID metadata: {path.to_human()}") from exc
    if actual_id != segment.field_id:
        raise ValueError(f"Field ID does not match path: {path.to_human()}")


def _resolve_collection_segment(
    current: pa.Field | None,
    segment: ListElementSegment | MapKeySegment | MapValueSegment,
    path: FieldPath,
) -> pa.Field:
    if current is None:
        raise ValueError(f"Invalid field path: {path.to_human()}")
    if isinstance(segment, ListElementSegment):
        if not pa.types.is_list(current.type) and not pa.types.is_large_list(current.type):
            raise ValueError(f"Field path does not contain a list at: {path.to_human()}")
        return current.type.value_field
    if not pa.types.is_map(current.type):
        raise ValueError(f"Field path does not contain a map at: {path.to_human()}")
    return current.type.key_field if isinstance(segment, MapKeySegment) else current.type.item_field


def _split_path(value: str) -> list[tuple[str, bool]]:
    tokens: list[tuple[str, bool]] = []
    offset = 0
    while offset < len(value):
        if value[offset] == "[":
            try:
                token, consumed = json.JSONDecoder().raw_decode(value[offset + 1 :])
            except json.JSONDecodeError as exc:
                raise ValueError("Quoted field names must use JSON string syntax") from exc
            if not isinstance(token, str) or not token:
                raise ValueError("Quoted field names must be non-empty text")
            closing = offset + 1 + consumed
            if closing >= len(value) or value[closing] != "]":
                raise ValueError("Unterminated quoted field name")
            offset = closing + 1
            quoted = True
        else:
            next_dot = value.find(".", offset)
            end = len(value) if next_dot == -1 else next_dot
            token = value[offset:end]
            if not _SIMPLE_FIELD_NAME.fullmatch(token) and token not in _COLLECTION_SEGMENTS:
                raise ValueError(f"Invalid field path segment: {token}")
            offset = end
            quoted = False
        tokens.append((token, quoted))
        if offset == len(value):
            break
        if value[offset] != ".":
            raise ValueError("Expected '.' between field path segments")
        offset += 1
        if offset == len(value):
            raise ValueError("Field path cannot end with '.'")
    return tokens
