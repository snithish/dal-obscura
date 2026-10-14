"""Lossless human table identifiers."""

from typing import cast

from dal_obscura_plugin_api import TableIdentifier
from dal_obscura_plugin_api.paths import FieldPath, FieldSegment, parse_field_path


def table_identifier(target: str) -> TableIdentifier:
    if not isinstance(target, str) or not target.strip():
        raise ValueError("table target must be non-empty")
    segments = parse_field_path(target).segments
    if any(not isinstance(segment, FieldSegment) for segment in segments):
        raise ValueError("Table identifiers cannot contain collection segments")
    parts = tuple(cast(FieldSegment, segment).name for segment in segments)
    return TableIdentifier(namespace=parts[:-1], name=parts[-1])


def identifier_name(identifier: TableIdentifier) -> str:
    return FieldPath(
        tuple(FieldSegment(part) for part in (*identifier.namespace, identifier.name))
    ).to_human()
