"""Canonical field identities for stable and schema-scoped schemas."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Iterable, Iterator
from typing import cast

import pyarrow as pa

from dal_obscura_plugin_api.schema.bounds import (
    MAX_SCHEMA_ENCODING_BYTES,
    validate_arrow_schema_bounds,
)
from dal_obscura_plugin_api.schema.index import walk_schema_fields

SYNTHETIC_ID_PREFIX = "synthetic:"
MAX_PROVIDER_FIELD_ID_LENGTH = 128
_FIELD_ID_KEYS = (b"PARQUET:field_id", b"iceberg.field.id")


def schema_scope_digest(schema: pa.Schema) -> str:
    """Digest the semantic shape used to scope synthetic field identities."""

    return schema_identity_digest(schema, ensure_ascii=True)


def schema_identity_digest(schema: pa.Schema, *, ensure_ascii: bool) -> str:
    """Hash canonical v2 JSON incrementally within the shared schema budget."""
    validate_arrow_schema_bounds(schema)
    digest = hashlib.sha256()
    remaining = MAX_SCHEMA_ENCODING_BYTES
    for chunk in _identity_chunks(schema, ensure_ascii=ensure_ascii):
        encoded = chunk.encode("utf-8")
        remaining -= len(encoded)
        if remaining < 0:
            raise ValueError(f"Schema encoding exceeds the {MAX_SCHEMA_ENCODING_BYTES}-byte limit")
        digest.update(encoded)
    return digest.hexdigest()


def schema_field_id(
    field: pa.Field,
    path: Iterable[str],
    *,
    scope_digest: str | None = None,
) -> tuple[str, bool]:
    """Return ``(identity, stable)`` for one Arrow field.

    Provider IDs are stable identities. Fields without one receive a digest
    scoped to the complete schema shape; changing any schema semantic changes
    that scope and therefore requires explicit reapproval.
    """

    raw_id = _provider_field_id(field)
    if raw_id is not None:
        return canonical_provider_field_id(raw_id), True
    scope = scope_digest or ""
    path_value = tuple(path)
    path_digest = hashlib.sha256(
        json.dumps(path_value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:24]
    return f"{SYNTHETIC_ID_PREFIX}{scope[:32]}:{path_digest}", False


def canonical_provider_field_id(value: str) -> str:
    """Normalize a bounded provider identity into the shared vocabulary."""

    if not isinstance(value, str) or not value:
        raise ValueError("Provider field ID must be non-empty text")
    return value if ":" in value else f"iceberg:{value}"


def schema_has_stable_ids(schema: pa.Schema) -> bool:
    """Whether every field and collection child exposes a provider ID."""

    return all(_provider_field_id(field) is not None for field, _path in walk_schema_fields(schema))


def _provider_field_id(field: pa.Field) -> str | None:
    metadata = field.metadata or {}
    for key in _FIELD_ID_KEYS:
        raw_id = metadata.get(key)
        if raw_id is not None:
            try:
                value = raw_id.decode("utf-8").strip()
            except UnicodeDecodeError:
                continue
            if (
                value
                and len(value) <= MAX_PROVIDER_FIELD_ID_LENGTH
                and not any(ord(char) < 0x20 or ord(char) == 0x7F for char in value)
                and not value.startswith(SYNTHETIC_ID_PREFIX)
            ):
                return canonical_provider_field_id(value)
    return None


def _shape_field(field: pa.Field) -> dict[str, object]:
    return {
        "name": field.name,
        "nullable": field.nullable,
        "metadata": _metadata(field.metadata),
        "type": field.type,
    }


def _shape_type(data_type: pa.DataType) -> dict[str, object]:
    value: dict[str, object] = {"id": str(data_type)}
    if pa.types.is_struct(data_type):
        value["children"] = list(data_type)
    elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        value["value_field"] = data_type.value_field
    elif pa.types.is_map(data_type):
        value["key_field"] = data_type.key_field
        value["item_field"] = data_type.item_field
    elif pa.types.is_fixed_size_list(data_type):
        value["list_size"] = data_type.list_size
        value["value_field"] = data_type.value_field
    return value


def _metadata(metadata: dict[bytes, bytes] | None) -> list[list[str]]:
    if not metadata:
        return []
    return sorted(
        [
            [
                base64.b64encode(key).decode("ascii"),
                base64.b64encode(value).decode("ascii"),
            ]
            for key, value in metadata.items()
        ]
    )


def _identity_chunks(schema: pa.Schema, *, ensure_ascii: bool) -> Iterator[str]:
    """Iterative canonical traversal; escape long strings in bounded pieces."""
    root = {
        "encoding": 2,
        "fields": list(schema),
        "metadata": _metadata(schema.metadata),
    }
    encoder = json.JSONEncoder(ensure_ascii=ensure_ascii, sort_keys=True, separators=(",", ":"))
    stack: list[tuple[bool, object]] = [(False, root)]
    while stack:
        literal, value = stack.pop()
        if literal:
            yield cast(str, value)
            continue
        if (
            isinstance(value, pa.Field)
            and (encoded := _small_field_json(value, encoder)) is not None
        ):
            yield encoded
            continue
        value = _shallow_shape(value)
        if isinstance(value, dict):
            yield "{"
            stack.append((True, "}"))
            items = sorted(value.items())
            for index in range(len(items) - 1, -1, -1):
                key, child = items[index]
                stack.extend([(False, child), (True, ":"), (False, key)])
                if index:
                    stack.append((True, ","))
        elif isinstance(value, list):
            yield "["
            stack.append((True, "]"))
            for index in range(len(value) - 1, -1, -1):
                stack.append((False, value[index]))
                if index:
                    stack.append((True, ","))
        elif isinstance(value, str):
            yield from _string_chunks(value, encoder)
        else:
            yield encoder.encode(value)


def _string_chunks(value: str, encoder: json.JSONEncoder) -> Iterator[str]:
    yield '"'
    for offset in range(0, len(value), 4096):
        yield encoder.encode(value[offset : offset + 4096])[1:-1]
    yield '"'


def _shallow_shape(value: object) -> object:
    if isinstance(value, pa.Field):
        return _shape_field(value)
    if isinstance(value, pa.DataType):
        return _shape_type(value)
    return value


def _small_field_json(field: pa.Field, encoder: json.JSONEncoder) -> str | None:
    """Batch small scalar records to avoid hashing each JSON punctuation mark."""
    if pa.types.is_nested(field.type) or field.metadata or len(field.name) > 1024:
        return None
    type_shape = _shape_type(field.type)
    if len(cast(str, type_shape["id"])) > 1024:
        return None
    shape = _shape_field(field)
    shape["type"] = type_shape
    return encoder.encode(shape)


def numeric_field_id(field: pa.Field, path: Iterable[str], scope_digest: str) -> int:
    """Bounded numeric identity for the authoring path wire contract."""
    for key in _FIELD_ID_KEYS:
        raw = (field.metadata or {}).get(key)
        if raw is not None:
            try:
                value = int(raw.decode("utf-8"))
            except TypeError, ValueError:
                break
            if not 0 <= value <= 2**31 - 1:
                raise ValueError("Provider field ID must be a nonnegative 32-bit integer")
            return value
    digest = hashlib.sha256("\x1f".join((scope_digest, *path)).encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big", signed=False) & 0x7FFFFFFF
