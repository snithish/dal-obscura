"""Canonical field identities for stable and schema-scoped schemas."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable

import pyarrow as pa

from dal_obscura.policy.schema_index import walk_schema_fields

SYNTHETIC_ID_PREFIX = "synthetic:"
MAX_PROVIDER_FIELD_ID_LENGTH = 128
_FIELD_ID_KEYS = (b"PARQUET:field_id", b"iceberg.field.id")


def schema_scope_digest(schema: pa.Schema) -> str:
    """Digest the semantic shape used to scope synthetic field identities."""

    payload = schema_shape(schema)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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
        "type": _shape_type(field.type),
    }


def _shape_type(data_type: pa.DataType) -> object:
    value: dict[str, object] = {"id": str(data_type)}
    if pa.types.is_struct(data_type):
        value["children"] = [_shape_field(field) for field in data_type]
    elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        value["value_field"] = _shape_field(data_type.value_field)
    elif pa.types.is_map(data_type):
        value["key_field"] = _shape_field(data_type.key_field)
        value["item_field"] = _shape_field(data_type.item_field)
    elif pa.types.is_fixed_size_list(data_type):
        value["list_size"] = data_type.list_size
        value["value_field"] = _shape_field(data_type.value_field)
    return value


def _metadata(metadata: dict[bytes, bytes] | None) -> list[list[str]]:
    if not metadata:
        return []
    return sorted(
        [
            [
                key.decode("utf-8", "backslashreplace"),
                value.decode("utf-8", "backslashreplace"),
            ]
            for key, value in metadata.items()
        ]
    )


def schema_shape(schema: pa.Schema) -> dict[str, object]:
    """Canonical shape shared by admission fingerprints and synthetic IDs."""
    return {
        "fields": [_shape_field(field) for field in schema],
        "metadata": _metadata(schema.metadata),
    }


def numeric_field_id(field: pa.Field, path: Iterable[str], scope_digest: str) -> int:
    """Bounded numeric identity for the authoring path wire contract."""
    for key in _FIELD_ID_KEYS:
        raw = (field.metadata or {}).get(key)
        if raw is not None:
            try:
                value = int(raw.decode("utf-8"))
            except (TypeError, ValueError):
                break
            if not 0 <= value <= 2**31 - 1:
                raise ValueError("Provider field ID must be a nonnegative 32-bit integer")
            return value
    digest = hashlib.sha256("\x1f".join((scope_digest, *path)).encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big", signed=False) & 0x7FFFFFFF
