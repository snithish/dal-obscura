from __future__ import annotations

import hashlib
import json

import pyarrow as pa
import pytest
from dal_obscura_plugin_api.schema.bounds import MAX_SCHEMA_DEPTH
from dal_obscura_plugin_api.schema.identity import numeric_field_id, schema_scope_digest
from pyiceberg.schema import Schema
from pyiceberg.types import ListType, NestedField, StringType, StructType

from dal_obscura.application.governance.assets.schema import (
    schema_fingerprint,
)
from dal_obscura.application.governance.errors import ValidationFailure


def test_arrow_synthetic_field_ids_are_schema_scoped() -> None:
    integer_schema = pa.schema([pa.field("value", pa.int64())])
    string_schema = pa.schema([pa.field("value", pa.string())])

    integer_id = numeric_field_id(
        integer_schema.field("value"), ("value",), schema_scope_digest(integer_schema)
    )
    string_id = numeric_field_id(
        string_schema.field("value"), ("value",), schema_scope_digest(string_schema)
    )

    assert integer_id != string_id
    assert 0 <= integer_id <= 2**31 - 1
    assert 0 <= string_id <= 2**31 - 1


def test_schema_fingerprint_includes_collection_ids_and_matches_arrow_normalization() -> None:
    original = Schema(
        NestedField(
            field_id=1,
            name="items",
            field_type=ListType(
                element_id=2,
                element_type=StructType(
                    NestedField(field_id=3, name="value", field_type=StringType())
                ),
            ),
        )
    )
    changed_element = Schema(
        NestedField(
            field_id=1,
            name="items",
            field_type=ListType(
                element_id=99,
                element_type=StructType(
                    NestedField(field_id=3, name="value", field_type=StringType())
                ),
            ),
        )
    )

    assert schema_fingerprint(original) == schema_fingerprint(original.as_arrow())
    assert schema_fingerprint(original) != schema_fingerprint(changed_element)


def test_schema_fingerprint_rejects_excessive_arrow_nesting() -> None:
    nested: pa.DataType = pa.string()
    for _ in range(MAX_SCHEMA_DEPTH + 1):
        nested = pa.struct([pa.field("child", nested)])

    with pytest.raises(ValidationFailure, match="Arrow schema exceeds"):
        schema_fingerprint(pa.schema([pa.field("root", nested)]))


@pytest.mark.parametrize("location", ["schema", "field", "list-element", "map-value"])
@pytest.mark.parametrize("part", ["key", "value"])
def test_schema_identity_distinguishes_binary_metadata(location: str, part: str) -> None:
    def schema(raw: bytes) -> pa.Schema:
        metadata = {raw: b"value"} if part == "key" else {b"key": raw}
        leaf = pa.field("leaf", pa.string(), metadata=metadata)
        field = (
            pa.field("items", pa.list_(leaf))
            if location == "list-element"
            else pa.field("labels", pa.map_(pa.string(), leaf))
            if location == "map-value"
            else leaf
        )
        return (
            pa.schema([pa.field("leaf", pa.string())], metadata=metadata)
            if location == "schema"
            else pa.schema([field])
        )

    binary = schema(b"\xff")
    literal_escape = schema(b"\\xff")

    assert schema_fingerprint(binary) != schema_fingerprint(literal_escape)
    assert schema_scope_digest(binary) != schema_scope_digest(literal_escape)


def test_schema_identity_ignores_metadata_insertion_order() -> None:
    def schema(metadata: dict[bytes, bytes]) -> pa.Schema:
        return pa.schema([pa.field("value", pa.string(), metadata=metadata)], metadata=metadata)

    forward = schema({b"\xff": b"\x00", b"text": b"\\xff"})
    reverse = schema({b"text": b"\\xff", b"\xff": b"\x00"})
    assert schema_fingerprint(forward) == schema_fingerprint(reverse)
    assert schema_scope_digest(forward) == schema_scope_digest(reverse)


@pytest.mark.parametrize("name", ["界", "x" * 4095 + '\\"界', "🙂" * 4097])
def test_schema_identity_matches_independent_version_two_encoding(name: str) -> None:
    schema = pa.schema(
        [pa.field(name, pa.list_(pa.field("item", pa.string(), metadata={b"k": b"\xff"})))],
        metadata={b"s": b"v"},
    )
    expected = {
        "encoding": 2,
        "fields": [
            {
                "name": name,
                "nullable": True,
                "metadata": [],
                "type": {
                    "id": "list<item: string>",
                    "value_field": {
                        "name": "item",
                        "nullable": True,
                        "metadata": [["aw==", "/w=="]],
                        "type": {"id": "string"},
                    },
                },
            }
        ],
        "metadata": [["cw==", "dg=="]],
    }
    for digest, ensure_ascii in [(schema_fingerprint, False), (schema_scope_digest, True)]:
        encoded = json.dumps(
            expected, sort_keys=True, separators=(",", ":"), ensure_ascii=ensure_ascii
        ).encode("utf-8")
        assert digest(schema) == hashlib.sha256(encoded).hexdigest()


def test_schema_scope_rejects_json_expansion_within_arrow_ipc_budget() -> None:
    schema = pa.schema([pa.field("界" * 400_000, pa.string())])
    # UTF-8 Arrow IPC fits; the existing ASCII scope encoding expands each name
    # character to six bytes. Authoring fingerprints use UTF-8 and still fit.
    assert len(schema_fingerprint(schema)) == 64
    with pytest.raises(ValueError, match="Schema encoding exceeds"):
        schema_scope_digest(schema)


def test_schema_identity_rejects_base64_expansion_within_arrow_ipc_budget() -> None:
    schema = pa.schema([pa.field("value", pa.string())], metadata={b"key": b"x" * 1_600_000})
    with pytest.raises(ValidationFailure, match="Schema encoding exceeds"):
        schema_fingerprint(schema)


@pytest.mark.parametrize(
    ("original", "changed"),
    [
        pytest.param(pa.list_(pa.string()), pa.large_list(pa.string()), id="list-width"),
        pytest.param(pa.list_(pa.string(), 2), pa.list_(pa.string(), 3), id="fixed-list-size"),
        pytest.param(
            pa.map_(pa.string(), pa.int32()),
            pa.map_(pa.string(), pa.int32(), keys_sorted=True),
            id="map-key-order",
        ),
        pytest.param(
            pa.list_(pa.field("item", pa.string(), nullable=True)),
            pa.list_(pa.field("item", pa.string(), nullable=False)),
            id="child-nullability",
        ),
        pytest.param(pa.timestamp("us", "UTC"), pa.timestamp("us", "Europe/Paris"), id="timezone"),
        pytest.param(pa.decimal128(10, 2), pa.decimal128(10, 3), id="decimal-scale"),
        pytest.param(
            pa.struct([pa.field("first", pa.string()), pa.field("second", pa.string())]),
            pa.struct([pa.field("second", pa.string()), pa.field("first", pa.string())]),
            id="child-order",
        ),
    ],
)
def test_schema_identity_preserves_type_semantics(
    original: pa.DataType, changed: pa.DataType
) -> None:
    before = pa.schema([pa.field("value", original)])
    after = pa.schema([pa.field("value", changed)])
    assert schema_fingerprint(before) != schema_fingerprint(after)
    assert schema_scope_digest(before) != schema_scope_digest(after)
