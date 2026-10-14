from __future__ import annotations

import pyarrow as pa
import pytest
from dal_obscura_plugin_api.schema.bounds import (
    validate_arrow_schema_bounds,
    validate_native_schema,
)
from dal_obscura_plugin_conformance.schemas import nested_struct_schema


def test_schema_bounds_accept_exact_limits_and_nested_collections():
    schema = pa.schema(
        [
            pa.field(
                "payload",
                pa.struct(
                    [
                        pa.field("items", pa.list_(pa.struct([pa.field("value", pa.string())]))),
                        pa.field("labels", pa.map_(pa.string(), pa.int32())),
                    ]
                ),
            )
        ]
    )

    validate_arrow_schema_bounds(schema, max_nodes=7, max_depth=4)


def test_schema_bounds_reject_depth_nodes_and_encoded_bytes():
    deep = pa.schema([pa.field("a", pa.struct([pa.field("b", pa.string())]))])
    with pytest.raises(ValueError, match="nesting-depth"):
        validate_arrow_schema_bounds(deep, max_depth=1)

    wide = pa.schema([pa.field(f"field_{index}", pa.string()) for index in range(3)])
    with pytest.raises(ValueError, match="field-node"):
        validate_arrow_schema_bounds(wide, max_nodes=2)

    with pytest.raises(ValueError, match="encoding"):
        validate_arrow_schema_bounds(wide, max_encoding_bytes=1)


@pytest.mark.parametrize("nested", [False, True], ids=["root", "struct"])
def test_schema_bounds_reject_ambiguous_sibling_field_names(nested):
    fields = [pa.field("duplicate", pa.int64()), pa.field("duplicate", pa.string())]
    schema = pa.schema([pa.field("parent", pa.struct(fields))] if nested else fields)
    with pytest.raises(ValueError, match="Duplicate Arrow field name"):
        validate_arrow_schema_bounds(schema)


def test_executable_schema_accepts_exact_native_boundary_and_binary_metadata():
    schema = nested_struct_schema(62)
    validate_native_schema(schema)
    assert schema.metadata == {b"binary": b"\xff"}


def test_executable_schema_rejects_struct_depth_that_serializes_but_cannot_interchange():
    schema = nested_struct_schema(63)
    validate_arrow_schema_bounds(schema)
    assert pa.ipc.read_schema(schema.serialize()).equals(schema, check_metadata=True)
    with pytest.raises(ValueError, match="native interchange"):
        validate_native_schema(schema)


def test_executable_schema_rejects_field_identity_lost_in_native_interchange():
    with pytest.raises(ValueError, match="native interchange"):
        validate_native_schema(pa.schema([pa.field("field\x00suffix", pa.string())]))
