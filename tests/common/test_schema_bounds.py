from __future__ import annotations

import pyarrow as pa
import pytest

from dal_obscura.common.schema_bounds import validate_arrow_schema_bounds


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
