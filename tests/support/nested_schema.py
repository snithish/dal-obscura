"""Exact nested schemas for native interchange boundary qualification."""

import pyarrow as pa


def nested_struct_schema(layers: int) -> pa.Schema:
    data_type: pa.DataType = pa.string()
    for index in range(layers):
        data_type = pa.struct(
            [pa.field(f"level_{index}", data_type, metadata={b"layer": str(index).encode()})]
        )
    return pa.schema([pa.field("root", data_type)], metadata={b"binary": b"\xff"})
