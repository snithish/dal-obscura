from __future__ import annotations

import pyarrow as pa


def id_region_schema() -> pa.Schema:
    return pa.schema([pa.field("id", pa.int64()), pa.field("region", pa.string())])


def id_region_batch(ids: list[int], regions: list[str]) -> pa.RecordBatch:
    return pa.record_batch(
        [pa.array(ids, type=pa.int64()), pa.array(regions, type=pa.string())],
        schema=id_region_schema(),
    )


def nested_user_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("active", pa.bool_()),
            pa.field(
                "user",
                pa.struct(
                    [
                        pa.field("email", pa.string()),
                        pa.field("address", pa.struct([pa.field("zip", pa.int64())])),
                    ]
                ),
            ),
        ]
    )


def list_of_struct_metadata_type(*, large: bool = False) -> pa.DataType:
    preference_type = pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("theme", pa.string()),
        ]
    )
    value_type = (
        pa.large_list(pa.field("item", preference_type)) if large else pa.list_(preference_type)
    )
    return pa.struct([pa.field("preferences", value_type)])


def metadata_schema(*, large_list: bool = False) -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("metadata", list_of_struct_metadata_type(large=large_list)),
        ]
    )


def metadata_batch(*, large_list: bool = False) -> pa.RecordBatch:
    schema = metadata_schema(large_list=large_list)
    return pa.record_batch(
        [
            pa.array([1], type=pa.int64()),
            pa.array(
                [
                    {
                        "preferences": [
                            {"name": "web", "theme": "dark"},
                            {"name": "mobile", "theme": "light"},
                        ]
                    }
                ],
                type=schema.field("metadata").type,
            ),
        ],
        schema=schema,
    )
