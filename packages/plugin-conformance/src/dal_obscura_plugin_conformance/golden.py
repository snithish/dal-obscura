"""Small deterministic nested fixtures shared by plugin conformance tests."""

from __future__ import annotations

import pyarrow as pa


def nested_golden_table() -> pa.Table:
    """Return a stable nested table with struct/list/map values."""

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field(
                "profile",
                pa.struct([pa.field("email", pa.string()), pa.field("region", pa.string())]),
            ),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("labels", pa.map_(pa.string(), pa.string())),
        ]
    )
    return pa.Table.from_pylist(
        [
            {
                "id": 1,
                "profile": {"email": "alice@example.com", "region": "eu"},
                "tags": ["a", "b"],
                "labels": {"tier": "gold"},
            },
            {
                "id": 2,
                "profile": {"email": "bob@example.com", "region": "us"},
                "tags": ["b"],
                "labels": {"tier": "silver"},
            },
        ],
        schema=schema,
    )
