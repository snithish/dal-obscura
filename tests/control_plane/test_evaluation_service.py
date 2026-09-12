from __future__ import annotations

import pyarrow as pa

from dal_obscura.control_plane.application.evaluation_service import _leaf_paths


def test_leaf_paths_keep_literal_dotted_names_distinct_from_nested_fields() -> None:
    schema = pa.schema(
        [
            pa.field("a.b", pa.string()),
            pa.field("a", pa.struct([pa.field("b", pa.string())])),
        ]
    )

    assert _leaf_paths(schema) == ['["a.b"]', "a.b"]
