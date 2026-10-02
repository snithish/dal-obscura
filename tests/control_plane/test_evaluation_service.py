from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal

import pyarrow as pa
import pytest

from dal_obscura.control.errors import ValidationFailure
from dal_obscura.control.evaluation_service import (
    MAX_SYNTHETIC_BYTES,
    _sample_row,
    _validate_synthetic_rows,
)
from dal_obscura.policy.schema_index import SchemaIndex


def test_leaf_paths_keep_literal_dotted_names_distinct_from_nested_fields() -> None:
    schema = pa.schema(
        [
            pa.field("a.b", pa.string()),
            pa.field("a", pa.struct([pa.field("b", pa.string())])),
        ]
    )

    assert SchemaIndex(schema).expand(["*"]) == ['["a.b"]', "a.b"]


def test_sample_row_matches_nested_typed_arrow_schema() -> None:
    schema = pa.schema(
        [
            pa.field("when", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("day", pa.date32(), nullable=False),
            pa.field("at", pa.time64("us"), nullable=False),
            pa.field("amount", pa.decimal128(8, 2), nullable=False),
            pa.field("ids", pa.map_(pa.int32(), pa.string()), nullable=False),
            pa.field("payload", pa.struct([pa.field("email", pa.string())]), nullable=False),
        ]
    )

    row = _sample_row(schema)
    table = pa.Table.from_pylist([row], schema=schema)

    assert table.num_rows == 1
    assert isinstance(row["day"], date)
    assert isinstance(row["when"], datetime)
    assert isinstance(row["at"], time)
    assert isinstance(row["amount"], Decimal)
    assert row["ids"] == {1: "synthetic"}


def test_sample_row_supports_fixed_size_lists_and_top_level_collections() -> None:
    schema = pa.schema(
        [
            pa.field("fixed", pa.list_(pa.field("item", pa.int16()), 2), nullable=False),
            pa.field("large", pa.large_list(pa.field("item", pa.string())), nullable=False),
        ]
    )

    row = _sample_row(schema)
    table = pa.Table.from_pylist([row], schema=schema)

    assert table.num_rows == 1
    assert row["fixed"] == [1, 1]
    assert row["large"] == ["synthetic"]
    assert SchemaIndex(schema).expand(["*"]) == ["fixed.$element", "large.$element"]


def test_sample_row_rejects_unsupported_types_instead_of_inventing_values() -> None:
    schema = pa.schema([pa.field("duration", pa.duration("us"), nullable=False)])

    try:
        _sample_row(schema)
    except Exception as exc:
        assert "does not support Arrow type duration" in str(exc)
    else:
        raise AssertionError("unsupported synthetic type must fail closed")


def test_synthetic_row_budget_rejects_encoded_payload_explosion() -> None:
    with pytest.raises(ValidationFailure, match="encoded bytes"):
        _validate_synthetic_rows([{"payload": "x" * MAX_SYNTHETIC_BYTES}])
