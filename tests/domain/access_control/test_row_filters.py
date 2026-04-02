import pyarrow as pa
import pytest

from dal_obscura.domain.access_control.filters import (
    BooleanFilter,
    ComparisonFilter,
    InFilter,
    NullFilter,
    deserialize_row_filter,
    extract_row_filter_dependencies,
    parse_row_filter,
    row_filter_to_sql,
    serialize_row_filter,
)


def _schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("active", pa.bool_()),
            pa.field(
                "user",
                pa.struct(
                    [
                        pa.field(
                            "address",
                            pa.struct([pa.field("zip", pa.int64())]),
                        )
                    ]
                ),
            ),
        ]
    )


def test_parse_row_filter_validates_known_columns():
    with pytest.raises(ValueError, match="Unknown column"):
        parse_row_filter("missing = 1", _schema())


def test_parse_row_filter_rejects_unsupported_operator():
    with pytest.raises(
        ValueError,
        match=r"Unsupported row filter syntax|Expected literal|Expected comparison operator",
    ):
        parse_row_filter("region LIKE 'us%'", _schema())


def test_parse_row_filter_extracts_nested_dependencies():
    row_filter = parse_row_filter("user.address.zip > 10000 AND region = 'us'", _schema())

    assert extract_row_filter_dependencies(row_filter) == ["user.address.zip", "region"]


def test_parse_row_filter_renders_equivalent_duckdb_sql():
    row_filter = parse_row_filter(
        "(region = 'us' OR region = 'eu') AND active = true",
        _schema(),
    )

    assert row_filter_to_sql(row_filter) == (
        "((region = 'us') OR (region = 'eu')) AND (active = TRUE)"
    )


def test_parse_row_filter_supports_in_and_null_checks():
    row_filter = parse_row_filter("region IN ('us', 'eu') OR user.address.zip IS NULL", _schema())

    assert isinstance(row_filter, BooleanFilter)
    assert isinstance(row_filter.clauses[0], InFilter)
    assert isinstance(row_filter.clauses[1], NullFilter)


def test_row_filter_serialization_round_trips():
    row_filter = parse_row_filter("region = 'us' AND active = true", _schema())

    restored = deserialize_row_filter(serialize_row_filter(row_filter))

    assert restored == row_filter


def test_row_filter_deserialization_rejects_invalid_payload():
    with pytest.raises(ValueError, match="Invalid row filter payload"):
        deserialize_row_filter({"type": "comparison", "field": "region"})


def test_parse_row_filter_returns_comparison_node():
    row_filter = parse_row_filter("region = 'us'", _schema())

    assert isinstance(row_filter, ComparisonFilter)
