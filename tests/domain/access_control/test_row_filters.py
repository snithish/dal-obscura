import pyarrow as pa
import pytest

from dal_obscura.common.access_control.filters import (
    RowFilter,
    deserialize_row_filter,
    extract_row_filter_dependencies,
    parse_row_filter,
    row_filter_to_sql,
    serialize_row_filter,
)
from tests.support.row_filters import (
    PARSER_MULTIPLE_STATEMENT_ROW_FILTERS,
    PARSER_NON_FILTER_STATEMENT_ROW_FILTERS,
    PARSER_UNSAFE_EXPRESSION_ROW_FILTERS,
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


def test_parse_row_filter_accepts_function_and_computed_predicates():
    row_filter = parse_row_filter("lower(region) = 'us' AND id + 1 > 5", _schema())

    assert isinstance(row_filter, RowFilter)
    assert row_filter_to_sql(row_filter) == "LOWER(region) = 'us' AND id + 1 > 5"


def test_parse_row_filter_validates_columns_inside_functions():
    with pytest.raises(ValueError, match="Unknown column"):
        parse_row_filter("lower(missing) = 'us'", _schema())


def test_parse_row_filter_extracts_dependencies_from_nested_function_predicates():
    row_filter = parse_row_filter(
        "COALESCE(user.address.zip, 0) > 10000 AND lower(region) = 'us'",
        _schema(),
    )

    assert extract_row_filter_dependencies(row_filter) == ["user.address.zip", "region"]


def test_parse_row_filter_distinguishes_quoted_literal_dots_from_nested_fields():
    schema = pa.schema(
        [
            pa.field("a.b", pa.int64()),
            pa.field("a", pa.struct([pa.field("b", pa.int64())])),
        ]
    )

    literal = parse_row_filter('"a.b" = 1', schema)
    nested = parse_row_filter("a.b = 1", schema)

    assert extract_row_filter_dependencies(literal) == ['["a.b"]']
    assert extract_row_filter_dependencies(nested) == ["a.b"]


def test_row_filter_serialization_round_trips_as_sql_string():
    row_filter = parse_row_filter("lower(region) = 'us' AND active", _schema())

    payload = serialize_row_filter(row_filter)

    assert payload == "LOWER(region) = 'us' AND active"
    assert deserialize_row_filter(payload) == row_filter


def test_row_filter_deserialization_rejects_non_string_payload():
    with pytest.raises(ValueError, match="Invalid row filter payload"):
        deserialize_row_filter({"type": "comparison"})


@pytest.mark.parametrize(
    "payload, expected",
    [
        ("region = 'us' OR id = 1", "region = 'us' OR id = 1"),
        ("region IN ('us', 'eu')", "region IN ('us', 'eu')"),
        ("active IS NOT NULL", "NOT active IS NULL"),
        (
            "COALESCE(user.address.zip, 0) > 10000",
            'COALESCE("user".address.zip, 0) > 10000',
        ),
        ("id + 1 > 5", "id + 1 > 5"),
    ],
)
def test_parse_row_filter_keeps_supported_filter_subset(payload, expected):
    row_filter = parse_row_filter(payload, _schema())

    assert row_filter_to_sql(row_filter) == expected


@pytest.mark.parametrize(
    "payload,message",
    [
        pytest.param("SELECT * FROM users", "Row filter must be a DuckDB expression", id="query"),
        *[(sql, "single DuckDB expression") for sql in PARSER_MULTIPLE_STATEMENT_ROW_FILTERS],
        *[
            (sql, "Unsupported row filter expression")
            for sql in PARSER_NON_FILTER_STATEMENT_ROW_FILTERS
        ],
        *[
            (sql, "Unsupported row filter expression")
            for sql in PARSER_UNSAFE_EXPRESSION_ROW_FILTERS
        ],
    ],
)
def test_parse_row_filter_rejects_unsafe_sql(payload, message):
    with pytest.raises(ValueError, match=message):
        parse_row_filter(payload, _schema())
