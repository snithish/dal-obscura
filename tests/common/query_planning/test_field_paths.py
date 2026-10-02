import pyarrow as pa
import pytest

from dal_obscura.policy.paths import (
    MAX_FIELD_PATH_SEGMENTS,
    FieldPath,
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    MapValueSegment,
    parse_field_path,
    resolve_schema_path,
)


def test_field_path_distinguishes_literal_dots_from_nested_fields():
    literal = parse_field_path('["a.b"]')
    nested = parse_field_path("a.b")

    assert literal != nested
    assert literal.to_wire() == {
        "version": 1,
        "segments": [{"kind": "field", "name": "a.b"}],
    }
    assert nested.to_human() == "a.b"


def test_field_path_preserves_quoted_reserved_collection_names():
    literal = FieldPath((FieldSegment("$element"), FieldSegment("$value")))

    rendered = literal.to_human()

    assert rendered == '["$element"].["$value"]'
    assert parse_field_path(rendered) == literal


def test_field_path_preserves_brackets_inside_quoted_names():
    literal = FieldPath((FieldSegment("name]with[brackets"),))

    rendered = literal.to_human()

    assert parse_field_path(rendered) == literal


def test_field_path_resolves_nested_struct_list_and_map_nodes():
    schema = pa.schema(
        [
            pa.field("a.b", pa.string()),
            pa.field(
                "profile",
                pa.struct(
                    [
                        pa.field(
                            "contacts",
                            pa.list_(pa.struct([pa.field("email", pa.string())])),
                        )
                    ]
                ),
            ),
            pa.field("labels", pa.map_(pa.string(), pa.struct([pa.field("rank", pa.int64())]))),
        ]
    )

    assert resolve_schema_path(schema, parse_field_path('["a.b"]')).type == pa.string()
    assert (
        resolve_schema_path(schema, parse_field_path("profile.contacts.$element.email")).type
        == pa.string()
    )
    assert resolve_schema_path(schema, parse_field_path("labels.$key")).type == pa.string()
    assert resolve_schema_path(schema, parse_field_path("labels.$value.rank")).type == pa.int64()


@pytest.mark.parametrize("path", ["", "a.", ".a", "a..b", "[not-json]", '[""]'])
def test_field_path_rejects_malformed_paths(path):
    with pytest.raises(ValueError):
        parse_field_path(path)


def test_field_path_rejects_collection_traversal_through_a_scalar():
    schema = pa.schema([pa.field("a", pa.string())])

    with pytest.raises(ValueError, match="does not contain a list"):
        resolve_schema_path(schema, parse_field_path("a.$element"))


def test_field_path_validates_wire_model_invariants():
    with pytest.raises(ValueError, match="begin"):
        FieldPath((ListElementSegment(),))
    with pytest.raises(ValueError, match="version"):
        FieldPath((FieldSegment("id"),), version=2)

    path = FieldPath((FieldSegment("tags"), ListElementSegment(), MapKeySegment()))
    assert path.to_human() == "tags.$element.$key"
    assert path.to_wire() == {
        "version": 1,
        "segments": [
            {"kind": "field", "name": "tags"},
            {"kind": "list_element"},
            {"kind": "map_key"},
        ],
    }
    assert MapValueSegment() != MapKeySegment()


def test_field_path_rejects_unbounded_or_control_bearing_names():
    with pytest.raises(ValueError, match="bounded printable"):
        FieldPath((FieldSegment("x" * 257),))
    with pytest.raises(ValueError, match="bounded printable"):
        FieldPath((FieldSegment("bad\nname"),))
    with pytest.raises(ValueError, match="too many segments"):
        FieldPath(tuple([FieldSegment("root")] + [FieldSegment("child")] * MAX_FIELD_PATH_SEGMENTS))


def test_field_path_rejects_out_of_range_field_ids():
    with pytest.raises(ValueError, match="nonnegative 32-bit"):
        FieldPath((FieldSegment("id", field_id=-1),))
    with pytest.raises(ValueError, match="nonnegative 32-bit"):
        FieldPath((FieldSegment("id", field_id=2**31),))


def test_field_path_wire_round_trip_preserves_ids_and_collection_nodes():
    path = FieldPath(
        (
            FieldSegment("labels", field_id=7),
            MapValueSegment(),
            FieldSegment("value.with.dot", field_id=9),
        )
    )

    assert FieldPath.from_wire(path.to_wire()) == path


def test_field_path_rejects_a_name_match_with_a_different_field_id():
    schema = pa.schema([pa.field("profile", pa.string(), metadata={b"PARQUET:field_id": b"7"})])

    path = FieldPath((FieldSegment("profile", field_id=7),))
    assert resolve_schema_path(schema, path).name == "profile"
    with pytest.raises(ValueError, match="Field ID does not match"):
        resolve_schema_path(schema, FieldPath((FieldSegment("profile", field_id=8),)))
    with pytest.raises(ValueError, match="no bound field ID"):
        resolve_schema_path(
            pa.schema([pa.field("profile", pa.string())]),
            FieldPath((FieldSegment("profile", field_id=7),)),
        )


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"version": 1, "segments": "not-a-list"},
        {"version": True, "segments": []},
        {"version": 1, "segments": [{"kind": "field", "name": "", "extra": 1}]},
        {"version": 1, "segments": [{"kind": "field", "name": "id", "field_id": True}]},
        {"version": 1, "segments": [{"kind": "map_value", "name": "unexpected"}]},
        {"version": 1, "segments": [{"kind": "unknown"}]},
    ],
)
def test_field_path_wire_rejects_unknown_or_malformed_values(value):
    with pytest.raises(ValueError):
        FieldPath.from_wire(value)
