import pyarrow as pa
import pytest

from dal_obscura.common.query_planning.field_paths import (
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


@pytest.mark.parametrize(
    "path",
    ["", "a.", ".a", "a..b", "a.$element", "[not-json]", "[\"\"]"],
)
def test_field_path_rejects_malformed_or_incompatible_paths(path):
    schema = pa.schema([pa.field("a", pa.string())])

    if path in {"", "a.", ".a", "a..b", "[not-json]", "[\"\"]"}:
        with pytest.raises(ValueError):
            parse_field_path(path)
    else:
        with pytest.raises(ValueError, match="does not contain a list"):
            resolve_schema_path(schema, parse_field_path(path))


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
