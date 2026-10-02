import pyarrow as pa
import pytest

import dal_obscura.read.transform as duckdb_transform
from dal_obscura.policy.models import MaskRule
from dal_obscura.policy.projection import compile_projection
from dal_obscura.read.transform import (
    DuckDBRowTransformAdapter,
)


def test_duckdb_transform_executes_projection_and_mask_for_quoted_identifiers():
    from hashlib import sha256

    batch = pa.record_batch(
        [pa.array([7, 8]), pa.array(["secret", None]), pa.array([9, 10])],
        names=["id + 1", 'bad"name', "x; SELECT 1"],
    )
    columns = ['["id + 1"]', '["bad\\"name"]', '["x; SELECT 1"]']
    masks = {columns[1]: MaskRule(type="hash")}

    result = pa.Table.from_batches(
        list(
            DuckDBRowTransformAdapter().apply_filters_and_masks_stream(
                [batch], columns, None, masks
            )
        )
    )
    assert result.schema.equals(compile_projection(batch.schema, columns, masks).output_schema)
    assert result.to_pydict() == {
        "id + 1": [7, 8],
        'bad"name': [sha256(b"secret").hexdigest(), None],
        "x; SELECT 1": [9, 10],
    }


def test_nested_projection_ignores_masks_outside_the_selected_path():
    address = pa.struct([pa.field("zip", pa.int64()), pa.field("city", pa.string())])
    batch = pa.record_batch(
        [
            pa.array(
                [{"address": {"zip": 1234, "city": "London"}}],
                type=pa.struct([pa.field("address", address)]),
            ),
            pa.array(
                [{"address": {"zip": 5678, "city": "Paris"}}],
                type=pa.struct([pa.field("address", address)]),
            ),
        ],
        names=["user", "account"],
    )
    columns = ["user.address.zip"]
    masks = {
        "account.address.zip": MaskRule(type="hash"),
        "user.address.city": MaskRule(type="redact", value="hidden"),
    }

    result = pa.Table.from_batches(
        list(
            DuckDBRowTransformAdapter().apply_filters_and_masks_stream(
                [batch], columns, None, masks
            )
        )
    )
    assert result.to_pylist() == [{"user": {"address": {"zip": 1234}}}]
    assert result.schema.equals(compile_projection(batch.schema, columns, masks).output_schema)


def test_nested_child_projection_cannot_bypass_null_mask_on_parent():
    profile_type = pa.struct([pa.field("ssn", pa.string()), pa.field("name", pa.string())])
    input_batch = pa.record_batch(
        [pa.array([{"ssn": "123-45-6789", "name": "Ada"}], type=profile_type)],
        names=["profile"],
    )

    result_batches = list(
        DuckDBRowTransformAdapter().apply_filters_and_masks_stream(
            [input_batch],
            ["profile.ssn"],
            None,
            {"profile": MaskRule(type="null")},
        )
    )

    result = pa.Table.from_batches(result_batches)

    assert result.schema.names == ["profile"]
    assert result.column("profile").to_pylist() == [None]


def test_masked_schema_updates_nested_field_types():
    schema = pa.schema(
        [
            pa.field(
                "user",
                pa.struct(
                    [
                        pa.field(
                            "address",
                            pa.struct(
                                [
                                    pa.field("zip", pa.int64()),
                                    pa.field("city", pa.string()),
                                ]
                            ),
                        )
                    ]
                ),
            )
        ]
    )

    masked_schema = compile_projection(
        schema, ["user"], {"user.address.zip": MaskRule(type="hash")}
    ).output_schema

    address_field = masked_schema.field("user").type.field("address")
    assert address_field.type.field("zip").type == pa.string()


def test_masked_schema_exposes_nested_projection_as_pruned_struct():
    schema = pa.schema(
        [
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
            )
        ]
    )

    masked_schema = compile_projection(
        schema, ["user.address.zip"], {"user.address.zip": MaskRule(type="hash")}
    ).output_schema

    assert masked_schema.names == ["user"]
    address_field = masked_schema.field("user").type.field("address")
    assert address_field.type.names == ["zip"]
    assert address_field.type.field("zip").type == pa.string()


def test_redact_mask_preserves_null_and_honors_an_empty_replacement():
    adapter = DuckDBRowTransformAdapter()
    batch = pa.record_batch(
        [pa.array(["secret", None], type=pa.string())],
        names=["email"],
    )

    result = pa.Table.from_batches(
        list(
            adapter.apply_filters_and_masks_stream(
                [batch],
                ["email"],
                None,
                {"email": MaskRule(type="redact", value="")},
            )
        )
    )

    assert result.column("email").to_pylist() == ["", None]


def test_email_mask_nulls_malformed_values_instead_of_passing_them_through():
    adapter = DuckDBRowTransformAdapter()
    batch = pa.record_batch(
        [
            pa.array(
                ["ada@example.com", "", "missing-domain@", "two@@example.com", None],
                type=pa.string(),
            )
        ],
        names=["email"],
    )

    result = pa.Table.from_batches(
        list(
            adapter.apply_filters_and_masks_stream(
                [batch], ["email"], None, {"email": MaskRule(type="email")}
            )
        )
    )

    assert result.column("email").to_pylist() == ["a***@example.com", None, None, None, None]


def test_duckdb_transform_returns_empty_iterator_without_connecting(monkeypatch):
    connect_calls = 0

    def fake_connect(**_kwargs):
        nonlocal connect_calls
        connect_calls += 1
        raise AssertionError("connect should not be called for empty input")

    monkeypatch.setattr(duckdb_transform.duckdb, "connect", fake_connect)
    adapter = DuckDBRowTransformAdapter()

    assert list(adapter.apply_filters_and_masks_stream([], ["id"], None, {})) == []
    assert connect_calls == 0


def test_duckdb_transform_applies_list_of_struct_mask():
    adapter = DuckDBRowTransformAdapter()
    preference_type = pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("theme", pa.string()),
        ]
    )
    schema = pa.schema(
        [
            pa.field(
                "metadata",
                pa.struct(
                    [
                        pa.field("preferences", pa.list_(preference_type)),
                    ]
                ),
            )
        ]
    )
    input_batch = pa.record_batch(
        [
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
            )
        ],
        schema=schema,
    )

    result_batches = list(
        adapter.apply_filters_and_masks_stream(
            [input_batch],
            ["metadata"],
            None,
            {"metadata.preferences.$element.theme": MaskRule(type="redact", value="[hidden]")},
        )
    )
    result = pa.Table.from_batches(result_batches)

    preferences = result.column("metadata").to_pylist()[0]["preferences"]
    assert [item["theme"] for item in preferences] == ["[hidden]", "[hidden]"]
    assert result.schema.equals(
        compile_projection(
            input_batch.schema,
            ["metadata"],
            {"metadata.preferences.$element.theme": MaskRule(type="redact", value="[hidden]")},
        ).output_schema
    )


def test_duckdb_transform_preserves_literal_dotted_top_level_field_name():
    adapter = DuckDBRowTransformAdapter()
    input_batch = pa.record_batch(
        [pa.array(["visible"], type=pa.string())],
        names=["profile.name"],
    )

    result = pa.Table.from_batches(
        list(
            adapter.apply_filters_and_masks_stream(
                [input_batch],
                ['["profile.name"]'],
                None,
                {},
            )
        )
    )

    assert result.schema.names == ["profile.name"]
    assert result.column("profile.name").to_pylist() == ["visible"]


def test_duckdb_transform_projects_and_masks_map_value_struct_leaves():
    adapter = DuckDBRowTransformAdapter()
    value_type = pa.struct([pa.field("name", pa.string()), pa.field("ssn", pa.string())])
    input_batch = pa.record_batch(
        [
            pa.array(
                [[("primary", {"name": "Ada", "ssn": "123"})]],
                type=pa.map_(pa.string(), value_type),
            )
        ],
        names=["contacts"],
    )

    result = pa.Table.from_batches(
        list(
            adapter.apply_filters_and_masks_stream(
                [input_batch],
                ["contacts.$key", "contacts.$value.name"],
                None,
                {"contacts.$value.name": MaskRule(type="redact", value="[hidden]")},
            )
        )
    )

    assert result.column("contacts").to_pylist() == [[("primary", {"name": "[hidden]"})]]
    assert result.schema.equals(
        compile_projection(
            input_batch.schema,
            ["contacts.$key", "contacts.$value.name"],
            {"contacts.$value.name": MaskRule(type="redact", value="[hidden]")},
        ).output_schema
    )


def test_duckdb_transform_projects_and_masks_canonical_list_element_leaves():
    adapter = DuckDBRowTransformAdapter()
    value_type = pa.struct([pa.field("email", pa.string()), pa.field("ssn", pa.string())])
    input_batch = pa.record_batch(
        [
            pa.array(
                [[{"email": "ada@example.com", "ssn": "123"}], None],
                type=pa.list_(value_type),
            )
        ],
        names=["contacts"],
    )

    result = pa.Table.from_batches(
        list(
            adapter.apply_filters_and_masks_stream(
                [input_batch],
                ["contacts.$element.email"],
                None,
                {"contacts.$element.email": MaskRule(type="email")},
            )
        )
    )

    assert result.column("contacts").to_pylist() == [[{"email": "a***@example.com"}], None]
    assert result.schema.equals(
        compile_projection(
            input_batch.schema,
            ["contacts.$element.email"],
            {"contacts.$element.email": MaskRule(type="email")},
        ).output_schema
    )


def test_null_map_key_hides_entire_map_and_preserves_projected_value_schema():
    value_type = pa.struct([pa.field("name", pa.string()), pa.field("secret", pa.string())])
    schema = pa.schema([pa.field("contacts", pa.map_(pa.string(), value_type), nullable=False)])
    batch = pa.RecordBatch.from_pylist(
        [{"contacts": [("private-key", {"name": "Ada", "secret": "secret"})]}], schema=schema
    )
    columns = ["contacts.$key", "contacts.$value.name"]
    masks = {"contacts.$key": MaskRule(type="null")}

    result = pa.Table.from_batches(
        list(
            DuckDBRowTransformAdapter().apply_filters_and_masks_stream(
                [batch], columns, None, masks
            )
        )
    )
    assert result.to_pylist() == [{"contacts": None}]
    assert result.schema == compile_projection(schema, columns, masks).output_schema
    assert result.schema.field("contacts").type.item_type.names == ["name"]


def test_null_default_masks_deep_list_siblings_without_hiding_selected_leaf():
    from dal_obscura.policy.models import (
        AccessRule,
        DatasetPolicy,
        Policy,
        Principal,
    )
    from dal_obscura.policy.policy_resolution import resolve_access
    from dal_obscura.policy.schema_index import SchemaIndex

    leaf = pa.struct([pa.field("city", pa.string()), pa.field("postcode", pa.string())])
    element = pa.struct([pa.field("details", pa.struct([pa.field("address", leaf)]))])
    schema = pa.schema([pa.field("contacts", pa.list_(element))])
    batch = pa.RecordBatch.from_pylist(
        [{"contacts": [{"details": {"address": {"city": "Paris", "postcode": "private"}}}]}],
        schema=schema,
    )
    path = "contacts.$element.details.address.city"
    policy = Policy(
        version=1,
        datasets=[
            DatasetPolicy(
                catalog="demo",
                target="users",
                rules=[AccessRule(principals=["*"], columns=[path], masks={}, row_filter=None)],
            )
        ],
    )
    columns, masks, _ = resolve_access(
        policy,
        Principal(id="alice", groups=[], attributes={}),
        "users",
        "demo",
        SchemaIndex(schema).expand(["contacts"]),
    )
    result = pa.Table.from_batches(
        list(
            DuckDBRowTransformAdapter().apply_filters_and_masks_stream(
                [batch], columns, None, masks
            )
        )
    )
    assert result.to_pylist() == [
        {"contacts": [{"details": {"address": {"city": "Paris", "postcode": None}}}]}
    ]


def test_implicit_list_element_mask_paths_are_rejected():
    schema = pa.schema(
        [pa.field("contacts", pa.list_(pa.struct([pa.field("email", pa.string())])))]
    )

    for operation in (compile_projection,):
        with pytest.raises(ValueError, match="does not contain a struct"):
            operation(schema, ["contacts"], {"contacts.email": MaskRule(type="hash")})


@pytest.mark.parametrize(
    "mask",
    [
        MaskRule(type="unsupported"),
        MaskRule(type="keep_last", value=True),
        MaskRule(type="hash", value="ignored"),
    ],
)
def test_mask_schema_and_execution_reject_the_same_invalid_configuration(mask):
    schema = pa.schema([pa.field("id", pa.int64())])

    for operation in (compile_projection,):
        with pytest.raises(ValueError):
            operation(schema, ["id"], {"id": mask})


@pytest.mark.parametrize("container", ["struct", "list", "map"])
@pytest.mark.parametrize(
    ("mask", "masked_value"),
    [
        (MaskRule(type="null"), None),
        (MaskRule(type="default", value=None), None),
        (MaskRule(type="redact", value="hidden"), "hidden"),
        (MaskRule(type="default", value="replacement"), "replacement"),
    ],
)
def test_nested_parent_mask_governs_pruned_descendant(container, mask, masked_value):
    secret = pa.struct([pa.field("secret", pa.string()), pa.field("other", pa.string())])
    item = pa.struct([pa.field("details", secret)])
    if container == "struct":
        data_type = item
        value = {"details": {"secret": "private", "other": "hidden"}}
        columns = ["root.details.secret"]
        parent = "root.details"
        expected = {"root": {"details": masked_value}}
    elif container == "list":
        data_type = pa.list_(item)
        value = [{"details": {"secret": "private", "other": "hidden"}}]
        columns = ["root.$element.details.secret"]
        parent = "root.$element.details"
        expected = {"root": [{"details": masked_value}]}
    else:
        data_type = pa.map_(pa.string(), item)
        value = [("key", {"details": {"secret": "private", "other": "hidden"}})]
        columns = ["root.$key", "root.$value.details.secret"]
        parent = "root.$value.details"
        expected = {"root": [("key", {"details": masked_value})]}
    batch = pa.RecordBatch.from_pylist([{"root": value}], schema=pa.schema([("root", data_type)]))
    masks = {parent: mask, f"{parent}.secret": MaskRule(type="redact", value="child")}

    result = pa.Table.from_batches(
        list(
            DuckDBRowTransformAdapter().apply_filters_and_masks_stream(
                [batch], columns, None, masks
            )
        )
    )
    assert result.to_pylist() == [expected]
    assert result.schema == compile_projection(batch.schema, columns, masks).output_schema


@pytest.mark.parametrize("hide_key", [False, True])
def test_full_map_selection_enforces_descendant_masks(hide_key):
    schema = pa.schema([("contacts", pa.map_(pa.string(), pa.struct([("secret", pa.string())])))])
    batch = pa.RecordBatch.from_pylist(
        [
            {"contacts": [("private-key", {"secret": "raw-secret"})]},
            {"contacts": None},
            {"contacts": []},
        ],
        schema=schema,
    )
    masks = {"contacts.$value.secret": MaskRule(type="null")}
    if hide_key:
        masks["contacts.$key"] = MaskRule(type="null")

    result = pa.Table.from_batches(
        list(
            DuckDBRowTransformAdapter().apply_filters_and_masks_stream(
                [batch], ["contacts"], None, masks
            )
        )
    )
    expected = [None, None, None] if hide_key else [[("private-key", {"secret": None})], None, []]
    assert result.column("contacts").to_pylist() == expected
    assert result.schema == compile_projection(schema, ["contacts"], masks).output_schema


def test_scalar_masks_execute_values_and_match_advertised_schema():
    from hashlib import sha256

    masks = {
        "hashed": MaskRule(type="hash"),
        "redacted": MaskRule(type="redact", value="***"),
        "defaulted": MaskRule(type="default", value="unknown"),
        "default_null": MaskRule(type="default", value=None),
        "email": MaskRule(type="email"),
        "suffix": MaskRule(type="keep_last", value=2),
        "hidden": MaskRule(type="null"),
    }
    batch = pa.record_batch(
        [
            pa.array([123, None]),
            pa.array(["secret", None]),
            pa.array(["secret", None]),
            pa.array(["secret", None]),
            pa.array(["ada@example.com", None]),
            pa.array([123456, None]),
            pa.array([123, None]),
        ],
        names=list(masks),
    )

    result = pa.Table.from_batches(
        list(
            DuckDBRowTransformAdapter().apply_filters_and_masks_stream(
                [batch], list(masks), None, masks
            )
        )
    )
    assert result.schema.equals(compile_projection(batch.schema, list(masks), masks).output_schema)
    assert result.to_pydict() == {
        "hashed": [sha256(b"123").hexdigest(), None],
        "redacted": ["***", None],
        "defaulted": ["unknown", "unknown"],
        "default_null": [None, None],
        "email": ["a***@example.com", None],
        "suffix": ["****56", None],
        "hidden": [None, None],
    }
