import textwrap
from collections.abc import Generator
from typing import cast

import pyarrow as pa
import pytest

import dal_obscura.data_plane.infrastructure.adapters.duckdb_transform as duckdb_transform
from dal_obscura.common.access_control.filters import parse_row_filter
from dal_obscura.common.access_control.models import MaskRule
from dal_obscura.data_plane.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
    InputBatchLimitError,
    StreamAdmissionError,
    StreamMemoryLimitError,
)
from tests.support.memory_probe import run_memory_probe


def test_duckdb_transform_connection_disables_progress_bar(monkeypatch: pytest.MonkeyPatch):
    executed: list[str] = []

    class FakeConnection:
        def execute(self, query: str) -> None:
            executed.append(query)

    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda **_: FakeConnection())

    duckdb_transform._connect()

    assert executed == ["SET enable_progress_bar = false"]


def test_mask_select_list_basic():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    selection = DefaultMaskingAdapter().apply(
        schema,
        ["id", "name"],
        {"name": MaskRule(type="redact", value="***")},
    )
    assert "name" in selection.masked_columns
    assert "'***'" in selection.select_list[1]


def test_duckdb_transform_quotes_projection_identifiers_with_special_characters():
    schema = pa.schema(
        [
            pa.field("id + 1", pa.int64()),
            pa.field('bad"name', pa.int64()),
            pa.field("x; SELECT 1", pa.int64()),
        ]
    )

    query = duckdb_transform._build_query(
        schema,
        ['["id + 1"]', '["bad\\"name"]', '["x; SELECT 1"]'],
        None,
        {},
        DefaultMaskingAdapter(),
    )

    assert query == (
        'SELECT "id + 1" AS "id + 1", '
        '"bad""name" AS "bad""name", '
        '"x; SELECT 1" AS "x; SELECT 1" FROM input'
    )


def test_duckdb_transform_executes_projection_for_quoted_identifier():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    input_batch = pa.record_batch(
        [pa.array([7, 8], type=pa.int64())],
        names=["x; SELECT 1"],
    )

    result_batches = list(
        adapter.apply_filters_and_masks_stream(
            [input_batch],
            ['["x; SELECT 1"]'],
            None,
            {},
        )
    )

    result = pa.Table.from_batches(result_batches)
    assert result.schema.names == ["x; SELECT 1"]
    assert result.column("x; SELECT 1").to_pylist() == [7, 8]


def test_duckdb_transform_quotes_masked_column_identifiers():
    schema = pa.schema([pa.field('bad"name', pa.string())])

    selection = DefaultMaskingAdapter().apply(
        schema,
        ['["bad\\"name"]'],
        {'["bad\\"name"]': MaskRule(type="hash")},
    )

    assert selection.select_list == ['sha256(CAST("bad""name" AS VARCHAR)) AS "bad""name"']


def test_nested_mask_expression():
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
    selection = DefaultMaskingAdapter().apply(
        schema,
        ["user.address.zip"],
        {"user.address.zip": MaskRule(type="hash")},
    )
    assert selection.select_list[0].endswith(' AS "user"')
    assert 'struct_pack("address"' in selection.select_list[0]
    assert 'struct_pack("zip" := sha256' in selection.select_list[0]


def test_nested_projection_mask_bookkeeping_respects_top_level_field():
    address_type = pa.struct([pa.field("zip", pa.int64())])
    schema = pa.schema(
        [
            pa.field("user", pa.struct([pa.field("address", address_type)])),
            pa.field("account", pa.struct([pa.field("address", address_type)])),
        ]
    )

    selection = DefaultMaskingAdapter().apply(
        schema,
        ["user.address.zip"],
        {"account.address.zip": MaskRule(type="hash")},
    )

    assert selection.masked_columns == []


def test_nested_struct_selection_applies_descendant_masks():
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
    selection = DefaultMaskingAdapter().apply(
        schema,
        ["user.address"],
        {"user.address.zip": MaskRule(type="hash")},
    )
    assert 'struct_update(("user")."address", "zip"' in selection.select_list[0]


def test_nested_child_projection_cannot_bypass_null_mask_on_parent():
    profile_type = pa.struct([pa.field("ssn", pa.string()), pa.field("name", pa.string())])
    input_batch = pa.record_batch(
        [pa.array([{"ssn": "123-45-6789", "name": "Ada"}], type=profile_type)],
        names=["profile"],
    )

    result_batches = list(
        DuckDBRowTransformAdapter(DefaultMaskingAdapter()).apply_filters_and_masks_stream(
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

    masked_schema = DefaultMaskingAdapter().masked_schema(
        schema,
        ["user"],
        {"user.address.zip": MaskRule(type="hash")},
    )

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

    masked_schema = DefaultMaskingAdapter().masked_schema(
        schema,
        ["user.address.zip"],
        {"user.address.zip": MaskRule(type="hash")},
    )

    assert masked_schema.names == ["user"]
    address_field = masked_schema.field("user").type.field("address")
    assert address_field.type.names == ["zip"]
    assert address_field.type.field("zip").type == pa.string()


def test_default_mask_renders_literal():
    schema = pa.schema([pa.field("status", pa.string())])
    selection = DefaultMaskingAdapter().apply(
        schema,
        ["status"],
        {"status": MaskRule(type="default", value="unknown")},
    )
    assert "'unknown'" in selection.select_list[0]


def test_default_null_mask_renders_null_and_null_schema():
    schema = pa.schema([pa.field("status", pa.string())])
    adapter = DefaultMaskingAdapter()
    selection = adapter.apply(
        schema,
        ["status"],
        {"status": MaskRule(type="default", value=None)},
    )
    masked_schema = adapter.masked_schema(
        schema,
        ["status"],
        {"status": MaskRule(type="default", value=None)},
    )

    assert selection.select_list[0] == 'cast_to_type(NULL, "status") AS "status"'
    assert masked_schema.field("status").type == pa.string()
    assert masked_schema.field("status").nullable


def test_email_mask_renders_masking_expression():
    schema = pa.schema([pa.field("email", pa.string())])
    selection = DefaultMaskingAdapter().apply(
        schema,
        ["email"],
        {"email": MaskRule(type="email")},
    )

    assert "regexp_replace" in selection.select_list[0]
    assert 'AS "email"' in selection.select_list[0]


def test_redact_mask_preserves_null_and_honors_an_empty_replacement():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
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
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
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


def test_keep_last_mask_renders_masking_expression():
    schema = pa.schema([pa.field("account_id", pa.string())])
    selection = DefaultMaskingAdapter().apply(
        schema,
        ["account_id"],
        {"account_id": MaskRule(type="keep_last", value=4)},
    )

    assert 'right(CAST("account_id" AS VARCHAR), 4)' in selection.select_list[0]
    assert "repeat('*'" in selection.select_list[0]


def test_masked_schema_updates_list_of_struct_nested_field_types():
    schema = pa.schema(
        [
            pa.field(
                "metadata",
                pa.struct(
                    [
                        pa.field(
                            "preferences",
                            pa.list_(
                                pa.struct(
                                    [
                                        pa.field("name", pa.string()),
                                        pa.field("theme", pa.string()),
                                    ]
                                )
                            ),
                        )
                    ]
                ),
            )
        ]
    )

    masked_schema = DefaultMaskingAdapter().masked_schema(
        schema,
        ["metadata"],
        {"metadata.preferences.$element.theme": MaskRule(type="redact", value="[hidden]")},
    )

    preferences_field = masked_schema.field("metadata").type.field("preferences")
    theme_field = preferences_field.type.value_field.type.field("theme")
    assert theme_field.type == pa.string()


def test_list_of_struct_selection_applies_descendant_masks():
    schema = pa.schema(
        [
            pa.field(
                "metadata",
                pa.struct(
                    [
                        pa.field(
                            "preferences",
                            pa.list_(
                                pa.struct(
                                    [
                                        pa.field("name", pa.string()),
                                        pa.field("theme", pa.string()),
                                    ]
                                )
                            ),
                        )
                    ]
                ),
            )
        ]
    )
    selection = DefaultMaskingAdapter().apply(
        schema,
        ["metadata"],
        {"metadata.preferences.$element.theme": MaskRule(type="redact", value="[hidden]")},
    )

    assert "list_transform" in selection.select_list[0]
    assert 'struct_update(_item_2, "theme"' in selection.select_list[0]


def test_filters_and_masks_preserve_results_across_batch_boundaries():
    schema = pa.schema([pa.field("id", pa.int64())])
    batches = [
        pa.record_batch([pa.array(values)], schema=schema) for values in ([0, 1], [2, 3], [4, 5])
    ]
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    output = pa.Table.from_batches(
        list(
            adapter.apply_filters_and_masks_stream(
                batches,
                ["id"],
                parse_row_filter("id > 1", schema),
                {"id": MaskRule(type="redact", value="hidden")},
            )
        )
    )
    assert output.column("id").to_pylist() == ["hidden"] * 4


def test_duckdb_transform_disables_external_access(monkeypatch):
    result_batch = pa.record_batch([pa.array([1])], names=["id"])
    captured_configs: list[dict[str, object]] = []

    class FakeRelation:
        def query(self, table_name: str, query: str):
            del table_name, query
            return self

        def to_arrow_reader(self, batch_size: int):
            del batch_size
            return iter([result_batch])

    class FakeConnection:
        def execute(self, query: str) -> None:
            del query

        def from_arrow(self, reader: pa.RecordBatchReader):
            del reader
            return FakeRelation()

        def close(self) -> None:
            return None

    def fake_connect(**kwargs):
        captured_configs.append(kwargs)
        return FakeConnection()

    monkeypatch.setattr(duckdb_transform.duckdb, "connect", fake_connect)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())

    list(
        adapter.apply_filters_and_masks_stream(
            [pa.record_batch([pa.array([1], type=pa.int64())], names=["id"])],
            ["id"],
            None,
            {},
        )
    )

    assert captured_configs == [
        {
            "config": {
                "enable_external_access": "false",
                "autoload_known_extensions": "false",
                "autoinstall_known_extensions": "false",
                "threads": 1,
                "memory_limit": "512MB",
            }
        }
    ]


def test_duckdb_transform_closes_connection_when_consumer_stops_early(monkeypatch):
    result_batches = [
        pa.record_batch([pa.array([1, 2])], names=["id"]),
        pa.record_batch([pa.array([3, 4])], names=["id"]),
    ]

    class FakeRelation:
        def query(self, table_name: str, query: str):
            return self

        def to_arrow_reader(self, batch_size: int):
            return iter(result_batches)

    class FakeConnection:
        def __init__(self) -> None:
            self.closed = 0

        def execute(self, query: str) -> None:
            del query

        def from_arrow(self, reader: pa.RecordBatchReader):
            return FakeRelation()

        def close(self) -> None:
            self.closed += 1

    fake_connection = FakeConnection()
    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda **_kwargs: fake_connection)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    stream = adapter.apply_filters_and_masks_stream(
        [pa.record_batch([pa.array([1, 2, 3, 4])], names=["id"])],
        ["id"],
        None,
        {},
    )
    stream_iter = cast(Generator[pa.RecordBatch, None, None], stream)

    assert next(stream_iter).num_rows == 2
    assert fake_connection.closed == 0

    stream_iter.close()

    assert fake_connection.closed == 1


def test_duckdb_transform_returns_empty_iterator_without_connecting(monkeypatch):
    connect_calls = 0

    def fake_connect(**_kwargs):
        nonlocal connect_calls
        connect_calls += 1
        raise AssertionError("connect should not be called for empty input")

    monkeypatch.setattr(duckdb_transform.duckdb, "connect", fake_connect)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())

    assert list(adapter.apply_filters_and_masks_stream([], ["id"], None, {})) == []
    assert connect_calls == 0


def test_duckdb_transform_rejects_streams_beyond_configured_admission_limit():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter(), max_active_streams=1)
    batch = pa.record_batch([pa.array([1, 2, 3])], names=["id"])
    first = adapter.apply_filters_and_masks_stream([batch], ["id"], None, {})

    assert next(cast(Generator[pa.RecordBatch, None, None], first)).num_rows == 3
    second = adapter.apply_filters_and_masks_stream([batch], ["id"], None, {})
    with pytest.raises(StreamAdmissionError, match="capacity"):
        next(cast(Generator[pa.RecordBatch, None, None], second))

    cast(Generator[pa.RecordBatch, None, None], first).close()
    assert list(adapter.apply_filters_and_masks_stream([batch], ["id"], None, {}))


def test_stream_admission_precedes_source_reads_and_creation_is_lazy():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter(), max_active_streams=1)
    batch = pa.record_batch([pa.array([1])], names=["id"])
    consumed = []

    def source():
        consumed.append(True)
        yield batch

    first = iter(adapter.apply_filters_and_masks_stream(source(), ["id"], None, {}))
    assert consumed == []
    next(first)
    second = iter(adapter.apply_filters_and_masks_stream(source(), ["id"], None, {}))
    with pytest.raises(StreamAdmissionError):
        next(second)
    assert consumed == [True]
    cast(Generator[pa.RecordBatch, None, None], first).close()


def test_input_budget_counts_retained_slice_buffers():
    batch = pa.record_batch([pa.array(range(10000), type=pa.int64())], names=["id"]).slice(0, 1)
    assert batch.nbytes == 8
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter(), max_input_batch_bytes=100)
    with pytest.raises(InputBatchLimitError, match="limit"):
        list(adapter.apply_filters_and_masks_stream([batch], ["id"], None, {}))


def test_input_failure_closes_source_and_releases_admission():
    closed = []

    def source():
        try:
            yield pa.record_batch([pa.array(["oversized"])], names=["id"])
        finally:
            closed.append(True)

    adapter = DuckDBRowTransformAdapter(
        DefaultMaskingAdapter(), max_active_streams=1, max_input_batch_bytes=8
    )
    with pytest.raises(InputBatchLimitError):
        list(adapter.apply_filters_and_masks_stream(source(), ["id"], None, {}))
    assert closed == [True]
    assert list(
        adapter.apply_filters_and_masks_stream(
            [pa.record_batch([pa.array([1])], names=["id"])], ["id"], None, {}
        )
    )


@pytest.mark.parametrize("limit", ["-1", "0B", "unlimited", "80%", "nanGB", "garbage"])
def test_duckdb_memory_limit_must_be_a_positive_finite_size(limit):
    with pytest.raises(ValueError, match="memory_limit"):
        DuckDBRowTransformAdapter(DefaultMaskingAdapter(), duckdb_memory_limit=limit)


def test_real_duckdb_oom_releases_slot_and_does_not_expose_query():
    adapter = DuckDBRowTransformAdapter(
        DefaultMaskingAdapter(), max_active_streams=1, duckdb_memory_limit="1KB"
    )
    batch = pa.record_batch([pa.array(range(10000))], names=["id"])
    for _ in range(2):
        with pytest.raises(StreamMemoryLimitError, match="memory budget exhausted") as error:
            list(
                adapter.apply_filters_and_masks_stream(
                    [batch], ["id"], None, {"id": MaskRule(type="hash")}
                )
            )
        assert "SELECT" not in str(error.value)


def test_later_input_limit_failure_keeps_its_type_and_closes_source():
    closed = []

    def source():
        try:
            yield pa.record_batch([pa.array([1])], names=["id"])
            yield pa.record_batch([pa.array(range(100))], names=["id"])
        finally:
            closed.append(True)

    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter(), max_input_batch_bytes=8)
    with pytest.raises(InputBatchLimitError):
        list(adapter.apply_filters_and_masks_stream(source(), ["id"], None, {}))
    assert closed == [True]


@pytest.mark.parametrize("limit", [0, -1])
def test_duckdb_transform_rejects_non_positive_admission_limits(limit):
    with pytest.raises(ValueError, match="max_active_streams"):
        DuckDBRowTransformAdapter(DefaultMaskingAdapter(), max_active_streams=limit)


def test_duckdb_transform_rejects_an_oversized_input_batch_before_query_execution():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter(), max_input_batch_bytes=1)
    batch = pa.record_batch([pa.array([1])], names=["id"])

    with pytest.raises(InputBatchLimitError, match="limit"):
        list(adapter.apply_filters_and_masks_stream([batch], ["id"], None, {}))


def test_duckdb_transform_streams_chunked_output(monkeypatch):
    monkeypatch.setattr(duckdb_transform, "_DUCKDB_ARROW_OUTPUT_BATCH_SIZE", 2)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    input_batch = pa.record_batch([pa.array(list(range(6)))], names=["id"])

    result_batches = list(adapter.apply_filters_and_masks_stream([input_batch], ["id"], None, {}))

    assert [batch.num_rows for batch in result_batches] == [2, 2, 2]
    assert sum(batch.num_rows for batch in result_batches) == 6


def test_duckdb_transform_filters_on_hidden_execution_column():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    input_batch = pa.record_batch(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(["us", "eu", "us"], type=pa.string()),
        ],
        names=["id", "region"],
    )

    result_batches = list(
        adapter.apply_filters_and_masks_stream(
            [input_batch],
            ["id"],
            parse_row_filter("region = 'us'", input_batch.schema),
            {},
        )
    )
    result = pa.Table.from_batches(result_batches)

    assert result.schema.names == ["id"]
    assert result.column("id").to_pylist() == [1, 3]


def test_duckdb_transform_filters_original_masked_values_before_output_mask():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    input_batch = pa.record_batch(
        [pa.array(["alice@example.com", "bob@example.com"], type=pa.string())],
        names=["email"],
    )
    result_batches = list(
        adapter.apply_filters_and_masks_stream(
            [input_batch],
            ["email"],
            parse_row_filter("email = 'alice@example.com'", input_batch.schema),
            {"email": MaskRule(type="redact", value="[hidden]")},
        )
    )
    result = pa.Table.from_batches(result_batches)
    assert result.column("email").to_pylist() == ["[hidden]"]


def test_duckdb_transform_uses_canonical_sql_for_function_filters(monkeypatch):
    result_batch = pa.record_batch([pa.array([10])], names=["id"])

    class FakeRelation:
        def __init__(self) -> None:
            self.query_calls: list[tuple[str, str]] = []

        def query(self, table_name: str, query: str):
            self.query_calls.append((table_name, query))
            return self

        def to_arrow_reader(self, batch_size: int):
            del batch_size
            return iter([result_batch])

    class FakeConnection:
        def __init__(self) -> None:
            self.relation = FakeRelation()

        def execute(self, query: str) -> None:
            del query

        def from_arrow(self, reader: pa.RecordBatchReader):
            del reader
            return self.relation

        def close(self) -> None:
            return None

    fake_connection = FakeConnection()
    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda **_kwargs: fake_connection)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    input_batch = pa.record_batch(
        [pa.array([1], type=pa.int64()), pa.array(["us"], type=pa.string())],
        names=["id", "region"],
    )

    list(
        adapter.apply_filters_and_masks_stream(
            [input_batch],
            ["id"],
            parse_row_filter("lower(region) = 'us'", input_batch.schema),
            {},
        )
    )

    assert fake_connection.relation.query_calls == [
        ("input", 'SELECT "id" AS "id" FROM input WHERE LOWER(region) = \'us\'')
    ]


def test_duckdb_transform_applies_list_of_struct_mask():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
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


@pytest.mark.heavy
@pytest.mark.parametrize("payload_bytes", [0, 256])
def test_duckdb_transform_memory_is_bounded_in_subprocess(payload_bytes):
    script = textwrap.dedent(
        """
        import json
        import sys

        from tests.support.memory_probe import begin_memory_probe
        import pyarrow as pa

        from dal_obscura.common.access_control.filters import parse_row_filter
        from dal_obscura.data_plane.infrastructure.adapters.duckdb_transform import (
            DefaultMaskingAdapter,
            DuckDBRowTransformAdapter,
        )

        total_batches = int(sys.argv[1])
        rows_per_batch = int(sys.argv[2])
        payload_bytes = int(sys.argv[3])
        adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())

        def source():
            for batch_index in range(total_batches):
                start = batch_index * rows_per_batch
                yield pa.record_batch(
                    [
                        pa.array(range(start, start + rows_per_batch), type=pa.int64()),
                        pa.array(range(rows_per_batch), type=pa.int64()),
                        pa.array(["x" * payload_bytes] * rows_per_batch),
                    ],
                    names=["id", "value", "payload"],
                )

        begin_memory_probe()
        row_count = 0
        for batch in adapter.apply_filters_and_masks_stream(
            source(),
            ["id", "value", "payload"],
            parse_row_filter(
                "id >= 0",
                pa.schema(
                    [
                        pa.field("id", pa.int64()),
                        pa.field("value", pa.int64()),
                    ]
                ),
            ),
            {},
        ):
            row_count += batch.num_rows

        print(
            json.dumps(
                {
                    "rows": row_count,
                }
            )
        )
        """
    )

    def run_probe(total_batches: int, rows_per_batch: int) -> dict[str, int]:
        return run_memory_probe(
            script, [str(total_batches), str(rows_per_batch), str(payload_bytes)]
        )

    rows_per_batch = 50_000
    medium = run_probe(total_batches=32, rows_per_batch=rows_per_batch)
    large = run_probe(total_batches=256, rows_per_batch=rows_per_batch)
    medium_delta = medium["peak_rss"] - medium["baseline_rss"]
    large_delta = large["peak_rss"] - large["baseline_rss"]

    assert medium["rows"] == 32 * rows_per_batch
    assert large["rows"] == 256 * rows_per_batch
    assert medium["rss_samples"] > 2 and large["rss_samples"] > 2
    assert large_delta < medium_delta + 64 * 1024 * 1024
    assert large_delta < 256 * 1024 * 1024


def test_duckdb_transform_preserves_literal_dotted_top_level_field_name():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
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
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
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


def test_masked_schema_prunes_and_masks_map_value_struct_leaves():
    value_type = pa.struct([pa.field("name", pa.string()), pa.field("ssn", pa.string())])
    schema = pa.schema([pa.field("contacts", pa.map_(pa.string(), value_type))])

    output = DefaultMaskingAdapter().masked_schema(
        schema,
        ["contacts.$key", "contacts.$value.name"],
        {"contacts.$value.name": MaskRule(type="redact", value="[hidden]")},
    )

    item_type = output.field("contacts").type.item_field.type
    assert item_type.names == ["name"]
    assert item_type.field("name").type == pa.string()


def test_duckdb_transform_projects_and_masks_canonical_list_element_leaves():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
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


def test_masked_schema_prunes_canonical_list_element_struct_leaves():
    value_type = pa.struct([pa.field("email", pa.string()), pa.field("ssn", pa.string())])
    schema = pa.schema([pa.field("contacts", pa.list_(value_type))])

    output = DefaultMaskingAdapter().masked_schema(
        schema,
        ["contacts.$element.email"],
        {"contacts.$element.email": MaskRule(type="email")},
    )

    item_type = output.field("contacts").type.value_field.type
    assert item_type.names == ["email"]
    assert item_type.field("email").type == pa.string()


def test_output_batch_limit_rejects_oversized_result_batch():
    batch = pa.record_batch([pa.array(["payload"])], names=["value"])

    with pytest.raises(duckdb_transform.OutputBatchLimitError, match="output batch"):
        list(duckdb_transform._bounded_output_batches([batch], max_output_batch_bytes=1))


def test_null_map_key_hides_entire_map_and_preserves_projected_value_schema():
    value_type = pa.struct([pa.field("name", pa.string()), pa.field("secret", pa.string())])
    schema = pa.schema([pa.field("contacts", pa.map_(pa.string(), value_type), nullable=False)])
    batch = pa.RecordBatch.from_pylist(
        [{"contacts": [("private-key", {"name": "Ada", "secret": "secret"})]}], schema=schema
    )
    columns = ["contacts.$key", "contacts.$value.name"]
    masks = {"contacts.$key": MaskRule(type="null")}
    masking = DefaultMaskingAdapter()
    result = pa.Table.from_batches(
        list(
            DuckDBRowTransformAdapter(masking).apply_filters_and_masks_stream(
                [batch], columns, None, masks
            )
        )
    )
    assert result.to_pylist() == [{"contacts": None}]
    assert result.schema == masking.masked_schema(schema, columns, masks)
    assert result.schema.field("contacts").type.item_type.names == ["name"]


def test_null_default_masks_deep_list_siblings_without_hiding_selected_leaf():
    from dal_obscura.common.access_control.models import (
        AccessRule,
        DatasetPolicy,
        Policy,
        Principal,
    )
    from dal_obscura.common.access_control.policy_resolution import resolve_access
    from dal_obscura.data_plane.application.use_cases.plan_access import _expand_to_leaves

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
        _expand_to_leaves(schema, ["contacts"]),
    )
    result = pa.Table.from_batches(
        list(
            DuckDBRowTransformAdapter(DefaultMaskingAdapter()).apply_filters_and_masks_stream(
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
    adapter = DefaultMaskingAdapter()
    for operation in (adapter.apply, adapter.masked_schema):
        with pytest.raises(ValueError, match="does not contain a struct"):
            operation(schema, ["contacts"], {"contacts.email": MaskRule(type="hash")})


def test_first_output_does_not_consume_later_input_batches():
    consumed = []

    def source():
        for index in range(10):
            consumed.append(index)
            yield pa.record_batch([pa.array([index])], names=["id"])

    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    stream = cast(
        Generator[pa.RecordBatch, None, None],
        adapter.apply_filters_and_masks_stream(source(), ["id"], None, {}),
    )
    try:
        assert next(stream).column(0).to_pylist() == [0]
        assert consumed == [0]
    finally:
        stream.close()


def test_input_schema_changes_are_rejected():
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    batches = [
        pa.record_batch([pa.array([1])], names=["id"]),
        pa.record_batch([pa.array(["changed"])], names=["id"]),
    ]
    with pytest.raises(ValueError, match="schema changed"):
        list(adapter.apply_filters_and_masks_stream(batches, ["id"], None, {}))


def test_connection_setup_failure_closes_connection(monkeypatch):
    closed = []

    class Connection:
        def execute(self, query):
            raise RuntimeError("setup failed")

        def close(self):
            closed.append(True)

    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda **kwargs: Connection())
    with pytest.raises(RuntimeError, match="setup failed"):
        duckdb_transform._connect()
    assert closed == [True]


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
    adapter = DefaultMaskingAdapter()
    for operation in (adapter.apply, adapter.masked_schema):
        with pytest.raises(ValueError):
            operation(schema, ["id"], {"id": mask})
