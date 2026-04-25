import json
import subprocess
import sys
import textwrap
from collections.abc import Generator
from typing import cast

import pyarrow as pa

import dal_obscura.infrastructure.adapters.duckdb_transform as duckdb_transform
from dal_obscura.domain.access_control.filters import parse_row_filter
from dal_obscura.domain.access_control.models import MaskRule
from dal_obscura.infrastructure.adapters.duckdb_transform import (
    DefaultMaskingAdapter,
    DuckDBRowTransformAdapter,
)


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
        ["id + 1", 'bad"name', "x; SELECT 1"],
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
            ["x; SELECT 1"],
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
        ['bad"name'],
        {'bad"name': MaskRule(type="hash")},
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
    assert selection.select_list == [
        'sha256(CAST("user"."address"."zip" AS VARCHAR)) AS "user.address.zip"'
    ]


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
    assert 'struct_update("user"."address", "zip"' in selection.select_list[0]


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


def test_masked_schema_exposes_nested_projection_as_dotted_column():
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

    assert masked_schema.names == ["user.address.zip"]
    assert masked_schema.field("user.address.zip").type == pa.string()


def test_default_mask_renders_literal():
    schema = pa.schema([pa.field("status", pa.string())])
    selection = DefaultMaskingAdapter().apply(
        schema,
        ["status"],
        {"status": MaskRule(type="default", value="unknown")},
    )
    assert "'unknown'" in selection.select_list[0]


def test_email_mask_renders_masking_expression():
    schema = pa.schema([pa.field("email", pa.string())])
    selection = DefaultMaskingAdapter().apply(
        schema,
        ["email"],
        {"email": MaskRule(type="email")},
    )

    assert "regexp_replace" in selection.select_list[0]
    assert 'AS "email"' in selection.select_list[0]


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
        {"metadata.preferences.theme": MaskRule(type="redact", value="[hidden]")},
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
        {"metadata.preferences.theme": MaskRule(type="redact", value="[hidden]")},
    )

    assert "list_transform" in selection.select_list[0]
    assert 'struct_update(_item_2, "theme"' in selection.select_list[0]


def test_duckdb_transform_uses_single_arrow_stream_query(monkeypatch):
    result_batch = pa.record_batch([pa.array([10, 20])], names=["id"])

    class FakeRelation:
        def __init__(self) -> None:
            self.query_calls: list[tuple[str, str]] = []
            self.batch_size: int | None = None

        def query(self, table_name: str, query: str):
            self.query_calls.append((table_name, query))
            return self

        def to_arrow_reader(self, batch_size: int):
            self.batch_size = batch_size
            return iter([result_batch])

    class FakeConnection:
        def __init__(self) -> None:
            self.relation = FakeRelation()
            self.from_arrow_calls: list[pa.RecordBatchReader] = []
            self.register_calls = 0
            self.unregister_calls = 0
            self.closed = 0

        def from_arrow(self, reader: pa.RecordBatchReader):
            self.from_arrow_calls.append(reader)
            return self.relation

        def register(self, *args, **kwargs):
            self.register_calls += 1

        def unregister(self, *args, **kwargs):
            self.unregister_calls += 1

        def close(self) -> None:
            self.closed += 1

    fake_connection = FakeConnection()
    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda: fake_connection)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())
    input_batches = [
        pa.record_batch([pa.array([1, 2])], names=["id"]),
        pa.record_batch([pa.array([3, 4])], names=["id"]),
    ]

    result = list(
        adapter.apply_filters_and_masks_stream(
            input_batches,
            ["id"],
            parse_row_filter("id > 1", pa.schema([pa.field("id", pa.int64())])),
            {},
        )
    )

    assert len(result) == 1
    assert result[0].equals(result_batch)
    assert len(fake_connection.from_arrow_calls) == 1
    assert fake_connection.relation.query_calls == [
        ("input", 'SELECT "id" AS "id" FROM input WHERE id > 1')
    ]
    assert fake_connection.relation.batch_size == duckdb_transform._DUCKDB_ARROW_OUTPUT_BATCH_SIZE
    assert fake_connection.register_calls == 0
    assert fake_connection.unregister_calls == 0
    assert fake_connection.closed == 1


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

        def from_arrow(self, reader: pa.RecordBatchReader):
            return FakeRelation()

        def close(self) -> None:
            self.closed += 1

    fake_connection = FakeConnection()
    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda: fake_connection)
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

    def fake_connect():
        nonlocal connect_calls
        connect_calls += 1
        raise AssertionError("connect should not be called for empty input")

    monkeypatch.setattr(duckdb_transform.duckdb, "connect", fake_connect)
    adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())

    assert list(adapter.apply_filters_and_masks_stream([], ["id"], None, {})) == []
    assert connect_calls == 0


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

        def from_arrow(self, reader: pa.RecordBatchReader):
            del reader
            return self.relation

        def close(self) -> None:
            return None

    fake_connection = FakeConnection()
    monkeypatch.setattr(duckdb_transform.duckdb, "connect", lambda: fake_connection)
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
            {"metadata.preferences.theme": MaskRule(type="redact", value="[hidden]")},
        )
    )
    result = pa.Table.from_batches(result_batches)

    preferences = result.column("metadata").to_pylist()[0]["preferences"]
    assert [item["theme"] for item in preferences] == ["[hidden]", "[hidden]"]


def test_duckdb_transform_memory_is_bounded_in_subprocess():
    script = textwrap.dedent(
        """
        import json
        import sys

        import psutil
        import pyarrow as pa

        from dal_obscura.domain.access_control.filters import parse_row_filter
        from dal_obscura.infrastructure.adapters.duckdb_transform import (
            DefaultMaskingAdapter,
            DuckDBRowTransformAdapter,
        )

        total_batches = int(sys.argv[1])
        rows_per_batch = int(sys.argv[2])
        process = psutil.Process()
        adapter = DuckDBRowTransformAdapter(DefaultMaskingAdapter())

        def source():
            for batch_index in range(total_batches):
                start = batch_index * rows_per_batch
                yield pa.record_batch(
                    [
                        pa.array(range(start, start + rows_per_batch), type=pa.int64()),
                        pa.array(range(rows_per_batch), type=pa.int64()),
                    ],
                    names=["id", "value"],
                )

        baseline_rss = process.memory_info().rss
        peak_rss = baseline_rss
        row_count = 0
        for batch in adapter.apply_filters_and_masks_stream(
            source(),
            ["id", "value"],
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
            peak_rss = max(peak_rss, process.memory_info().rss)

        print(
            json.dumps(
                {
                    "baseline_rss": baseline_rss,
                    "peak_rss": peak_rss,
                    "rows": row_count,
                }
            )
        )
        """
    )

    def run_probe(total_batches: int, rows_per_batch: int) -> dict[str, int]:
        completed = subprocess.run(
            [sys.executable, "-c", script, str(total_batches), str(rows_per_batch)],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)

    rows_per_batch = 50_000
    medium = run_probe(total_batches=32, rows_per_batch=rows_per_batch)
    large = run_probe(total_batches=256, rows_per_batch=rows_per_batch)
    medium_delta = medium["peak_rss"] - medium["baseline_rss"]
    large_delta = large["peak_rss"] - large["baseline_rss"]

    assert medium["rows"] == 32 * rows_per_batch
    assert large["rows"] == 256 * rows_per_batch
    assert large_delta >= medium_delta
    assert large_delta < medium_delta * 6
    assert large_delta < 256 * 1024 * 1024
