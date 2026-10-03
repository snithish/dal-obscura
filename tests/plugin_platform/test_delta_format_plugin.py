"""Core routing and passive scan envelopes preserve actual Delta DV semantics."""

from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
from deltalake import DeltaTable

from dal_obscura.read.request import PlanRequest
from dal_obscura.sources.task_codec import SourceTaskCodec
from tests.support.consumer_backends import delta_registry, nested_consumer_table


def test_core_delta_tasks_restore_parallel_deletes_after_later_update(tmp_path):
    with delta_registry(tmp_path, nested_consumer_table()) as (registry, plugins):
        plan = registry.resolve("analytics", "default.users").plan(
            PlanRequest(target="default.users", columns=["id", "metadata"]), max_tickets=2
        )
        assert len(plan.tasks) == 2
        codec = SourceTaskCodec(plugins)
        payloads = [codec.encode(task) for task in plan.tasks]
        DeltaTable(tmp_path / "sales").update(updates={"id": "99"}, predicate="id = 2")

        def read(payload):
            task = codec.decode(payload)
            schema, batches = task.table_format.execute(task.partition)
            return pa.Table.from_batches(list(batches), schema=schema)

        with ThreadPoolExecutor(max_workers=2) as pool:
            tables = list(pool.map(read, payloads))
        actual = pa.concat_tables(tables).sort_by([("id", "ascending")])
    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "metadata",
                pa.struct(
                    [
                        pa.field(
                            "preferences",
                            pa.list_(
                                pa.field(
                                    "element",
                                    pa.struct(
                                        [
                                            pa.field("name", pa.string()),
                                            pa.field("theme", pa.string()),
                                        ]
                                    ),
                                )
                            ),
                        )
                    ]
                ),
            ),
        ]
    )
    assert actual.schema.equals(expected_schema, check_metadata=True)
    assert actual.to_pylist() == [
        {
            "id": 1,
            "metadata": {
                "preferences": [
                    {"name": "web", "theme": "dark"},
                    {"name": "mobile", "theme": "light"},
                ]
            },
        },
        {"id": 2, "metadata": {"preferences": [{"name": "desktop", "theme": "dark"}]}},
    ]
