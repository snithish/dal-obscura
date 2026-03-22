import pyarrow as pa

from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.infrastructure.adapters.duckdb_handler import (
    DuckDBHandler,
    FileInputPartition,
    FileTable,
)


def _file_table(format: str, paths: tuple[str, ...], options: dict) -> FileTable:
    return FileTable(
        catalog_name="local_files",
        table_name="target",
        format="duckdb_file",
        file_format=format,
        paths=paths,
        options=options,
    )


def test_file_handler_uses_bounded_schema_inference_queries(monkeypatch):
    queries: list[str] = []
    schema = pa.schema([pa.field("id", pa.int64())])

    class FakeRelation:
        def __init__(self, query: str) -> None:
            self._query = query

        def limit(self, count: int):
            return self

        def to_arrow_table(self):
            return pa.Table.from_arrays([pa.array([], type=pa.int64())], schema=schema)

        def to_arrow_reader(self, batch_size: int):
            return iter([])

    class FakeConnection:
        def sql(self, query: str):
            queries.append(query)
            return FakeRelation(query)

        def close(self):
            return None

    monkeypatch.setattr(
        "dal_obscura.infrastructure.adapters.duckdb_handler.duckdb.connect",
        lambda: FakeConnection(),
    )
    handler = DuckDBHandler()

    handler.get_schema(_file_table("csv", ("users.csv",), {"sample_rows": 321, "sample_files": 5}))
    handler.get_schema(
        _file_table("json", ("events.ndjson",), {"sample_rows": 222, "sample_files": 4})
    )
    handler.get_schema(_file_table("parquet", ("facts.parquet",), {}))

    assert (
        "read_csv_auto(['users.csv'], union_by_name=true, sample_size=321, files_to_sniff=5)"
        in queries[0]
    )
    assert (
        "read_json_auto(['events.ndjson'], union_by_name=true, sample_size=222, "
        "maximum_sample_files=4)" in queries[1]
    )
    assert "read_parquet(['facts.parquet'], union_by_name=true)" in queries[2]


def test_file_handler_plan_stores_explicit_paths(monkeypatch):
    handler = DuckDBHandler()
    table = _file_table("parquet", ("a.parquet", "b.parquet"), {})

    monkeypatch.setattr(
        handler, "get_schema", lambda table: pa.schema([pa.field("id", pa.int64())])
    )

    request = PlanRequest(catalog="local_files", target="facts", columns=["id"])
    plan = handler.plan(table, request, max_tickets=4)
    specs = [task.partition for task in plan.tasks]

    assert all(isinstance(spec, FileInputPartition) for spec in specs)
    assert {
        path for spec in specs if isinstance(spec, FileInputPartition) for path in spec.paths
    } == {"a.parquet", "b.parquet"}


def test_file_handler_reads_array_json_documents(tmp_path):
    json_path = tmp_path / "users.json"
    json_path.write_text('[{"id":1,"name":"a"},{"id":2,"name":"b"}]')
    handler = DuckDBHandler()
    table = _file_table("json", (str(json_path),), {"sample_rows": 10, "sample_files": 1})

    schema = handler.get_schema(table)
    request = PlanRequest(catalog="local_files", target="users", columns=["id", "name"])
    plan = handler.plan(table, request, max_tickets=1)

    _, batches = handler.execute(plan.tasks[0].partition)
    arrow_table = pa.Table.from_batches(list(batches))

    assert schema.names == ["id", "name"]
    assert arrow_table.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
