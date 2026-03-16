import pickle

import pyarrow as pa

from dal_obscura.domain.query_planning.models import (
    BackendReference,
    DatasetSelector,
    ResolvedBackendTarget,
)
from dal_obscura.infrastructure.adapters.file_backend import DuckDBFileBackend, FileReadSpec


def test_file_backend_uses_bounded_schema_inference_queries(monkeypatch):
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
        "dal_obscura.infrastructure.adapters.file_backend.duckdb.connect",
        lambda: FakeConnection(),
    )
    backend = DuckDBFileBackend()

    backend.get_schema(
        ResolvedBackendTarget(
            dataset_identity=DatasetSelector(target="users"),
            backend=BackendReference(backend_id="duckdb_file", generation=1),
            handle={
                "format": "csv",
                "paths": ["users.csv"],
                "options": {"sample_rows": 321, "sample_files": 5},
            },
        )
    )
    backend.get_schema(
        ResolvedBackendTarget(
            dataset_identity=DatasetSelector(target="events"),
            backend=BackendReference(backend_id="duckdb_file", generation=1),
            handle={
                "format": "json",
                "paths": ["events.ndjson"],
                "options": {"sample_rows": 222, "sample_files": 4},
            },
        )
    )
    backend.get_schema(
        ResolvedBackendTarget(
            dataset_identity=DatasetSelector(target="facts"),
            backend=BackendReference(backend_id="duckdb_file", generation=1),
            handle={"format": "parquet", "paths": ["facts.parquet"], "options": {}},
        )
    )

    assert (
        "read_csv_auto(['users.csv'], union_by_name=true, sample_size=321, files_to_sniff=5)"
        in queries[0]
    )
    assert (
        "read_json_auto(['events.ndjson'], union_by_name=true, sample_size=222, maximum_sample_files=4)"
        in queries[1]
    )
    assert "read_parquet(['facts.parquet'], union_by_name=true)" in queries[2]


def test_file_backend_plan_stores_explicit_paths(monkeypatch):
    backend = DuckDBFileBackend()
    target = ResolvedBackendTarget(
        dataset_identity=DatasetSelector(catalog="local_files", target="facts"),
        backend=BackendReference(backend_id="duckdb_file", generation=1),
        handle={"format": "parquet", "paths": ["a.parquet", "b.parquet"], "options": {}},
    )
    monkeypatch.setattr(
        backend, "get_schema", lambda target: pa.schema([pa.field("id", pa.int64())])
    )

    plan = backend.plan(target, ["id"], max_tickets=4)
    specs = [pickle.loads(task.payload) for task in plan.tasks]

    assert all(isinstance(spec, FileReadSpec) for spec in specs)
    assert {path for spec in specs for path in spec.paths} == {"a.parquet", "b.parquet"}
    assert all(spec.dataset == target.dataset_identity for spec in specs)


def test_file_backend_reads_array_json_documents(tmp_path):
    json_path = tmp_path / "users.json"
    json_path.write_text('[{"id":1,"name":"a"},{"id":2,"name":"b"}]')
    backend = DuckDBFileBackend()
    target = ResolvedBackendTarget(
        dataset_identity=DatasetSelector(target=str(json_path)),
        backend=BackendReference(backend_id="duckdb_file", generation=1),
        handle={
            "format": "json",
            "paths": [str(json_path)],
            "options": {"sample_rows": 10, "sample_files": 1},
        },
    )

    schema = backend.get_schema(target)
    plan = backend.plan(target, ["id", "name"], max_tickets=1)
    batches = list(backend.read_stream(plan.tasks[0].payload))
    table = pa.Table.from_batches(batches)

    assert schema.names == ["id", "name"]
    assert table.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
