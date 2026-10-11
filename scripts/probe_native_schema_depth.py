"""Measure native nesting boundaries in isolated processes with exact values.

Run with the repository's uv environment. This diagnostic does not change service
admission limits or qualify table-format/cloud behavior.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def probe(depth: int) -> dict[str, object]:
    values = pa.array(["synthetic-value", None], type=pa.string())
    for index in range(depth):
        values = pa.StructArray.from_arrays(
            [values],
            fields=[
                pa.field(f"level_{index}", values.type, metadata={b"layer": str(index).encode()})
            ],
        )
    schema = pa.schema([pa.field("root", values.type)], metadata={b"binary": b"\xff"})
    batch = pa.RecordBatch.from_arrays([values], schema=schema)

    def schema_ipc() -> None:
        restored = pa.ipc.read_schema(schema.serialize())
        assert restored.equals(schema, check_metadata=True)

    def batch_ipc() -> None:
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(batch)
        with pa.ipc.open_stream(sink.getvalue()) as reader:
            restored = reader.read_next_batch()
            assert restored.equals(batch, check_metadata=True)

    def parquet() -> None:
        with tempfile.TemporaryDirectory(prefix="dal-nesting-probe-") as temporary:
            path = Path(temporary) / "fixture.parquet"
            pq.write_table(pa.Table.from_batches([batch]), path)
            restored = pq.read_table(path).combine_chunks().to_batches()[0]
            assert restored.equals(batch, check_metadata=True)

    def query(sql: str, expected: pa.Array) -> None:
        with duckdb.connect() as connection:
            connection.register("fixture", batch)
            table = connection.execute(sql).to_arrow_table()
            assert table.column(0).combine_chunks().equals(expected)

    leaf = '"root"' + "".join(f'."level_{index}"' for index in reversed(range(depth)))
    operations: dict[str, Callable[[], None]] = {
        "schema_ipc": schema_ipc,
        "batch_ipc": batch_ipc,
        "parquet": parquet,
        "duckdb_leaf": lambda: query(
            f"SELECT {leaf} FROM fixture", pa.array(["synthetic-value", None])
        ),
        "duckdb_struct": lambda: query('SELECT "root" FROM fixture', values),
    }
    stages = {}
    for name, operation in operations.items():
        try:
            operation()
            stages[name] = {"ok": True}
        except Exception as error:
            stages[name] = {"ok": False, "error": type(error).__name__, "message": str(error)[:300]}
    return {"struct_layers": depth, "stages": stages}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--depth", type=int, action="append", help="Struct layers, repeatable (1-512)"
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--child-depth", type=int, help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    depths = (
        [arguments.child_depth]
        if arguments.child_depth is not None
        else arguments.depth or [32, 62, 63, 64, 65, 96, 128, 256]
    )
    if any(not 1 <= depth <= 512 for depth in depths):
        parser.error("Probe depths must be between 1 and 512 struct layers")
    if arguments.child_depth is not None:
        print(json.dumps(probe(arguments.child_depth), allow_nan=False))
        return
    results = []
    for depth in depths:
        try:
            child = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--child-depth", str(depth)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if child.returncode:
                results.append(
                    {
                        "struct_layers": depth,
                        "process_error": child.returncode,
                        "stderr": child.stderr[-500:],
                    }
                )
            else:
                results.append(json.loads(child.stdout))
        except subprocess.TimeoutExpired:
            results.append({"struct_layers": depth, "process_error": "timeout"})
    report = {"pyarrow": pa.__version__, "duckdb": duckdb.__version__, "results": results}
    content = json.dumps(report, indent=2, allow_nan=False) + "\n"
    if arguments.output:
        arguments.output.write_text(content)
    else:
        print(content, end="")


if __name__ == "__main__":
    main()
