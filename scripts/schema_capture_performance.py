"""Measure control schema/evaluation capture cost with a wide admitted schema.

Provider IO is stubbed to isolate HTTP/database/Python work. These measurements
are not native allocation, cloud throughput, or process RSS qualification.
"""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
import tracemalloc
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
from fastapi.testclient import TestClient
from sqlalchemy import event

from dal_obscura.interfaces.http.app import create_app
from dal_obscura.storage.database.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)


def measure(field_count: int, iterations: int) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="dal-schema-capture-") as temporary:
        engine = create_engine_from_url(f"sqlite+pysqlite:///{temporary}/control.db")
        try:
            migrate_config_store(engine)
            factory = session_factory(engine)
            headers = {"authorization": "Bearer performance-token"}
            with TestClient(create_app(factory, admin_token="performance-token")) as client:
                catalog = client.put(
                    "/v1/catalogs/analytics",
                    headers=headers,
                    json={
                        "plugin_id": "iceberg.catalog",
                        "options": {"type": "sql", "uri": "sqlite:///source.db"},
                    },
                )
                catalog.raise_for_status()
                asset = client.put(
                    "/v1/assets/analytics/users",
                    headers=headers,
                    json={"backend": "iceberg", "table_identifier": "default.users"},
                )
                asset.raise_for_status()
                asset_id = asset.json()["id"]
                fields = [
                    {
                        "name": "id" if index == 0 else f"field_{index}",
                        "type": "int64" if index == 0 else "string",
                        "field_id": str(index + 1),
                        "path": ["id" if index == 0 else f"field_{index}"],
                    }
                    for index in range(field_count)
                ]
                admitted = client.put(
                    f"/v1/assets/{asset_id}/schema-fields",
                    headers=headers,
                    json={"fields": fields, "expected_revision": 0},
                )
                if admitted.status_code != 200:
                    raise RuntimeError(admitted.json())
                client.put(
                    f"/v1/assets/{asset_id}/policy",
                    headers=headers,
                    json={
                        "expected_revision": 0,
                        "rules": [
                            {
                                "ordinal": 10,
                                "principals": ["reader"],
                                "columns": ["id"],
                                "masks": {},
                            }
                        ],
                    },
                ).raise_for_status()
                queries = []

                def record(connection, cursor, statement, parameters, context, many):
                    queries.append(statement.lower())

                event.listen(engine, "before_cursor_execute", record)
                results = {}
                try:
                    with patch(
                        "dal_obscura.control.schema_service.load_source_schema",
                        return_value=pa.schema([("id", pa.int64())]),
                    ):
                        for operation in ("schema", "evaluation"):

                            def request(operation=operation):
                                if operation == "schema":
                                    return client.get(
                                        f"/v1/assets/{asset_id}/schema", headers=headers
                                    )
                                return client.post(
                                    f"/v1/assets/{asset_id}/policy-evaluate",
                                    headers=headers,
                                    json={"principal": "reader", "rows": [{"id": 7}]},
                                )

                            for _ in range(2):
                                request().raise_for_status()
                            timings, peaks, counts = [], [], []
                            for _ in range(iterations):
                                queries.clear()
                                tracemalloc.start()
                                try:
                                    start = time.perf_counter()
                                    response = request()
                                    timings.append((time.perf_counter() - start) * 1000)
                                    peaks.append(tracemalloc.get_traced_memory()[1])
                                finally:
                                    tracemalloc.stop()
                                response.raise_for_status()
                                payload = response.json()
                                if operation == "schema":
                                    assert payload["fields"][0]["human_path"] == "id"
                                    assert payload["fields"][0]["type"] == "int64"
                                else:
                                    assert payload["rows"] == [{"id": 7}]
                                    assert payload["policy_revision"] == 1
                                counts.append(
                                    {
                                        "total": len(queries),
                                        "admitted_fields": sum(
                                            "asset_schema_fields" in query for query in queries
                                        ),
                                        "policy_rules": sum(
                                            "policy_rules" in query for query in queries
                                        ),
                                    }
                                )
                            results[operation] = {
                                "median_ms": statistics.median(timings),
                                "median_python_peak_bytes": statistics.median(peaks),
                                "query_counts": counts,
                            }
                finally:
                    event.remove(engine, "before_cursor_execute", record)
                return {
                    "admitted_fields": field_count,
                    "iterations": iterations,
                    "results": results,
                }
        finally:
            engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fields", type=int, default=4000)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    if not 1 <= arguments.fields <= 5000 or not 1 <= arguments.iterations <= 100:
        parser.error("Fields must be 1-5000 and iterations 1-100")
    report = json.dumps(measure(arguments.fields, arguments.iterations), indent=2, allow_nan=False)
    if arguments.output:
        arguments.output.write_text(report + "\n")
    print(report)


if __name__ == "__main__":
    main()
