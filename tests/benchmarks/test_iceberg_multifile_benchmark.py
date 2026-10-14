"""Real native file fan-out; setup stays outside measured scans."""

from datetime import UTC, datetime, timedelta

import pytest
from dal_obscura_iceberg.format import IcebergFormatPlugin
from dal_obscura_plugin_api import ExecutionContext, ScanRequest, TableHandle, TableIdentifier
from dal_obscura_plugin_conformance.iceberg_fixtures import (
    create_iceberg_table,
    iceberg_sql_catalog_options,
)
from dal_obscura_plugin_conformance.testing import parallel_rows

pytestmark = pytest.mark.heavy


@pytest.mark.benchmark(group="iceberg-multifile")
def test_benchmark_iceberg_multifile_scan(catalog_factory, benchmark, tmp_path):
    identifier = create_iceberg_table(
        tmp_path,
        "bench",
        "warehouse",
        append_batches=[list(range(start, start + 1000)) for start in range(0, 16000, 1000)],
    )
    table = catalog_factory(
        "bench",
        **{
            key: str(value)
            for key, value in iceberg_sql_catalog_options(tmp_path, "bench", "warehouse").items()
        },
    ).load_table(identifier)
    handle = TableHandle(
        "iceberg.catalog",
        "bench",
        1,
        TableIdentifier(("default",), "users"),
        "iceberg",
        1,
        str(table.metadata.current_snapshot_id),
        {"metadata_location": table.metadata_location},
    )
    context = ExecutionContext(datetime.now(UTC) + timedelta(minutes=10), "benchmark")
    plugin = IcebergFormatPlugin(handle, context)
    schema = plugin.schema(context).arrow_schema
    tasks = plugin.plan(ScanRequest(schema, 4), context)
    plugin.close()
    rows = benchmark(lambda: parallel_rows(IcebergFormatPlugin, handle, tasks, context))
    assert sorted(row["id"] for row in rows) == list(range(16000))
    benchmark.extra_info.update(
        scenario="native-iceberg-file-fanout", planned_files=16, tasks=4, output_rows=len(rows)
    )
