"""Real native file fan-out; setup stays outside measured scans."""

from datetime import datetime, timedelta, timezone

import pytest
from dal_obscura_plugin_api import ExecutionContext, ScanRequest, TableHandle, TableIdentifier
from pyiceberg.catalog import load_catalog

from dal_obscura.sources.iceberg_plugin import IcebergFormatPlugin
from tests.support.iceberg import create_iceberg_table, iceberg_sql_catalog_options
from tests.support.plugin_scans import parallel_rows

pytestmark = pytest.mark.heavy


@pytest.mark.benchmark(group="iceberg-multifile")
def test_benchmark_iceberg_multifile_scan(benchmark, tmp_path):
    identifier = create_iceberg_table(
        tmp_path,
        "bench",
        "warehouse",
        append_batches=[list(range(start, start + 1000)) for start in range(0, 16000, 1000)],
    )
    table = load_catalog(
        "bench",
        **{
            key: str(value)
            for key, value in iceberg_sql_catalog_options(tmp_path, "bench", "warehouse").items()
        },
    ).load_table(identifier)
    handle = TableHandle(
        "iceberg.sql",
        "bench",
        1,
        TableIdentifier(("default",), "users"),
        "iceberg",
        1,
        str(table.metadata.current_snapshot_id),
        {"metadata_location": table.metadata_location},
    )
    context = ExecutionContext(datetime.now(timezone.utc) + timedelta(minutes=10), "benchmark")
    plugin = IcebergFormatPlugin(handle, context)
    schema = plugin.schema(context).arrow_schema
    tasks = plugin.plan(ScanRequest(schema, 4), context)
    plugin.close()
    rows = benchmark(lambda: parallel_rows(IcebergFormatPlugin, handle, tasks, context))
    assert sorted(row["id"] for row in rows) == list(range(16000))
    benchmark.extra_info.update(
        scenario="native-iceberg-file-fanout", planned_files=16, tasks=4, output_rows=len(rows)
    )
