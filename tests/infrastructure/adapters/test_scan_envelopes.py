import json
from dataclasses import replace

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import ScanTask as PluginScanTask
from dal_obscura_plugin_api import TableHandle, TableIdentifier

from dal_obscura.sources.builtins import (
    create_builtin_plugin_registry,
)
from dal_obscura.sources.iceberg_plugin import IcebergFormatPlugin
from dal_obscura.sources.planning import ScanTask
from dal_obscura.sources.plugin_runtime import PublicPluginPartition, PublicPluginTableFormat
from dal_obscura.sources.task_codec import SourceTaskCodec


def envelope():
    handle = TableHandle(
        "iceberg.sql",
        "warehouse",
        0,
        TableIdentifier(("default",), "users"),
        "iceberg",
        1,
        "123",
        {"metadata_location": "/warehouse/metadata.json"},
    )
    schema = pa.schema([("id", pa.int64())])
    source = PublicPluginTableFormat(
        catalog_name="warehouse",
        table_name="default.users",
        format="iceberg",
        handle=handle,
        format_factory=IcebergFormatPlugin,
        path_roots=("/warehouse",),
    )
    partition = PublicPluginPartition(
        task=PluginScanTask(
            {
                "columns": ["id"],
                "files": ["/warehouse/a.parquet"],
                "parallelism": 1,
                "row_filter": None,
            }
        ),
        schema=schema,
    )
    return SourceTaskCodec(create_builtin_plugin_registry()), ScanTask(source, schema, partition)


def test_scan_envelope_preserves_schema_native_tasks_and_storage_bounds():
    codec, task = envelope()
    encoded = codec.encode(task)
    restored = codec.decode(encoded)
    assert restored.schema.equals(task.schema, check_metadata=True)
    assert restored.partition.task.to_json() == task.partition.task.to_json()
    assert restored.table_format.path_roots == ("/warehouse",)
    assert "format_factory" not in encoded


def test_scan_envelope_rejects_oversized_schemas_before_arrow_decoding(monkeypatch):
    from dal_obscura.policy.schema_bounds import MAX_SCHEMA_ENCODING_BYTES

    codec, task = envelope()
    raw = json.loads(codec.encode(task))
    raw["schema"] = "A" * (4 * ((MAX_SCHEMA_ENCODING_BYTES + 2) // 3) + 4)

    def unexpected_decode(*args, **kwargs):
        pytest.fail("Oversized schema reached Arrow decoder")

    monkeypatch.setattr(pa.ipc, "read_schema", unexpected_decode)
    with pytest.raises(ValueError, match="Invalid read payload"):
        codec.decode(json.dumps(raw))


def test_scan_envelope_rejects_duplicate_json_fields():
    codec, task = envelope()
    encoded = codec.encode(task)
    with pytest.raises(ValueError, match="Invalid read payload"):
        codec.decode('{"version":99,' + encoded[1:])


def test_scan_envelope_rejects_excessive_json_nesting():
    import sys

    codec, _ = envelope()
    depth = max(10_000, sys.getrecursionlimit() * 2)
    with pytest.raises(ValueError, match="Invalid read payload"):
        codec.decode("[" * depth + "0" + "]" * depth)


def test_scan_envelope_can_cover_a_large_splittable_table():
    codec, task = envelope()
    task = replace(
        task,
        partition=replace(
            task.partition,
            task=PluginScanTask(
                {
                    **task.partition.task.to_json(),
                    "files": [f"/warehouse/{index}.parquet" for index in range(512)],
                }
            ),
        ),
    )
    assert len(codec.decode(codec.encode(task)).partition.task.to_json()["files"]) == 512


@pytest.mark.parametrize(
    "change",
    [{"version": True}, {"version": 99}, {"plugin": ["different"]}, {"factory": "os.system"}],
)
def test_scan_envelope_rejects_unknown_versions_artifacts_and_executable_fields(change):
    codec, task = envelope()
    raw = {**json.loads(codec.encode(task)), **change}
    with pytest.raises(ValueError, match="Invalid read payload"):
        codec.decode(json.dumps(raw))
