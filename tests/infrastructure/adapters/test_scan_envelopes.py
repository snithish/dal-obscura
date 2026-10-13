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
        "iceberg.catalog",
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
    restored = codec.decode(codec.encode(task))
    assert restored.partition.task.to_json()["files"] == [
        f"/warehouse/{index}.parquet" for index in range(512)
    ]
    assert restored.schema.equals(pa.schema([("id", pa.int64())]), check_metadata=True)


@pytest.mark.parametrize(
    "change",
    [{"version": True}, {"version": 99}, {"plugin": ["different"]}, {"factory": "os.system"}],
)
def test_scan_envelope_rejects_unknown_versions_artifacts_and_executable_fields(change):
    codec, task = envelope()
    raw = {**json.loads(codec.encode(task)), **change}
    with pytest.raises(ValueError, match="Invalid read payload"):
        codec.decode(json.dumps(raw))


def test_scan_envelope_rejects_native_incompatible_schema_when_encoding():
    from tests.support.nested_schema import nested_struct_schema

    codec, task = envelope()
    deep = nested_struct_schema(63)
    task = replace(task, partition=replace(task.partition, schema=deep))
    with pytest.raises(ValueError, match="native interchange"):
        codec.encode(task)


def test_scan_envelope_rejects_native_incompatible_schema_when_decoding():
    import base64

    from tests.support.nested_schema import nested_struct_schema

    codec, task = envelope()
    raw = json.loads(codec.encode(task))
    raw["output_schema"] = base64.b64encode(nested_struct_schema(63).serialize()).decode("ascii")
    with pytest.raises(ValueError, match="Invalid read payload") as error:
        codec.decode(json.dumps(raw))
    assert "native interchange" in str(error.value.__cause__)


def test_scan_envelope_retains_deep_source_metadata_with_a_native_projection():
    from tests.support.nested_schema import nested_struct_schema

    codec, task = envelope()
    metadata_schema = nested_struct_schema(63)
    task = replace(task, schema=metadata_schema)
    restored = codec.decode(codec.encode(task))
    assert restored.schema.equals(metadata_schema, check_metadata=True)
    assert restored.partition.schema.equals(pa.schema([("id", pa.int64())]), check_metadata=True)
