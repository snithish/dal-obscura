import json
from dataclasses import replace
from types import MappingProxyType

import pyarrow as pa
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table import FileScanTask
from pyiceberg.typedef import Record

from dal_obscura.sources.builtins import (
    create_builtin_plugin_registry,
)
from dal_obscura.sources.iceberg import (
    IcebergInputPartition,
    IcebergTableFormat,
)
from dal_obscura.sources.iceberg_tasks import encode_scan_task
from dal_obscura.sources.paths import PathRuleEnforcer
from dal_obscura.sources.planning import ScanTask
from dal_obscura.sources.task_codec import SourceTaskCodec
from tests.support.use_cases import public_native_scan


def envelope():
    file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path="/warehouse/a.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=10,
        file_size_in_bytes=100,
    )
    file.spec_id = 0
    table = IcebergTableFormat(
        catalog_name="warehouse",
        table_name="default.users",
        metadata_location="/warehouse/metadata.json",
        io_options={},
        path_enforcer=PathRuleEnforcer([{"root": "/warehouse"}]),
    )
    schema = pa.schema([("id", pa.int64())])
    partition = IcebergInputPartition(columns=["id"], tasks=[encode_scan_task(FileScanTask(file))])
    return SourceTaskCodec(create_builtin_plugin_registry()), public_native_scan(
        ScanTask(table, schema, partition)
    )


def test_scan_envelope_preserves_schema_native_tasks_and_storage_bounds():
    codec, task = envelope()
    encoded = codec.encode(task)
    restored = codec.decode(encoded)
    assert restored.schema.equals(task.schema, check_metadata=True)
    assert restored.partition.task["tasks"] == task.partition.task["tasks"]
    assert restored.table_format.path_roots == ("/warehouse",)
    assert "format_factory" not in encoded


def test_scan_envelope_accepts_immutable_passive_task_metadata():
    codec, task = envelope()
    task = replace(
        task, partition=replace(task.partition, task=MappingProxyType(task.partition.task))
    )
    assert codec.decode(codec.encode(task)).partition.task == dict(task.partition.task)


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


def test_scan_envelope_can_cover_a_large_splittable_table():
    codec, task = envelope()
    task = replace(
        task,
        partition=replace(
            task.partition,
            task={**task.partition.task, "tasks": task.partition.task["tasks"] * 512},
        ),
    )
    assert len(codec.decode(codec.encode(task)).partition.task["tasks"]) == 512


@pytest.mark.parametrize(
    "change",
    [{"version": True}, {"version": 99}, {"plugin": ["different"]}, {"factory": "os.system"}],
)
def test_scan_envelope_rejects_unknown_versions_artifacts_and_executable_fields(change):
    codec, task = envelope()
    raw = {**json.loads(codec.encode(task)), **change}
    with pytest.raises(ValueError, match="Invalid read payload"):
        codec.decode(json.dumps(raw))
