import json
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest
from dal_obscura_iceberg.format import IcebergFormatPlugin
from dal_obscura_plugin_api import ExecutionContext, TableHandle, TableIdentifier
from dal_obscura_plugin_api import ScanTask as PluginScanTask

from dal_obscura.adapters.providers.adapters import PublicPluginPartition, PublicPluginTableFormat
from dal_obscura.adapters.providers.task_codec import SourceTaskCodec
from dal_obscura.application.reads.sources import ScanTask
from dal_obscura.kernel.plugins import PluginLock
from dal_obscura.plugins.bootstrap import (
    create_plugin_registry,
)


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
    return SourceTaskCodec(create_plugin_registry()), ScanTask(source, schema, partition)


def test_scan_envelope_matches_architecture_baseline(monkeypatch):
    codec, task = envelope()
    baseline = (Path(__file__).parent / "fixtures" / "scan_envelope_v1.json").read_text().strip()
    # The wire fixture owns serialization; installed-artifact admission has its own tests.
    lock = PluginLock.from_json(json.loads(baseline)["plugin"])
    monkeypatch.setattr(codec.registry, "identity", lambda kind, plugin_id: lock)
    assert codec.encode(task) == baseline
    restored = codec.decode(baseline)
    assert restored.schema.equals(task.schema, check_metadata=True)
    assert restored.partition.schema.equals(task.partition.schema, check_metadata=True)
    assert restored.partition.task.to_json() == task.partition.task.to_json()
    assert restored.table_format.handle.to_json() == task.table_format.handle.to_json()
    assert restored.table_format.path_roots == task.table_format.path_roots


def test_scan_envelope_rejects_oversized_schemas_before_arrow_decoding(monkeypatch):
    from dal_obscura_plugin_api.schema.bounds import MAX_SCHEMA_ENCODING_BYTES

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
    from dal_obscura_plugin_conformance.schemas import nested_struct_schema

    codec, task = envelope()
    deep = nested_struct_schema(63)
    task = replace(task, partition=replace(task.partition, schema=deep))
    with pytest.raises(ValueError, match="native interchange"):
        codec.encode(task)


def test_scan_envelope_rejects_native_incompatible_schema_when_decoding():
    import base64

    from dal_obscura_plugin_conformance.schemas import nested_struct_schema

    codec, task = envelope()
    raw = json.loads(codec.encode(task))
    raw["output_schema"] = base64.b64encode(nested_struct_schema(63).serialize()).decode("ascii")
    with pytest.raises(ValueError, match="Invalid read payload") as error:
        codec.decode(json.dumps(raw))
    assert "native interchange" in str(error.value.__cause__)


def test_scan_envelope_retains_deep_source_metadata_with_a_native_projection():
    from dal_obscura_plugin_conformance.schemas import nested_struct_schema

    codec, task = envelope()
    metadata_schema = nested_struct_schema(63)
    task = replace(task, schema=metadata_schema)
    restored = codec.decode(codec.encode(task))
    assert restored.schema.equals(metadata_schema, check_metadata=True)
    assert restored.partition.schema.equals(pa.schema([("id", pa.int64())]), check_metadata=True)


@pytest.mark.parametrize("instance_options, batch_sizes", [({}, [1, 1]), ({"batch_rows": 2}, [2])])
def test_scan_envelope_captures_format_options_for_fetch(tmp_path, instance_options, batch_sizes):
    from datetime import UTC, datetime, timedelta

    import pyarrow.parquet as pq

    from dal_obscura.adapters.providers.published import LiveConfigCatalogRegistry
    from dal_obscura.kernel.plans.request import PlanRequest
    from dal_obscura.kernel.plans.snapshot import (
        PublishedAssetSnapshot,
        PublishedCatalog,
        PublishedSource,
    )
    from dal_obscura.kernel.policy.models import AssetPolicy
    from dal_obscura.plugins import PluginRegistry
    from tests.support.plugins.plugins import manifest_registry
    from tests.support.plugins.public_plugins import _fixture

    root, expected = _fixture(tmp_path)
    pq.write_table(expected, root / "part.parquet", row_group_size=2)
    installed = manifest_registry()
    locks = {key: installed.identity(*key) for key in installed.admitted()}
    planner = PluginRegistry(allowlist=locks, options={"dal.plugins.parquet.dataset.batch_rows": 1})
    planner.reload()
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog(
            "fixture", "manifest", 1, {"root": str(root), "manifest_path": "manifest.json"}
        ),
        target="default.users",
        source=PublishedSource("parquet.dataset", "default.users", instance_options),
        policy=AssetPolicy(1, "fixture", "default.users", []),
    )

    class SnapshotReader:
        def get_asset_and_catalog(self, catalog, target):
            return asset

    catalogs = LiveConfigCatalogRegistry(SnapshotReader(), plugin_registry=planner)
    try:
        with catalogs.open("fixture", "default.users") as access:
            plan = access.table_format.plan(
                PlanRequest(target="default.users", columns=["id"]), max_tickets=1
            )
            payload = SourceTaskCodec(planner).encode(plan.tasks[0])
    finally:
        catalogs.close()
    assert json.loads(payload)["options"] == {"batch_rows": instance_options.get("batch_rows", 1)}
    worker = PluginRegistry(allowlist=locks, options={"dal.plugins.parquet.dataset.batch_rows": 64})
    worker.reload()
    restored = SourceTaskCodec(worker).decode(payload)
    schema, batches = restored.table_format.execute(
        restored.partition,
        context=ExecutionContext(datetime.now(UTC) + timedelta(minutes=1), "fixture-read"),
    )
    values = list(batches)
    assert [batch.num_rows for batch in values] == batch_sizes
    assert pa.Table.from_batches(values, schema=schema).equals(expected, check_metadata=True)


def test_scan_envelope_requires_captured_options_even_for_version_one():
    codec, task = envelope()
    document = json.loads(codec.encode(task))
    del document["options"]
    with pytest.raises(ValueError, match="Invalid read payload"):
        codec.decode(json.dumps(document))


def test_scan_envelope_rejects_unadmitted_options_before_serialization():
    codec, task = envelope()
    source = replace(
        task.table_format, options={"endpoint": "https://user:sentinel-credential@host"}
    )
    with pytest.raises(ValueError, match="Unsupported format options") as error:
        codec.encode(replace(task, table_format=source))
    assert "sentinel-credential" not in str(error.value)


def test_live_format_binding_rejects_a_different_admitted_format_before_open(tmp_path):
    from dal_obscura.adapters.providers.published import LiveConfigCatalogRegistry
    from dal_obscura.kernel.plans.snapshot import (
        PublishedAssetSnapshot,
        PublishedCatalog,
        PublishedSource,
    )
    from dal_obscura.kernel.policy.models import AssetPolicy
    from dal_obscura.plugins import PluginRegistry
    from tests.support.plugins.plugins import manifest_registry
    from tests.support.plugins.public_plugins import _fixture

    root, _ = _fixture(tmp_path)
    manifest = manifest_registry()
    locks = {key: manifest.identity(*key) for key in manifest.admitted()}
    locks[("table_format", "iceberg")] = create_plugin_registry().identity(
        "table_format", "iceberg"
    )
    opened_formats = []

    def factory_loader(entry):
        factory = entry.load()
        if entry.name != "parquet.dataset":
            return factory

        def open_format(handle, context):
            opened_formats.append(handle.format_plugin_id)
            return factory(handle, context)

        return open_format

    plugins = PluginRegistry(allowlist=locks, factory_loader=factory_loader)
    plugins.reload()
    asset = PublishedAssetSnapshot(
        catalog=PublishedCatalog(
            "fixture", "manifest", 1, {"root": str(root), "manifest_path": "manifest.json"}
        ),
        target="default.users",
        source=PublishedSource("iceberg", "default.users", {}),
        policy=AssetPolicy(1, "fixture", "default.users", []),
    )

    class SnapshotReader:
        def get_asset_and_catalog(self, catalog, target):
            return asset

    catalogs = LiveConfigCatalogRegistry(SnapshotReader(), plugin_registry=plugins)
    try:
        with (
            pytest.raises(ValueError, match="Catalog and table-format plugins do not match"),
            catalogs.open("fixture", "default.users"),
        ):
            pass
        assert opened_formats == []
    finally:
        catalogs.close()
