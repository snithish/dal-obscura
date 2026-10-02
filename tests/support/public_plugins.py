from __future__ import annotations

import base64
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dal_obscura_manifest_parquet.format import FORMAT_DESCRIPTOR
from dal_obscura_plugin_api import TableHandle, TableIdentifier

from dal_obscura.sources.plugin_runtime import (
    PublicPluginTableFormat,
)


def _fixture(tmp_path: Path) -> tuple[Path, pa.Table]:
    root = tmp_path / "dataset"
    root.mkdir()
    table = pa.table({"id": pa.array([1, 2], type=pa.int64())})
    pq.write_table(table, root / "part.parquet", row_group_size=1)
    schema_ipc = base64.b64encode(table.schema.serialize().to_pybytes()).decode("ascii")
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "revision": "r1",
                "tables": {
                    "default.users": {
                        "namespace": ["default"],
                        "name": "users",
                        "files": ["part.parquet"],
                        "schema_ipc": schema_ipc,
                        "field_ids": ["id"],
                    }
                },
            }
        )
    )
    return root, table


def _contract_format(schema, *, tasks=("scan",)):
    from dal_obscura_plugin_api import SchemaDescriptor

    calls = []
    handle = TableHandle(
        catalog_plugin_id="manifest",
        catalog_instance_id="fixture",
        catalog_revision=1,
        identifier=TableIdentifier(namespace=("default",), name="users"),
        format_plugin_id="parquet.dataset",
        handle_version=1,
    )

    class Format:
        descriptor = FORMAT_DESCRIPTOR

        def schema(self, handle, context):
            return SchemaDescriptor(schema_version=1, fingerprint="0" * 64, arrow_schema=schema)

        def plan(self, handle, schema, context, **kwargs):
            calls.append(kwargs)
            return list(tasks)

        def execute(self, task, context):
            calls.append("execute")
            return schema, iter(())

        def close(self):
            calls.append("close")

    def factory(handle, context):
        calls.append("open")
        return Format()

    return PublicPluginTableFormat(
        catalog_name="fixture",
        table_name="default.users",
        format="parquet.dataset",
        format_factory=factory,
        handle=handle,
    ), calls
