"""Signed read scan envelopes select only already admitted factories."""

from __future__ import annotations

import base64
import json

import pyarrow as pa
from dal_obscura_plugin_api import ScanTask as PluginScanTask
from dal_obscura_plugin_api import TableHandle
from dal_obscura_plugin_api.options import PluginOptions
from dal_obscura_plugin_api.schema.bounds import (
    MAX_SCHEMA_ENCODING_BYTES,
    validate_arrow_schema_bounds,
    validate_native_schema,
)
from dal_obscura_plugin_api.storage import PathRuleEnforcer

from dal_obscura.adapters.providers.adapters import (
    PublicPluginPartition,
    PublicPluginTableFormat,
)
from dal_obscura.application.reads.sources import ScanTask
from dal_obscura.plugins import PluginRegistry


class SourceTaskCodec:
    def __init__(self, registry: PluginRegistry) -> None:
        self.registry = registry

    def encode(self, task: ScanTask) -> str:
        table, partition = task.table_format, task.partition
        if not isinstance(table, PublicPluginTableFormat) or not isinstance(
            partition, PublicPluginPartition
        ):
            raise ValueError("Unregistered table format cannot issue scan tickets")
        handle, data, output_schema = table.handle, partition.task, partition.schema
        roots = table.path_roots
        envelope = {
            "version": 1,
            "plugin": list(self.registry.identity("table_format", handle.format_plugin_id)),
            "handle": handle.to_json(),
            "schema": _encode_schema(task.schema),
            "output_schema": _encode_schema(output_schema, for_execution=True),
            "path_roots": list(roots),
            "options": PluginOptions({table.format: table.options}).to_json()[table.format],
            "task": data.to_json(),
        }
        return json.dumps(envelope, sort_keys=True, separators=(",", ":"), allow_nan=False)

    def decode(self, payload: str) -> ScanTask:
        try:
            raw = json.loads(payload, object_pairs_hook=_unique_json_object)
            if (
                not isinstance(raw, dict)
                or set(raw)
                != {
                    "version",
                    "plugin",
                    "handle",
                    "schema",
                    "output_schema",
                    "path_roots",
                    "task",
                    "options",
                }
                or type(raw["version"]) is not int
                or raw["version"] != 1
            ):
                raise ValueError("Unsupported scan envelope version")
            handle = TableHandle.from_json(raw["handle"])
            if raw["plugin"] != list(
                self.registry.identity("table_format", handle.format_plugin_id)
            ):
                raise ValueError("Scan plugin artifact has changed; re-plan the read")
            roots = raw["path_roots"]
            if not isinstance(roots, list) or any(not isinstance(root, str) for root in roots):
                raise ValueError("Invalid scan path roots")
            PathRuleEnforcer([{"root": root} for root in roots])
            task = PluginScanTask.from_json(raw["task"])
            schema = _decode_schema(raw["schema"])
            output_schema = _decode_schema(raw["output_schema"], for_execution=True)
            factory = self.registry.load("table_format", handle.format_plugin_id)
            table = PublicPluginTableFormat(
                catalog_name=handle.catalog_instance_id,
                table_name=".".join((*handle.identifier.namespace, handle.identifier.name)),
                format=handle.format_plugin_id,
                handle=handle,
                format_factory=factory,
                path_roots=tuple(roots),
                options=self.registry.format_options(
                    handle.format_plugin_id, captured=raw["options"]
                ),
            )
            partition = PublicPluginPartition(
                task=task,
                schema=output_schema,
            )
            return ScanTask(table, schema, partition)
        except (ValueError, TypeError, KeyError, OverflowError, RecursionError) as exc:
            raise ValueError("Invalid read payload in ticket") from exc


def _encode_schema(schema: pa.Schema, *, for_execution: bool = False) -> str:
    validator = validate_native_schema if for_execution else validate_arrow_schema_bounds
    validator(schema)
    return base64.b64encode(schema.serialize()).decode("ascii")


def _decode_schema(raw: object, *, for_execution: bool = False) -> pa.Schema:
    if not isinstance(raw, str) or len(raw) > 4 * ((MAX_SCHEMA_ENCODING_BYTES + 2) // 3):
        raise ValueError("Invalid scan schema")
    decoded = base64.b64decode(raw, validate=True)
    if len(decoded) > MAX_SCHEMA_ENCODING_BYTES:
        raise ValueError("Invalid scan schema")
    schema = pa.ipc.read_schema(pa.BufferReader(decoded))
    validator = validate_native_schema if for_execution else validate_arrow_schema_bounds
    validator(schema)
    return schema


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate scan envelope field")
        result[key] = value
    return result
