"""Local/S3 path admission and immutable Delta snapshot loading."""

from __future__ import annotations

import json
from pathlib import PurePosixPath

import pyarrow.fs as fs
import pyarrow.parquet as pq
from dal_obscura_plugin_api import ExecutionContext
from dal_obscura_plugin_api.storage import StorageRoot
from deltalake import DeltaTable
from deltalake.exceptions import DeltaError

MAX_FILES = 10_000
MAX_LOG_BYTES = 64 * 1024 * 1024
MAX_DV_ROWS = 16_000_000


def _check_action_paths(row: dict, root: StorageRoot) -> None:
    for kind in ("add", "remove", "sidecar"):
        action = row.get(kind)
        if not isinstance(action, dict):
            continue
        if isinstance(action.get("path"), str):
            root.member(action["path"], encoded=True, exists=False)
        dv = action.get("deletionVector")
        if isinstance(dv, dict) and dv.get("storageType") == "p":
            root.member(dv["pathOrInlineDv"], exists=False)
        elif isinstance(dv, dict) and dv.get("storageType") == "u":
            encoded = dv.get("pathOrInlineDv")
            if not isinstance(encoded, str) or len(encoded) < 20:
                raise ValueError("Invalid Delta deletion-vector path")
            root.member(encoded[:-20], encoded=True, exists=False) if encoded[:-20] else None


def _check_log_file(path: str, root: StorageRoot, context: ExecutionContext) -> None:
    if path.endswith(".json"):
        for line in root.read(path, limit=MAX_LOG_BYTES).splitlines():
            context.check_active()
            if len(line) > 1_048_576:
                raise ValueError("Delta log action exceeds the byte budget")
            _check_action_paths(json.loads(line), root)
    elif path.endswith(".parquet"):
        with pq.ParquetFile(path, filesystem=root.filesystem) as checkpoint:
            columns = [
                name
                for name in ("add", "remove", "sidecar")
                if name in checkpoint.schema_arrow.names
            ]
            for batch in checkpoint.iter_batches(batch_size=256, columns=columns):
                context.check_active()
                for row in batch.to_pylist():
                    _check_action_paths(row, root)
    elif PurePosixPath(path).name == "_last_checkpoint":
        checkpoint = json.loads(root.read(path, limit=1_048_576))
        v2 = checkpoint.get("v2Checkpoint")
        if isinstance(v2, dict) and isinstance(v2.get("path"), str):
            root.member("_delta_log/" + v2["path"], encoded=True, exists=False)


def _check_logs(root: StorageRoot, context: ExecutionContext) -> None:
    """Guard kernel IO, including paths embedded in historical checkpoints.

    The public binding does not expose DV descriptors. Checking the bounded log
    directory before opening it prevents absolute DV paths escaping the table.
    This validates IO locations only; delta-rs owns snapshot reconciliation.
    """
    log = root.member("_delta_log")
    entries = root.filesystem.get_file_info(fs.FileSelector(log, recursive=True))
    if len(entries) > MAX_FILES:
        raise ValueError("Delta log exceeds the file budget")
    total = 0
    for entry in entries:
        context.check_active()
        path = root.member(root.relative(entry.path))
        if entry.type != fs.FileType.File:
            continue
        total += entry.size
        if total > MAX_LOG_BYTES:
            raise ValueError("Delta log exceeds the byte budget")
        _check_log_file(path, root, context)


def open_snapshot(
    path: StorageRoot, context: ExecutionContext, version: int | None = None
) -> DeltaTable:
    context.check_active()
    _check_logs(path, context)
    try:
        table = DeltaTable(path.uri, version=version)
    except DeltaError as exc:
        raise ValueError("Unable to load the pinned Delta snapshot") from exc
    context.check_active()
    protocol = table.protocol()
    supported = {"deletionVectors", "timestampNtz", "v2Checkpoint"}
    unsupported = set(protocol.reader_features or ()) - supported
    if protocol.min_reader_version not in (1, 3) or unsupported:
        raise ValueError(f"Unsupported Delta reader protocol/features: {protocol}")
    if table.metadata().configuration.get("delta.columnMapping.mode", "none") != "none":
        raise ValueError("Delta column mapping is not supported")
    return table
