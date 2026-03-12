from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pyarrow as pa
import pyarrow.dataset as ds
from pyiceberg.catalog import load_catalog

from dal_obscura.backend.base import Backend, Plan, ScanTask


@dataclass(frozen=True)
class IcebergConfig:
    catalog_name: str
    catalog_options: dict


class IcebergBackend(Backend):
    def __init__(self, config: IcebergConfig) -> None:
        self._config = config
        self._catalog = load_catalog(config.catalog_name, **config.catalog_options)

    def plan(self, table: str, columns: Iterable[str]) -> Plan:
        tbl = self._catalog.load_table(table)
        format_version = int(getattr(tbl.metadata, "format_version", 1))
        if format_version not in {2, 3}:
            raise ValueError(f"Unsupported Iceberg format version: {format_version}")
        snapshot_id = str(getattr(tbl, "current_snapshot_id", ""))

        column_tuple = tuple(columns)
        try:
            scan = tbl.scan(selected_fields=column_tuple)
        except TypeError:
            scan = tbl.scan().select(*column_tuple)

        tasks: list[ScanTask] = []
        try:
            for task in scan.plan_files():
                data_file = task.file
                descriptor: dict = {
                    "file_path": getattr(data_file, "file_path", None),
                    "file_format": getattr(data_file, "file_format", "parquet"),
                    "columns": list(column_tuple),
                }
                tasks.append(ScanTask(descriptor=descriptor))
        except Exception:
            tasks.append(ScanTask(descriptor={"mode": "full", "columns": list(column_tuple)}))

        return Plan(snapshot=snapshot_id, tasks=tasks)

    def read(self, table: str, snapshot: str, task: ScanTask) -> pa.Table:
        tbl = self._catalog.load_table(table)
        columns = task.descriptor.get("columns") or []
        if task.descriptor.get("mode") == "full":
            column_tuple = tuple(columns)
            try:
                scan = tbl.scan(selected_fields=column_tuple)
            except TypeError:
                scan = tbl.scan().select(*column_tuple)
            return scan.to_arrow()

        file_path = task.descriptor.get("file_path")
        if not file_path:
            raise ValueError("Scan task missing file path")
        dataset = ds.dataset([file_path])
        return dataset.to_table(columns=list(columns) if columns else None)
