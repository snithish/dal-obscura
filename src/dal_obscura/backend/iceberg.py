from __future__ import annotations

import pickle
from dataclasses import dataclass
from typing import Iterable

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import ALWAYS_TRUE

from dal_obscura.backend.base import Backend, Plan, ReadPayload


@dataclass(frozen=True)
class IcebergConfig:
    catalog_name: str
    catalog_options: dict


class IcebergBackend(Backend):
    def __init__(self, config: IcebergConfig) -> None:
        self._config = config
        self._catalog = load_catalog(config.catalog_name, **config.catalog_options)

    def plan(self, table: str, columns: Iterable[str], max_tickets: int) -> Plan:
        tbl = self._catalog.load_table(table)
        format_version = int(getattr(tbl.metadata, "format_version", 1))
        if format_version not in {2, 3}:
            raise ValueError(f"Unsupported Iceberg format version: {format_version}")

        column_tuple = tuple(columns)
        try:
            scan = tbl.scan(selected_fields=column_tuple)
        except TypeError:
            scan = tbl.scan().select(*column_tuple)

        tasks: list[ReadPayload] = []
        try:
            file_tasks = list(scan.plan_files())
            max_tickets = max(1, max_tickets)
            for group in _chunk_by_max_tickets(file_tasks, max_tickets):
                spec = IcebergReadSpec(
                    table=table,
                    columns=list(column_tuple),
                    mode="files",
                    tasks=group,
                )
                tasks.append(ReadPayload(payload=pickle.dumps(spec)))
        except Exception:
            spec = IcebergReadSpec(
                table=table,
                columns=list(column_tuple),
                mode="full",
                tasks=[],
            )
            tasks.append(ReadPayload(payload=pickle.dumps(spec)))

        return Plan(tasks=tasks)

    def read(self, read_payload: bytes) -> pa.Table:
        spec = pickle.loads(read_payload)
        if not isinstance(spec, IcebergReadSpec):
            raise ValueError("Invalid read payload for Iceberg backend")
        tbl = self._catalog.load_table(spec.table)
        column_tuple = tuple(spec.columns)
        if spec.mode == "full":
            try:
                scan = tbl.scan(selected_fields=column_tuple)
            except TypeError:
                scan = tbl.scan().select(*column_tuple)
            return scan.to_arrow()

        file_tasks = spec.tasks

        projected_schema = tbl.schema()
        if column_tuple:
            projected_schema = projected_schema.select(*column_tuple)

        arrow_scan = ArrowScan(
            table_metadata=tbl.metadata,
            io=tbl.io,
            projected_schema=projected_schema,
            row_filter=ALWAYS_TRUE,
        )
        return arrow_scan.to_table(file_tasks)


@dataclass(frozen=True)
class IcebergReadSpec:
    table: str
    columns: list[str]
    mode: str
    tasks: list


def _chunk_by_max_tickets(tasks: list, max_tickets: int) -> list[list]:
    if not tasks:
        return []
    max_tickets = max(1, max_tickets)
    chunk_size = max(1, (len(tasks) + max_tickets - 1) // max_tickets)
    return [tasks[i : i + chunk_size] for i in range(0, len(tasks), chunk_size)]
