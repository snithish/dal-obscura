from __future__ import annotations

import pickle
from dataclasses import dataclass
from typing import Iterable

from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import ALWAYS_TRUE

from dal_obscura.domain.query_planning import Plan, ReadPayload, ReadSpec


@dataclass(frozen=True)
class IcebergConfig:
    catalog_name: str
    catalog_options: dict


@dataclass(frozen=True)
class IcebergReadSpec:
    table: str
    columns: list[str]
    tasks: list[bytes]


class IcebergBackend:
    def __init__(self, config: IcebergConfig) -> None:
        self._config = config
        self._catalog = load_catalog(config.catalog_name, **config.catalog_options)

    def get_schema(self, table: str):
        tbl = self._load_table(table)
        return tbl.schema().as_arrow()

    def plan(self, table: str, columns: Iterable[str], max_tickets: int) -> Plan:
        tbl = self._load_table(table)
        column_tuple = tuple(columns)
        base_schema = tbl.schema().as_arrow()
        try:
            scan = tbl.scan(selected_fields=column_tuple)
        except TypeError:
            scan = tbl.scan().select(*column_tuple)

        file_tasks = list(scan.plan_files())
        groups = _chunk_by_max_tickets(file_tasks, max_tickets)
        if not groups:
            groups = [[]]

        tasks: list[ReadPayload] = []
        for group in groups:
            spec = IcebergReadSpec(
                table=table,
                columns=list(column_tuple),
                tasks=[pickle.dumps(task) for task in group],
            )
            tasks.append(ReadPayload(payload=pickle.dumps(spec)))
        return Plan(schema=base_schema, tasks=tasks)

    def read_spec(self, read_payload: bytes) -> ReadSpec:
        spec = self._decode_spec(read_payload)
        return ReadSpec(table=spec.table, columns=list(spec.columns))

    def read_stream(self, read_payload: bytes):
        spec = self._decode_spec(read_payload)
        tbl = self._load_table(spec.table)
        column_tuple = tuple(spec.columns)
        file_tasks = [pickle.loads(task) for task in spec.tasks]

        projected_schema = tbl.schema()
        if column_tuple:
            projected_schema = projected_schema.select(*column_tuple)
        if not file_tasks:
            return iter(())

        arrow_scan = ArrowScan(
            table_metadata=tbl.metadata,
            io=tbl.io,
            projected_schema=projected_schema,
            row_filter=ALWAYS_TRUE,
        )
        return arrow_scan.to_record_batches(file_tasks)

    def _decode_spec(self, read_payload: bytes) -> IcebergReadSpec:
        spec = pickle.loads(read_payload)
        if not isinstance(spec, IcebergReadSpec):
            raise ValueError("Invalid read payload for Iceberg backend")
        return spec

    def _load_table(self, table: str):
        tbl = self._catalog.load_table(table)
        format_version = int(getattr(tbl.metadata, "format_version", 1))
        if format_version not in {2, 3}:
            raise ValueError(f"Unsupported Iceberg format version: {format_version}")
        return tbl


def _chunk_by_max_tickets(tasks: list, max_tickets: int) -> list[list]:
    if not tasks:
        return []
    max_tickets = max(1, max_tickets)
    group_count = min(max_tickets, len(tasks))
    groups: list[list] = [[] for _ in range(group_count)]
    for index, task in enumerate(tasks):
        groups[index % group_count].append(task)
    return groups
