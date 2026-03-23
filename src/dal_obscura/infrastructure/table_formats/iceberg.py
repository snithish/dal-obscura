from __future__ import annotations

import pickle
from collections.abc import Iterable
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import ALWAYS_TRUE

from dal_obscura.domain.catalog.ports import TableFormat
from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.domain.table_format.ports import InputPartition, Plan, ScanTask


@dataclass(frozen=True, kw_only=True)
class IcebergInputPartition(InputPartition):
    """Concrete partition containing specific Iceberg file scan tasks."""

    columns: list[str]
    tasks: list[bytes]


@dataclass(frozen=True, kw_only=True)
class IcebergTableFormat(TableFormat):
    """Catalog-resolved Iceberg table that can self-plan and self-execute."""

    format: str = "iceberg"
    metadata_location: str
    io_options: dict[str, object]

    def get_schema(self) -> pa.Schema:
        """Loads the Iceberg table schema used by planning and validation."""
        pyiceberg_table = self._load_table()
        return pyiceberg_table.schema().as_arrow()

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        """Plans file tasks and distributes them across the available tickets."""
        pyiceberg_table = self._load_table()

        column_tuple = tuple(request.columns)
        base_schema = pyiceberg_table.schema().as_arrow()

        try:
            scan = pyiceberg_table.scan(selected_fields=column_tuple)
        except TypeError:
            scan = pyiceberg_table.scan().select(*column_tuple)

        file_tasks = list(scan.plan_files())
        groups = _chunk_by_max_tickets(file_tasks, max_tickets)
        if not groups:
            groups = [[]]

        tasks = [
            ScanTask(
                table_format=self,
                schema=base_schema,
                partition=IcebergInputPartition(
                    columns=list(column_tuple),
                    tasks=[pickle.dumps(task) for task in group],
                ),
            )
            for group in groups
        ]
        return Plan(schema=base_schema, tasks=tasks)

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        """Executes the pre-planned Iceberg file tasks stored in the ticket."""
        if not isinstance(partition, IcebergInputPartition):
            raise TypeError("IcebergTableFormat requires an IcebergInputPartition")

        table = self._load_table()
        column_tuple = tuple(partition.columns)
        file_tasks = [pickle.loads(task) for task in partition.tasks]

        projected_schema = table.schema()
        if column_tuple:
            projected_schema = projected_schema.select(*column_tuple)

        arrow_schema = projected_schema.as_arrow()

        if not file_tasks:
            return arrow_schema, iter(())

        arrow_scan = ArrowScan(
            table_metadata=table.metadata,
            io=table.io,
            projected_schema=projected_schema,
            row_filter=ALWAYS_TRUE,
        )
        return arrow_schema, arrow_scan.to_record_batches(file_tasks)

    def _load_table(
        self,
    ):
        """Loads the table from metadata location and enforces the supported
        Iceberg format versions."""
        from pyiceberg.table import StaticTable

        table = StaticTable.from_metadata(self.metadata_location, properties=self.io_options)
        format_version = int(getattr(table.metadata, "format_version", 1))
        if format_version not in {2, 3}:
            raise ValueError(f"Unsupported Iceberg format version: {format_version}")
        return table


def _chunk_by_max_tickets(tasks: list, max_tickets: int) -> list[list]:
    """Balances planned Iceberg file tasks across the available tickets."""
    if not tasks:
        return []
    max_tickets = max(1, max_tickets)
    group_count = min(max_tickets, len(tasks))
    groups: list[list] = [[] for _ in range(group_count)]
    for index, task in enumerate(tasks):
        groups[index % group_count].append(task)
    return groups
