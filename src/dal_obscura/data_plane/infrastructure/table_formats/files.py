from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds

from dal_obscura.common.access_control.filters import RowFilter, row_filter_to_sql
from dal_obscura.common.catalog.ports import TableFormat
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.table_formats.filters import (
    row_filter_to_arrow_expression,
)


@dataclass(frozen=True, kw_only=True)
class FileInputPartition(InputPartition):
    columns: list[str]
    paths: list[str]
    row_filter_sql: str | None = None


@dataclass(frozen=True, kw_only=True)
class ArrowDatasetTableFormat(TableFormat):
    uri: str
    format: str
    options: dict[str, Any] = field(default_factory=dict)
    path_enforcer: PathRuleEnforcer | None = None

    def get_schema(self) -> pa.Schema:
        return self._dataset().schema

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        arrow_filter = row_filter_to_arrow_expression(request.row_filter)
        schema = self.get_schema()
        fragments = list(self._dataset().get_fragments(filter=arrow_filter))
        groups = _groups(fragments, max_tickets) or [[]]
        tasks = [
            ScanTask(
                table_format=self,
                schema=schema,
                partition=FileInputPartition(
                    columns=list(request.columns),
                    paths=[fragment.path for fragment in group],
                    row_filter_sql=_filter_sql(request.row_filter, arrow_filter),
                ),
            )
            for group in groups
        ]
        return Plan(
            schema=schema,
            tasks=tasks,
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=request.row_filter if arrow_filter is not None else None,
            residual_row_filter=None if arrow_filter is not None else request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, FileInputPartition):
            raise TypeError("ArrowDatasetTableFormat requires a FileInputPartition")
        for path in partition.paths:
            _check_path(path, self.path_enforcer)
        dataset = (
            ds.dataset(partition.paths, format=self.format, **self.options)
            if partition.paths
            else self._dataset()
        )
        row_filter = _partition_filter(partition.row_filter_sql, dataset.schema)
        scanner = dataset.scanner(columns=partition.columns or None, filter=row_filter)
        return scanner.projected_schema, scanner.to_batches()

    def _dataset(self) -> ds.Dataset:
        _check_path(self.uri, self.path_enforcer)
        return ds.dataset(self.uri, format=self.format, **self.options)


@dataclass(frozen=True, kw_only=True)
class AvroTableFormat(TableFormat):
    format: str = "avro"
    uri: str
    options: dict[str, Any] = field(default_factory=dict)
    path_enforcer: PathRuleEnforcer | None = None

    def get_schema(self) -> pa.Schema:
        table = _read_avro_table(_paths(self.uri)[0], self.path_enforcer)
        return table.schema

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        arrow_filter = row_filter_to_arrow_expression(request.row_filter)
        schema = self.get_schema()
        paths = _paths(self.uri)
        groups = _groups(paths, max_tickets) or [[]]
        return Plan(
            schema=schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=schema,
                    partition=FileInputPartition(
                        columns=list(request.columns),
                        paths=list(group),
                        row_filter_sql=_filter_sql(request.row_filter, arrow_filter),
                    ),
                )
                for group in groups
            ],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, FileInputPartition):
            raise TypeError("AvroTableFormat requires a FileInputPartition")
        table = (
            pa.concat_tables(
                [_read_avro_table(path, self.path_enforcer) for path in partition.paths]
            )
            if partition.paths
            else pa.Table.from_batches([], schema=self.get_schema())
        )
        row_filter = _partition_filter(partition.row_filter_sql, table.schema)
        if row_filter is not None:
            table = ds.dataset(table).scanner(filter=row_filter).to_table()
        if partition.columns:
            table = table.select(partition.columns)
        return table.schema, table.to_batches()


@dataclass(frozen=True, kw_only=True)
class TextTableFormat(TableFormat):
    format: str = "text"
    uri: str
    column_name: str = "value"
    path_enforcer: PathRuleEnforcer | None = None

    def get_schema(self) -> pa.Schema:
        return pa.schema([pa.field(self.column_name, pa.string())])

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        arrow_filter = row_filter_to_arrow_expression(request.row_filter)
        schema = self.get_schema()
        paths = _paths(self.uri)
        groups = _groups(paths, max_tickets) or [[]]
        return Plan(
            schema=schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=schema,
                    partition=FileInputPartition(
                        columns=list(request.columns),
                        paths=list(group),
                        row_filter_sql=_filter_sql(request.row_filter, arrow_filter),
                    ),
                )
                for group in groups
            ],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, FileInputPartition):
            raise TypeError("TextTableFormat requires a FileInputPartition")
        lines: list[str] = []
        for path in partition.paths:
            _check_path(path, self.path_enforcer)
            with open(path, encoding="utf-8") as handle:
                lines.extend(line.rstrip("\n") for line in handle)
        table = pa.table({self.column_name: lines})
        row_filter = _partition_filter(partition.row_filter_sql, table.schema)
        if row_filter is not None:
            table = ds.dataset(table).scanner(filter=row_filter).to_table()
        if partition.columns:
            table = table.select(partition.columns)
        return table.schema, table.to_batches()


def _read_avro_table(path: str, enforcer: PathRuleEnforcer | None) -> pa.Table:
    _check_path(path, enforcer)
    import fastavro

    with open(path, "rb") as handle:
        records = list(fastavro.reader(handle))
    return pa.Table.from_pylist(records)


def _check_path(path: str, enforcer: PathRuleEnforcer | None) -> None:
    if enforcer is not None:
        enforcer.check(path)


def _paths(uri: str) -> list[str]:
    path = Path(uri)
    if path.is_dir():
        return sorted(str(item) for item in path.iterdir() if item.is_file())
    return [uri]


def _groups(items: list[Any], max_tickets: int) -> list[list[Any]]:
    if not items:
        return []
    ticket_count = max(1, min(max_tickets, len(items)))
    return [
        items[index::ticket_count] for index in range(ticket_count) if items[index::ticket_count]
    ]


def _filter_sql(row_filter: RowFilter | None, arrow_filter: ds.Expression | None) -> str | None:
    if row_filter is None or arrow_filter is None:
        return None
    return row_filter_to_sql(row_filter)


def _partition_filter(filter_sql: str | None, schema: pa.Schema) -> ds.Expression | None:
    if filter_sql is None:
        return None
    from dal_obscura.common.access_control.filters import parse_row_filter

    return row_filter_to_arrow_expression(parse_row_filter(filter_sql, schema))
