from __future__ import annotations

from collections.abc import Iterable, Iterator
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

_FILE_FORMAT_BATCH_SIZE = 8_192


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
        execution_columns = _execution_columns(request.columns)
        fragments = list(self._dataset().get_fragments(filter=arrow_filter))
        groups = _groups(fragments, max_tickets) or [[]]
        tasks = [
            ScanTask(
                table_format=self,
                schema=schema,
                partition=FileInputPartition(
                    columns=execution_columns,
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
        execution_columns = _execution_columns(request.columns)
        paths = _paths(self.uri)
        groups = _groups(paths, max_tickets) or [[]]
        return Plan(
            schema=schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=schema,
                    partition=FileInputPartition(
                        columns=execution_columns,
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
        schema = _projected_schema(self.get_schema(), partition.columns)
        return schema, _avro_batches(
            partition.paths,
            columns=partition.columns,
            row_filter_sql=partition.row_filter_sql,
            path_enforcer=self.path_enforcer,
        )


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
        execution_columns = _execution_columns(request.columns)
        paths = _paths(self.uri)
        groups = _groups(paths, max_tickets) or [[]]
        return Plan(
            schema=schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=schema,
                    partition=FileInputPartition(
                        columns=execution_columns,
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
        schema = _projected_schema(self.get_schema(), partition.columns)
        return schema, _text_batches(
            partition.paths,
            column_name=self.column_name,
            columns=partition.columns,
            row_filter_sql=partition.row_filter_sql,
            path_enforcer=self.path_enforcer,
        )


def _avro_batches(
    paths: list[str],
    *,
    columns: list[str],
    row_filter_sql: str | None,
    path_enforcer: PathRuleEnforcer | None,
) -> Iterator[pa.RecordBatch]:
    for path in paths:
        table = _read_avro_table(path, path_enforcer)
        yield from _filtered_table_batches(
            table,
            columns=columns,
            row_filter_sql=row_filter_sql,
        )


def _text_batches(
    paths: list[str],
    *,
    column_name: str,
    columns: list[str],
    row_filter_sql: str | None,
    path_enforcer: PathRuleEnforcer | None,
) -> Iterator[pa.RecordBatch]:
    pending: list[str] = []
    for path in paths:
        _check_path(path, path_enforcer)
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                pending.append(line.rstrip("\n"))
                if len(pending) >= _FILE_FORMAT_BATCH_SIZE:
                    yield from _text_chunk_batches(
                        pending,
                        column_name=column_name,
                        columns=columns,
                        row_filter_sql=row_filter_sql,
                    )
                    pending = []
    if pending:
        yield from _text_chunk_batches(
            pending,
            column_name=column_name,
            columns=columns,
            row_filter_sql=row_filter_sql,
        )


def _text_chunk_batches(
    lines: list[str],
    *,
    column_name: str,
    columns: list[str],
    row_filter_sql: str | None,
) -> Iterator[pa.RecordBatch]:
    table = pa.table({column_name: lines})
    yield from _filtered_table_batches(table, columns=columns, row_filter_sql=row_filter_sql)


def _filtered_table_batches(
    table: pa.Table,
    *,
    columns: list[str],
    row_filter_sql: str | None,
) -> Iterator[pa.RecordBatch]:
    row_filter = _partition_filter(row_filter_sql, table.schema)
    scanner = ds.dataset(table).scanner(
        columns=columns or None,
        filter=row_filter,
        batch_size=_FILE_FORMAT_BATCH_SIZE,
    )
    yield from scanner.to_batches()


def _projected_schema(schema: pa.Schema, columns: list[str]) -> pa.Schema:
    if not columns:
        return schema
    return pa.schema([schema.field(column) for column in columns])


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


def _execution_columns(columns: Iterable[str]) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()
    for column in columns:
        top_level = column.split(".", 1)[0]
        if top_level in seen:
            continue
        seen.add(top_level)
        selected.append(top_level)
    return selected


def _filter_sql(row_filter: RowFilter | None, arrow_filter: ds.Expression | None) -> str | None:
    if row_filter is None or arrow_filter is None:
        return None
    return row_filter_to_sql(row_filter)


def _partition_filter(filter_sql: str | None, schema: pa.Schema) -> ds.Expression | None:
    if filter_sql is None:
        return None
    from dal_obscura.common.access_control.filters import parse_row_filter

    return row_filter_to_arrow_expression(parse_row_filter(filter_sql, schema))
