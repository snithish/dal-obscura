from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds

from dal_obscura.common.catalog.ports import TableFormat
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer


@dataclass(frozen=True, kw_only=True)
class FileInputPartition(InputPartition):
    columns: list[str]
    paths: list[str]


@dataclass(frozen=True, kw_only=True)
class ArrowDatasetTableFormat(TableFormat):
    uri: str
    format: str
    options: dict[str, Any] = field(default_factory=dict)
    path_enforcer: PathRuleEnforcer | None = None

    def get_schema(self) -> pa.Schema:
        return self._dataset().schema

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        del max_tickets
        schema = self.get_schema()
        task = ScanTask(
            table_format=self,
            schema=schema,
            partition=FileInputPartition(columns=list(request.columns), paths=[self.uri]),
        )
        return Plan(
            schema=schema,
            tasks=[task],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, FileInputPartition):
            raise TypeError("ArrowDatasetTableFormat requires a FileInputPartition")
        for path in partition.paths:
            _check_path(path, self.path_enforcer)
        dataset = self._dataset()
        scanner = dataset.scanner(columns=partition.columns or None)
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
        table = _read_avro_table(self.uri, self.path_enforcer)
        return table.schema

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        del max_tickets
        schema = self.get_schema()
        return Plan(
            schema=schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=schema,
                    partition=FileInputPartition(columns=list(request.columns), paths=[self.uri]),
                )
            ],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, FileInputPartition):
            raise TypeError("AvroTableFormat requires a FileInputPartition")
        table = _read_avro_table(partition.paths[0], self.path_enforcer)
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
        del max_tickets
        schema = self.get_schema()
        return Plan(
            schema=schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=schema,
                    partition=FileInputPartition(columns=list(request.columns), paths=[self.uri]),
                )
            ],
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=None,
            residual_row_filter=request.row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        if not isinstance(partition, FileInputPartition):
            raise TypeError("TextTableFormat requires a FileInputPartition")
        path = partition.paths[0]
        _check_path(path, self.path_enforcer)
        with open(path, encoding="utf-8") as handle:
            lines = [line.rstrip("\n") for line in handle]
        table = pa.table({self.column_name: lines})
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
