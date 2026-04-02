from __future__ import annotations

import pickle
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

import pyarrow as pa
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNull,
    LessThan,
    LessThanOrEqual,
    NotEqualTo,
    NotNull,
    Or,
)
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.table import ALWAYS_TRUE

from dal_obscura.domain.access_control.filters import (
    BooleanFilter,
    BooleanOperator,
    ComparisonFilter,
    InFilter,
    NullFilter,
    RowFilter,
    RowFilterPayload,
    deserialize_row_filter,
    serialize_row_filter,
)
from dal_obscura.domain.catalog.ports import TableFormat
from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.domain.table_format.ports import InputPartition, Plan, ScanTask


@dataclass(frozen=True, kw_only=True)
class IcebergInputPartition(InputPartition):
    """Concrete partition containing specific Iceberg file scan tasks."""

    columns: list[str]
    tasks: list[bytes]
    pushdown_row_filter: RowFilterPayload | None = None


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
        pushdown_row_filter, residual_row_filter = _split_row_filter(request.row_filter)
        iceberg_row_filter = _compile_row_filter(pushdown_row_filter)

        try:
            scan = pyiceberg_table.scan(
                row_filter=iceberg_row_filter,
                selected_fields=column_tuple,
            )
        except TypeError:
            scan = pyiceberg_table.scan(row_filter=iceberg_row_filter).select(*column_tuple)

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
                    pushdown_row_filter=None
                    if pushdown_row_filter is None
                    else serialize_row_filter(pushdown_row_filter),
                ),
            )
            for group in groups
        ]
        return Plan(schema=base_schema, tasks=tasks, residual_row_filter=residual_row_filter)

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        """Executes the pre-planned Iceberg file tasks stored in the ticket."""
        if not isinstance(partition, IcebergInputPartition):
            raise TypeError("IcebergTableFormat requires an IcebergInputPartition")

        table = self._load_table()
        column_tuple = tuple(partition.columns)
        file_tasks = [pickle.loads(task) for task in partition.tasks]
        pushdown_row_filter = (
            None
            if partition.pushdown_row_filter is None
            else deserialize_row_filter(partition.pushdown_row_filter)
        )

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
            row_filter=_compile_row_filter(pushdown_row_filter),
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


def _split_row_filter(row_filter: RowFilter | None) -> tuple[RowFilter | None, RowFilter | None]:
    if row_filter is None:
        return None, None
    if _is_pushdown_safe(row_filter):
        return row_filter, None
    if isinstance(row_filter, BooleanFilter) and row_filter.operator == "and":
        pushdown_clauses: list[RowFilter] = []
        residual_clauses: list[RowFilter] = []
        for clause in row_filter.clauses:
            pushdown_clause, residual_clause = _split_row_filter(clause)
            if pushdown_clause is not None:
                pushdown_clauses.append(pushdown_clause)
            if residual_clause is not None:
                residual_clauses.append(residual_clause)
        return _combine_boolean("and", pushdown_clauses), _combine_boolean("and", residual_clauses)
    return None, row_filter


def _is_pushdown_safe(row_filter: RowFilter) -> bool:
    if isinstance(row_filter, BooleanFilter):
        if row_filter.operator == "and":
            return all(_is_pushdown_safe(clause) for clause in row_filter.clauses)
        return all(_is_pushdown_safe(clause) for clause in row_filter.clauses)
    return "." not in row_filter.field.path


def _combine_boolean(operator: BooleanOperator, clauses: list[RowFilter]) -> RowFilter | None:
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return BooleanFilter(operator=operator, clauses=tuple(clauses))


def _compile_row_filter(row_filter: RowFilter | None) -> BooleanExpression:
    if row_filter is None:
        return ALWAYS_TRUE
    if isinstance(row_filter, ComparisonFilter):
        return _compile_comparison_filter(row_filter)
    if isinstance(row_filter, InFilter):
        in_expr = cast(Any, In)
        return cast(
            BooleanExpression,
            in_expr(
                term=row_filter.field.path,
                literals=[value.value for value in row_filter.values],
            ),
        )
    if isinstance(row_filter, NullFilter):
        if row_filter.is_null:
            return cast(BooleanExpression, cast(Any, IsNull)(term=row_filter.field.path))
        return cast(BooleanExpression, cast(Any, NotNull)(term=row_filter.field.path))
    compiled_clauses = [_compile_row_filter(clause) for clause in row_filter.clauses]
    left, right, *rest = compiled_clauses
    if row_filter.operator == "and":
        return And(left, right, *rest)
    return Or(left, right, *rest)


def _compile_comparison_filter(row_filter: ComparisonFilter) -> BooleanExpression:
    if row_filter.operator == "=":
        return cast(
            BooleanExpression,
            cast(Any, EqualTo)(term=row_filter.field.path, value=row_filter.value.value),
        )
    if row_filter.operator == "!=":
        return cast(
            BooleanExpression,
            cast(Any, NotEqualTo)(term=row_filter.field.path, value=row_filter.value.value),
        )
    if row_filter.operator == ">":
        return cast(
            BooleanExpression,
            cast(Any, GreaterThan)(term=row_filter.field.path, value=row_filter.value.value),
        )
    if row_filter.operator == ">=":
        return cast(
            BooleanExpression,
            cast(Any, GreaterThanOrEqual)(
                term=row_filter.field.path,
                value=row_filter.value.value,
            ),
        )
    if row_filter.operator == "<":
        return cast(
            BooleanExpression,
            cast(Any, LessThan)(term=row_filter.field.path, value=row_filter.value.value),
        )
    return cast(
        BooleanExpression,
        cast(Any, LessThanOrEqual)(term=row_filter.field.path, value=row_filter.value.value),
    )
