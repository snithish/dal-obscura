from __future__ import annotations

import logging
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
from sqlglot import exp

from dal_obscura.domain.access_control.filters import (
    RowFilter,
    deserialize_row_filter,
    serialize_row_filter,
)
from dal_obscura.domain.catalog.ports import TableFormat
from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.domain.table_format.ports import InputPartition, Plan, ScanTask

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class IcebergInputPartition(InputPartition):
    """Concrete partition containing specific Iceberg file scan tasks."""

    columns: list[str]
    tasks: list[bytes]
    backend_pushdown_row_filter: str | None = None


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
        LOGGER.debug(
            "iceberg_filter_split",
            extra={
                "pushdown_row_filter_present": pushdown_row_filter is not None,
                "residual_row_filter_present": residual_row_filter is not None,
            },
        )
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
                    backend_pushdown_row_filter=None
                    if pushdown_row_filter is None
                    else serialize_row_filter(pushdown_row_filter),
                ),
            )
            for group in groups
        ]
        return Plan(
            schema=base_schema,
            tasks=tasks,
            full_row_filter=request.row_filter,
            backend_pushdown_row_filter=pushdown_row_filter,
            residual_row_filter=residual_row_filter,
        )

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        """Executes the pre-planned Iceberg file tasks stored in the ticket."""
        if not isinstance(partition, IcebergInputPartition):
            raise TypeError("IcebergTableFormat requires an IcebergInputPartition")

        table = self._load_table()
        column_tuple = tuple(partition.columns)
        file_tasks = [pickle.loads(task) for task in partition.tasks]
        pushdown_row_filter = (
            None
            if partition.backend_pushdown_row_filter is None
            else deserialize_row_filter(partition.backend_pushdown_row_filter)
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

    clauses = _flatten_and_clauses(row_filter.expression)
    pushdown_clauses = [clause for clause in clauses if _is_pushdown_safe(clause)]
    residual_clauses = [clause for clause in clauses if not _is_pushdown_safe(clause)]

    return _row_filter_from_clauses(pushdown_clauses), _row_filter_from_clauses(residual_clauses)


def _flatten_and_clauses(expression: exp.Expr) -> list[exp.Expr]:
    expression = _strip_parens(expression)
    if isinstance(expression, exp.And):
        return _flatten_and_clauses(expression.this) + _flatten_and_clauses(expression.expression)
    return [expression.copy()]


def _row_filter_from_clauses(clauses: list[exp.Expr]) -> RowFilter | None:
    if not clauses:
        return None

    expression = clauses[0].copy()
    for clause in clauses[1:]:
        expression = exp.and_(expression, clause.copy())
    return deserialize_row_filter(expression.sql(dialect="duckdb"))


def _is_pushdown_safe(expression: exp.Expr) -> bool:
    expression = _strip_parens(expression)

    if isinstance(expression, (exp.And, exp.Or)):
        return _is_pushdown_safe(expression.this) and _is_pushdown_safe(expression.expression)

    if isinstance(expression, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        return _is_top_level_column(expression.this) and _is_scalar_literal(expression.expression)

    if isinstance(expression, exp.In):
        return _is_top_level_column(expression.this) and all(
            _is_scalar_literal(item) for item in expression.expressions
        )

    if isinstance(expression, exp.Is):
        return _is_top_level_column(expression.this) and isinstance(expression.expression, exp.Null)

    if isinstance(expression, exp.Not):
        return (
            isinstance(expression.this, exp.Is)
            and _is_top_level_column(expression.this.this)
            and isinstance(expression.this.expression, exp.Null)
        )

    return False


def _is_top_level_column(node: exp.Expr) -> bool:
    return isinstance(node, exp.Column) and len(node.parts) == 1


def _is_scalar_literal(node: exp.Expr) -> bool:
    node = _strip_parens(node)
    return isinstance(node, (exp.Boolean, exp.Literal, exp.Null))


def _strip_parens(node: exp.Expr) -> exp.Expr:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _compile_row_filter(row_filter: RowFilter | None) -> BooleanExpression:
    if row_filter is None:
        return ALWAYS_TRUE
    return _compile_expression(row_filter.expression)


def _compile_expression(expression: exp.Expr) -> BooleanExpression:
    expression = _strip_parens(expression)

    if isinstance(expression, exp.And):
        return And(
            _compile_expression(expression.this),
            _compile_expression(expression.expression),
        )
    if isinstance(expression, exp.Or):
        return Or(
            _compile_expression(expression.this),
            _compile_expression(expression.expression),
        )
    if isinstance(expression, exp.EQ):
        return cast(
            BooleanExpression,
            cast(Any, EqualTo)(
                term=_column_name(expression.this),
                value=_literal_value(expression.expression),
            ),
        )
    if isinstance(expression, exp.NEQ):
        return cast(
            BooleanExpression,
            cast(Any, NotEqualTo)(
                term=_column_name(expression.this),
                value=_literal_value(expression.expression),
            ),
        )
    if isinstance(expression, exp.GT):
        return cast(
            BooleanExpression,
            cast(Any, GreaterThan)(
                term=_column_name(expression.this),
                value=_literal_value(expression.expression),
            ),
        )
    if isinstance(expression, exp.GTE):
        return cast(
            BooleanExpression,
            cast(Any, GreaterThanOrEqual)(
                term=_column_name(expression.this),
                value=_literal_value(expression.expression),
            ),
        )
    if isinstance(expression, exp.LT):
        return cast(
            BooleanExpression,
            cast(Any, LessThan)(
                term=_column_name(expression.this),
                value=_literal_value(expression.expression),
            ),
        )
    if isinstance(expression, exp.LTE):
        return cast(
            BooleanExpression,
            cast(Any, LessThanOrEqual)(
                term=_column_name(expression.this),
                value=_literal_value(expression.expression),
            ),
        )
    if isinstance(expression, exp.In):
        return cast(
            BooleanExpression,
            cast(Any, In)(
                term=_column_name(expression.this),
                literals=[_literal_value(item) for item in expression.expressions],
            ),
        )
    if isinstance(expression, exp.Is):
        return cast(BooleanExpression, cast(Any, IsNull)(term=_column_name(expression.this)))
    if isinstance(expression, exp.Not) and isinstance(expression.this, exp.Is):
        return cast(BooleanExpression, cast(Any, NotNull)(term=_column_name(expression.this.this)))

    raise ValueError(f"Unsupported Iceberg pushdown expression: {expression.sql(dialect='duckdb')}")


def _column_name(node: exp.Expr) -> str:
    if not _is_top_level_column(node):
        raise ValueError("Iceberg pushdown requires top-level column references")

    column = cast(exp.Column, node)
    return column.name


def _literal_value(node: exp.Expr) -> object:
    node = _strip_parens(node)

    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Literal):
        return node.to_py()
    if isinstance(node, exp.Null):
        return None

    raise ValueError("Iceberg pushdown requires scalar literal values")
