from __future__ import annotations

import heapq
import logging
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from functools import cached_property
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
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import ArrowScan, _read_all_delete_files
from pyiceberg.table import ALWAYS_TRUE, FileScanTask
from sqlglot import exp

from dal_obscura.policy.filters import (
    RowFilter,
    deserialize_row_filter,
    serialize_row_filter,
)
from dal_obscura.policy.paths import FieldSegment, parse_field_path
from dal_obscura.policy.schema_bounds import validate_arrow_schema_bounds
from dal_obscura.read.request import PlanRequest
from dal_obscura.sources.contracts import TableFormat
from dal_obscura.sources.iceberg_tasks import decode_scan_task, encode_scan_task
from dal_obscura.sources.paths import PathRuleEnforcer
from dal_obscura.sources.planning import InputPartition, Plan, ScanTask

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class IcebergInputPartition(InputPartition):
    """Concrete partition containing specific Iceberg file scan tasks."""

    columns: list[str]
    tasks: list[dict[str, object]]
    backend_pushdown_row_filter: str | None = None


@dataclass(frozen=True, kw_only=True)
class IcebergTableFormat(TableFormat):
    """Catalog-resolved Iceberg table that can self-plan and self-execute."""

    format: str = "iceberg"
    metadata_location: str
    io_options: dict[str, object]
    path_enforcer: PathRuleEnforcer | None = None

    def get_schema(self) -> pa.Schema:
        """Loads the Iceberg table schema used by planning and validation."""
        pyiceberg_table = self._load_table()
        schema = pyiceberg_table.schema().as_arrow()
        validate_arrow_schema_bounds(schema)
        return schema

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        """Plans file tasks and distributes them across the available tickets."""
        pyiceberg_table = self._load_table()

        column_tuple = tuple(_execution_columns(request.columns))
        base_schema = pyiceberg_table.schema().as_arrow()
        validate_arrow_schema_bounds(base_schema)
        pushdown_row_filter, residual_row_filter = _split_row_filter(request.row_filter)
        LOGGER.debug(
            "iceberg_filter_split",
            extra={
                "pushdown_row_filter_present": pushdown_row_filter is not None,
                "residual_row_filter_present": residual_row_filter is not None,
            },
        )
        iceberg_row_filter = _compile_row_filter(pushdown_row_filter)

        scan = pyiceberg_table.scan(
            row_filter=iceberg_row_filter,
            selected_fields=column_tuple,
        )

        file_tasks = list(scan.plan_files())
        _check_file_tasks(file_tasks, self.path_enforcer)
        groups = _chunk_by_max_tickets(file_tasks, max_tickets)
        if not groups:
            groups = [[]]

        tasks = [
            ScanTask(
                table_format=self,
                schema=base_schema,
                partition=IcebergInputPartition(
                    columns=list(column_tuple),
                    tasks=[encode_scan_task(task) for task in group],
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
        file_tasks = [decode_scan_task(task) for task in partition.tasks]
        _check_file_tasks(file_tasks, self.path_enforcer)
        pushdown_row_filter = (
            None
            if partition.backend_pushdown_row_filter is None
            else deserialize_row_filter(partition.backend_pushdown_row_filter)
        )

        projected_schema = table.schema()
        if column_tuple:
            projected_schema = projected_schema.select(*column_tuple)

        arrow_schema = projected_schema.as_arrow()
        validate_arrow_schema_bounds(arrow_schema)

        if not file_tasks:
            return arrow_schema, iter(())

        arrow_scan = ArrowScan(
            table_metadata=table.metadata,
            io=table.io,
            projected_schema=projected_schema,
            row_filter=_compile_row_filter(pushdown_row_filter),
        )
        return arrow_schema, _stream_iceberg_batches(arrow_scan, table.io, file_tasks)

    def _load_table(self):
        return self._loaded_table

    @cached_property
    def _loaded_table(self):
        """Loads the table from metadata location and enforces the supported
        Iceberg format versions."""
        from pyiceberg.table import StaticTable

        _check_path(self.metadata_location, self.path_enforcer)
        _check_io_options(self.io_options, self.path_enforcer)
        table = StaticTable.from_metadata(self.metadata_location, properties=self.io_options)
        _check_table_locations(table, self.path_enforcer)
        _require_supported_format_version(int(getattr(table.metadata, "format_version", 1)))
        return table


def _stream_iceberg_batches(
    scan: ArrowScan, io: FileIO, tasks: Iterable[FileScanTask]
) -> Iterator[pa.RecordBatch]:
    # PyIceberg 0.11's public to_record_batches materializes each entire file in
    # executor.map. Use its native projection/delete implementation lazily instead.
    # This private dependency is intentional; native-scan conformance tests must
    # pass on upgrades. Parallelism remains at the independently planned tickets.
    for task in tasks:
        deletes = _read_all_delete_files(io, [task])
        yield from scan._record_batches_from_scan_tasks_and_deletes([task], deletes)
        del deletes


def _require_supported_format_version(format_version: int) -> None:
    """Reject table formats without native scan conformance evidence.

    The current executor delegates file and delete handling to PyIceberg's
    ``ArrowScan``. Its v2 behavior is covered by this gateway's native-scan
    tests. Version 3 is deliberately rejected until the same evidence exists;
    accepting a metadata version alone would overstate support.
    """
    if format_version != 2:
        raise ValueError(f"Unsupported Iceberg format version: {format_version}")


def _chunk_by_max_tickets(tasks: list[FileScanTask], max_tickets: int) -> list[list[FileScanTask]]:
    """Assign largest estimated reads first, with deterministic ties.

    Data and delete-file bytes approximate scan work. This avoids concentrating
    large files in one ticket while keeping every task exactly once and its
    original relative order within a group. Tickets execute independently.
    """
    if not tasks:
        return []
    group_count = min(max(1, max_tickets), len(tasks))
    groups: list[list[tuple[int, FileScanTask]]] = [[] for _ in range(group_count)]
    loads = [(0, index) for index in range(group_count)]
    weighted = [
        (
            max(1, task.file.file_size_in_bytes)
            + sum(max(0, delete.file_size_in_bytes) for delete in task.delete_files),
            index,
            task,
        )
        for index, task in enumerate(tasks)
    ]
    for weight, ordinal, task in sorted(weighted, key=lambda item: (-item[0], item[1])):
        total, group = heapq.heappop(loads)
        groups[group].append((ordinal, task))
        heapq.heappush(loads, (total + weight, group))
    # Stable group order also keeps the one-file-per-ticket case in source order.
    return [
        [task for _ordinal, task in sorted(group)]
        for group in sorted(groups, key=lambda group: min(ordinal for ordinal, _task in group))
    ]


def _execution_columns(columns: Iterable[str]) -> list[str]:
    selected: list[str] = []
    seen: set[str] = set()
    for column in columns:
        top_level = (
            "*" if column == "*" else cast(FieldSegment, parse_field_path(column).segments[0]).name
        )
        if top_level in seen:
            continue
        seen.add(top_level)
        selected.append(top_level)
    return selected


def _check_file_tasks(tasks: Iterable[object], enforcer: PathRuleEnforcer | None) -> None:
    if enforcer is None or not enforcer.enabled:
        return
    for task in tasks:
        paths = _file_task_paths(task)
        if not paths:
            raise PermissionError("Path is not allowed")
        for path in paths:
            enforcer.check(path)


def _check_path(path: str, enforcer: PathRuleEnforcer | None) -> None:
    if enforcer is None:
        return
    enforcer.check(path)


def _file_task_paths(task: object) -> list[str]:
    """Return all data and delete-file locations carried by a scan task.

    PyIceberg's ``FileScanTask`` contains one data file plus zero or more
    delete files.  Checking only the data file would allow a provider to make
    the executor fetch a delete file from an unapproved bucket.
    """
    paths: list[str] = []
    for attribute_path in (
        ("file", "file_path"),
        ("data_file", "file_path"),
        ("file_path",),
        ("path",),
    ):
        value = task
        for attribute in attribute_path:
            value = getattr(value, attribute, None)
            if value is None:
                break
        if value is not None:
            paths.append(str(value))
            break

    delete_files = getattr(task, "delete_files", ())
    if isinstance(delete_files, (set, frozenset, list, tuple)):
        for delete_file in delete_files:
            path = getattr(delete_file, "file_path", None)
            if path is not None:
                paths.append(str(path))
    return paths


def _check_table_locations(table: object, enforcer: PathRuleEnforcer | None) -> None:
    """Check metadata, manifest-list, and historical metadata locations.

    These locations are fetched by PyIceberg before/while it plans files and
    are not represented by ``FileScanTask``.  Enforcing them at table-load
    time closes that gap while keeping the path policy in one adapter.
    """
    if enforcer is None or not enforcer.enabled:
        return
    metadata = getattr(table, "metadata", None)
    candidates: list[object] = [getattr(table, "metadata_location", None)]
    if metadata is not None:
        candidates.extend(
            [
                getattr(metadata, "location", None),
                *(
                    getattr(snapshot, "manifest_list", None)
                    for snapshot in (getattr(metadata, "snapshots", None) or ())
                ),
                *(
                    getattr(entry, "metadata_file", None)
                    for entry in (getattr(metadata, "metadata_log", None) or ())
                ),
                *(
                    getattr(entry, "statistics_path", None)
                    for entry in (getattr(metadata, "partition_statistics", None) or ())
                ),
            ]
        )
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            enforcer.check(candidate)


def _check_io_options(options: Mapping[str, object], enforcer: PathRuleEnforcer | None) -> None:
    """Check path-bearing provider options before PyIceberg opens them."""
    if enforcer is None or not enforcer.enabled:
        return
    for value in _nested_strings(options):
        if "://" in value or value.startswith(("/", "file:")):
            enforcer.check(value)


def _nested_strings(value: object) -> Iterable[str]:
    if isinstance(value, Mapping):
        for item in value.values():
            yield from _nested_strings(item)
    elif isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            yield from _nested_strings(item)
    elif isinstance(value, str):
        yield value


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
    # SQL comparisons/IN with NULL use three-valued logic; Iceberg literal
    # predicates cannot represent those semantics. Keep them in DuckDB.
    # IS NULL / IS NOT NULL have their own supported pushdown branches.
    return isinstance(node, (exp.Boolean, exp.Literal))


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
