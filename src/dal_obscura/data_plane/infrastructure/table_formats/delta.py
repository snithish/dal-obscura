from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from dal_obscura.common.access_control.filters import RowFilter, row_filter_to_sql
from dal_obscura.common.catalog.ports import TableFormat
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.data_plane.infrastructure.adapters.path_rules import PathRuleEnforcer
from dal_obscura.data_plane.infrastructure.table_formats.filters import (
    row_filter_to_arrow_expression,
)


@dataclass(frozen=True, kw_only=True)
class DeltaInputPartition(InputPartition):
    columns: list[str]
    paths: list[str]
    version: int | None = None
    row_filter_sql: str | None = None


@dataclass(frozen=True, kw_only=True)
class DeltaTableFormat(TableFormat):
    format: str = "delta"
    table_uri: str
    storage_options: dict[str, str] = field(default_factory=dict)
    path_enforcer: PathRuleEnforcer | None = None

    def get_schema(self) -> pa.Schema:
        table = self._load_table()
        schema = table.schema().to_arrow()
        if isinstance(schema, pa.Schema):
            return schema
        return pa.schema(schema)

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        table = self._load_table()
        version = table.version()
        schema = self.get_schema()
        execution_columns = _execution_columns(request.columns)
        arrow_filter = row_filter_to_arrow_expression(request.row_filter)
        dataset = table.to_pyarrow_dataset(schema=schema)
        fragments = list(dataset.get_fragments(filter=arrow_filter))
        _check_fragment_paths(self.table_uri, fragments, self.path_enforcer)
        groups = _groups(fragments, max_tickets) or [[]]
        tasks = [
            ScanTask(
                table_format=self,
                schema=schema,
                partition=DeltaInputPartition(
                    columns=execution_columns,
                    paths=[fragment.path for fragment in group],
                    version=version,
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
        if not isinstance(partition, DeltaInputPartition):
            raise TypeError("DeltaTableFormat requires a DeltaInputPartition")
        table = self._load_table(version=partition.version)
        dataset = table.to_pyarrow_dataset()
        row_filter = _partition_filter(partition.row_filter_sql, dataset.schema)
        selected_paths = set(partition.paths)
        fragment_tables = [
            _fragment_table(fragment, dataset.schema, partition.columns)
            for fragment in dataset.get_fragments(filter=row_filter)
            if fragment.path in selected_paths
        ]
        arrow_table = (
            pa.concat_tables(fragment_tables, promote_options="default")
            if fragment_tables
            else pa.Table.from_batches([], schema=dataset.schema)
        )
        if row_filter is not None and arrow_table.num_rows:
            import pyarrow.dataset as ds

            arrow_table = ds.dataset(arrow_table).scanner(filter=row_filter).to_table()
        if partition.columns:
            arrow_table = arrow_table.select(partition.columns)
        projected_schema = dataset.schema
        if partition.columns:
            projected_schema = pa.schema(
                [projected_schema.field(column) for column in partition.columns]
            )
        return projected_schema, arrow_table.to_batches()

    def _load_table(self, *, version: int | None = None) -> Any:
        _check_path(self.table_uri, self.path_enforcer)
        from deltalake import DeltaTable

        return DeltaTable(
            self.table_uri,
            version=version,
            storage_options=dict(self.storage_options),
        )


def _check_path(path: str, enforcer: PathRuleEnforcer | None) -> None:
    if enforcer is not None:
        enforcer.check(path)


def _check_fragment_paths(
    table_uri: str,
    fragments: list[Any],
    enforcer: PathRuleEnforcer | None,
) -> None:
    if enforcer is None or not enforcer.enabled:
        return
    for fragment in fragments:
        enforcer.check(_data_file_path(table_uri, fragment.path))


def _data_file_path(table_uri: str, fragment_path: str) -> str:
    if "://" in fragment_path or fragment_path.startswith("/"):
        return fragment_path
    return f"{table_uri.rstrip('/')}/{fragment_path}"


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


def _filter_sql(row_filter: RowFilter | None, arrow_filter: object | None) -> str | None:
    if row_filter is None or arrow_filter is None:
        return None
    return row_filter_to_sql(row_filter)


def _partition_filter(filter_sql: str | None, schema: pa.Schema):
    if filter_sql is None:
        return None
    from dal_obscura.common.access_control.filters import parse_row_filter

    return row_filter_to_arrow_expression(parse_row_filter(filter_sql, schema))


def _fragment_table(fragment: Any, schema: pa.Schema, columns: list[str]) -> pa.Table:
    physical_names = set(fragment.physical_schema.names)
    physical_columns = [column for column in columns if column in physical_names]
    table = fragment.to_table(columns=physical_columns or None)
    partition_values = _partition_values(fragment.path)
    for name, value in partition_values.items():
        if name not in table.schema.names and name in schema.names:
            field = schema.field(name)
            coerced = _coerce_partition_value(value, field.type)
            table = table.append_column(
                field,
                pa.array([coerced] * table.num_rows, type=field.type),
            )
    return table


def _coerce_partition_value(value: str, data_type: pa.DataType) -> object:
    return pa.scalar(value).cast(data_type).as_py()


def _partition_values(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for part in path.split("/"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key:
            values[key] = value
    return values
