from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import duckdb
import pyarrow as pa

from dal_obscura.domain.catalog.ports import ResolvedTable
from dal_obscura.domain.format_handler.ports import FormatHandler, InputPartition, Plan, ScanTask
from dal_obscura.domain.query_planning.models import PlanRequest
from dal_obscura.infrastructure.adapters.service_config import (
    DEFAULT_SAMPLE_FILES,
    DEFAULT_SAMPLE_ROWS,
)

_FILE_ARROW_BATCH_SIZE = 8_192


@dataclass(frozen=True, kw_only=True)
class FileTable(ResolvedTable):
    file_format: str
    paths: tuple[str, ...]
    options: dict[str, object]


@dataclass(frozen=True, kw_only=True)
class FileInputPartition(InputPartition):
    table: FileTable
    columns: list[str]
    paths: tuple[str, ...]


class DuckDBHandler(FormatHandler):
    """Format handler that reads CSV, JSON, and Parquet files through DuckDB."""

    @property
    def supported_format(self) -> str:
        return "duckdb_file"

    def get_schema(self, table: ResolvedTable) -> pa.Schema:
        """Infers the output schema without reading the full dataset."""
        if not isinstance(table, FileTable):
            raise TypeError("DuckDBHandler requires a FileTable")
        con = duckdb.connect()
        try:
            relation = con.sql(
                _select_sql(table.file_format, table.paths, table.options, columns=None)
            )
            return relation.limit(0).to_arrow_table().schema
        finally:
            con.close()

    def plan(self, table: ResolvedTable, request: PlanRequest, max_tickets: int) -> Plan:
        """Splits the resolved file set into balanced ticket payloads."""
        if not isinstance(table, FileTable):
            raise TypeError("DuckDBHandler requires a FileTable")
        column_list = list(request.columns)
        schema = self.get_schema(table)
        groups = _chunk_by_max_tickets(list(table.paths), max_tickets)

        tasks = [
            ScanTask(
                format=self.supported_format,
                schema=schema,
                partition=FileInputPartition(
                    table=table,
                    columns=column_list,
                    paths=tuple(group),
                ),
            )
            for group in groups
        ]
        return Plan(schema=schema, tasks=tasks)

    def execute(self, partition: InputPartition) -> tuple[pa.Schema, Iterable[pa.RecordBatch]]:
        """Streams the ticket's subset of files as Arrow record batches."""
        if not isinstance(partition, FileInputPartition):
            raise TypeError("DuckDBHandler requires a FileInputPartition")

        con = duckdb.connect()
        relation = con.sql(
            _select_sql(
                partition.table.file_format,
                partition.paths,
                partition.table.options,
                columns=partition.columns,
            )
        )
        schema = relation.limit(0).to_arrow_table().schema

        def _stream():
            try:
                reader = relation.to_arrow_reader(batch_size=_FILE_ARROW_BATCH_SIZE)
                yield from reader
            finally:
                con.close()

        return schema, _stream()


def _select_sql(
    file_format: str,
    paths: tuple[str, ...],
    options: dict[str, object],
    columns: list[str] | None,
) -> str:
    """Builds the final projection query on top of the DuckDB scan function."""
    scan_sql = _scan_sql(file_format, paths, options)
    if columns:
        select_list = ", ".join(f'"{column.replace(chr(34), chr(34) * 2)}"' for column in columns)
    else:
        select_list = "*"
    return f"SELECT {select_list} FROM {scan_sql}"


def _scan_sql(file_format: str, paths: tuple[str, ...], options: dict[str, object]) -> str:
    """Returns the DuckDB table function for the file format being read."""
    paths_sql = "[" + ", ".join(_sql_literal(path) for path in paths) + "]"
    if file_format == "csv":
        return (
            "read_csv_auto("
            f"{paths_sql}, union_by_name=true, "
            f"sample_size={_int_option(options, 'sample_rows', DEFAULT_SAMPLE_ROWS)}, "
            f"files_to_sniff={_int_option(options, 'sample_files', DEFAULT_SAMPLE_FILES)}"
            ")"
        )
    if file_format == "json":
        return (
            "read_json_auto("
            f"{paths_sql}, union_by_name=true, "
            f"sample_size={_int_option(options, 'sample_rows', DEFAULT_SAMPLE_ROWS)}, "
            f"maximum_sample_files={_int_option(options, 'sample_files', DEFAULT_SAMPLE_FILES)}"
            ")"
        )
    if file_format == "parquet":
        return f"read_parquet({paths_sql}, union_by_name=true)"
    raise ValueError(f"Unsupported file format: {file_format}")


def _sql_literal(value: str) -> str:
    """Escapes a filesystem path for interpolation into DuckDB SQL."""
    return "'" + value.replace("'", "''") + "'"


def _int_option(options: dict[str, object], key: str, default: int) -> int:
    """Accepts numeric options even when config values arrive as strings or floats."""
    value = options.get(key, default)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value)
    return default


def _chunk_by_max_tickets(paths: list[str], max_tickets: int) -> list[list[str]]:
    """Balances file paths across tickets using simple round-robin assignment."""
    if not paths:
        return [[]]
    max_tickets = max(1, max_tickets)
    group_count = min(max_tickets, len(paths))
    groups: list[list[str]] = [[] for _ in range(group_count)]
    for index, path in enumerate(paths):
        groups[index % group_count].append(path)
    return groups
