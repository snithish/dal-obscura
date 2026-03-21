from __future__ import annotations

import pickle
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import duckdb
import pyarrow as pa

from dal_obscura.domain.catalog.ports import ResolvedTable
from dal_obscura.domain.format_handler.ports import FormatHandler, Plan, ScanTask
from dal_obscura.domain.query_planning.models import DatasetSelector, PlanRequest
from dal_obscura.infrastructure.adapters.service_config import (
    DEFAULT_SAMPLE_FILES,
    DEFAULT_SAMPLE_ROWS,
)

_FILE_ARROW_BATCH_SIZE = 8_192


@dataclass(frozen=True)
class FileReadSpec:
    """Serialized file-scan instructions embedded inside a signed ticket."""

    dataset: DatasetSelector
    format: str
    paths: tuple[str, ...]
    columns: list[str]
    options: dict[str, object]


class DuckDBHandler(FormatHandler):
    """Format handler that reads CSV, JSON, and Parquet files through DuckDB."""

    @property
    def supported_format(self) -> str:
        return "duckdb_file"

    def get_schema(self, table: ResolvedTable) -> pa.Schema:
        """Infers the output schema without reading the full dataset."""
        file_format, paths, options = _file_config(table)
        con = duckdb.connect()
        try:
            relation = con.sql(_select_sql(file_format, paths, options, columns=None))
            return relation.limit(0).to_arrow_table().schema
        finally:
            con.close()

    def plan(self, table: ResolvedTable, request: PlanRequest, max_tickets: int) -> Plan:
        """Splits the resolved file set into balanced ticket payloads."""
        file_format, paths, options = _file_config(table)
        column_list = list(request.columns)
        schema = self.get_schema(table)
        groups = _chunk_by_max_tickets(list(paths), max_tickets)

        dataset = DatasetSelector(catalog=request.catalog, target=request.target)

        tasks = [
            ScanTask(
                format=self.supported_format,
                schema=schema,
                payload=pickle.dumps(
                    FileReadSpec(
                        dataset=dataset,
                        format=file_format,
                        paths=tuple(group),
                        columns=column_list,
                        options=options,
                    )
                ),
            )
            for group in groups
        ]
        return Plan(schema=schema, tasks=tasks)

    def execute(self, payload: bytes) -> tuple[pa.Schema, Iterable[Any]]:
        """Streams the ticket's subset of files as Arrow record batches."""
        spec = _decode_spec(payload)
        con = duckdb.connect()
        relation = con.sql(_select_sql(spec.format, spec.paths, spec.options, columns=spec.columns))
        schema = relation.limit(0).to_arrow_table().schema

        def _stream():
            try:
                reader = relation.to_arrow_reader(batch_size=_FILE_ARROW_BATCH_SIZE)
                yield from reader
            finally:
                con.close()

        return schema, _stream()


def _decode_spec(payload: bytes) -> FileReadSpec:
    """Deserializes and validates the file read specification."""
    spec = pickle.loads(payload)
    if not isinstance(spec, FileReadSpec):
        raise ValueError("Invalid read payload for DuckDB format handler")
    return spec


def _file_config(table: ResolvedTable) -> tuple[str, tuple[str, ...], dict[str, object]]:
    """Normalizes the table object into concrete file scan parameters."""
    if not isinstance(table.table_object, dict):
        raise ValueError("DuckDB format handler requires table_object to be a configuration dict")

    config = table.table_object
    file_format = str(config.get("format", "")).strip().lower()
    if file_format not in {"csv", "json", "parquet"}:
        raise ValueError("DuckDB handler requires format to be csv, json, or parquet")

    paths = tuple(str(path) for path in config.get("paths", []))
    if not paths:
        raise ValueError("DuckDB handler requires explicit paths")

    options = dict(config.get("options", {}))
    return file_format, paths, options


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
