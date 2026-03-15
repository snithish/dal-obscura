from __future__ import annotations

import pickle
from dataclasses import dataclass
from typing import Iterable

import duckdb
import pyarrow as pa

from dal_obscura.domain.query_planning import (
    DatasetSelector,
    Plan,
    ReadPayload,
    ReadSpec,
    ResolvedBackendTarget,
)

from .service_config import DEFAULT_SAMPLE_FILES, DEFAULT_SAMPLE_ROWS

_FILE_ARROW_BATCH_SIZE = 8_192


@dataclass(frozen=True)
class FileReadSpec:
    dataset: DatasetSelector
    format: str
    paths: tuple[str, ...]
    columns: list[str]
    options: dict[str, object]
    schema: pa.Schema


class DuckDBFileBackend:
    def get_schema(self, target: ResolvedBackendTarget) -> pa.Schema:
        file_format, paths, options = _file_config(target)
        con = duckdb.connect()
        try:
            relation = con.sql(_select_sql(file_format, paths, options, columns=None))
            return relation.limit(0).to_arrow_table().schema
        finally:
            con.close()

    def plan(self, target: ResolvedBackendTarget, columns: Iterable[str], max_tickets: int) -> Plan:
        file_format, paths, options = _file_config(target)
        column_list = list(columns)
        schema = self.get_schema(target)
        groups = _chunk_by_max_tickets(list(paths), max_tickets)
        tasks = [
            ReadPayload(
                payload=pickle.dumps(
                    FileReadSpec(
                        dataset=target.dataset_identity,
                        format=file_format,
                        paths=tuple(group),
                        columns=column_list,
                        options=options,
                        schema=schema,
                    )
                )
            )
            for group in groups
        ]
        return Plan(schema=schema, tasks=tasks)

    def read_spec(self, read_payload: bytes) -> ReadSpec:
        spec = _decode_spec(read_payload)
        return ReadSpec(dataset=spec.dataset, columns=list(spec.columns), schema=spec.schema)

    def read_stream(self, read_payload: bytes):
        spec = _decode_spec(read_payload)
        con = duckdb.connect()
        try:
            relation = con.sql(
                _select_sql(spec.format, spec.paths, spec.options, columns=spec.columns)
            )
            reader = relation.to_arrow_reader(batch_size=_FILE_ARROW_BATCH_SIZE)
            for batch in reader:
                yield batch
        finally:
            con.close()


def _decode_spec(read_payload: bytes) -> FileReadSpec:
    spec = pickle.loads(read_payload)
    if not isinstance(spec, FileReadSpec):
        raise ValueError("Invalid read payload for file backend")
    return spec


def _file_config(target: ResolvedBackendTarget) -> tuple[str, tuple[str, ...], dict[str, object]]:
    file_format = str(target.handle.get("format", "")).strip().lower()
    if file_format not in {"csv", "json", "parquet"}:
        raise ValueError("File backend requires format to be csv, json, or parquet")

    paths_raw = target.handle.get("paths", [])
    paths = tuple(str(path) for path in paths_raw)
    if not paths:
        raise ValueError("File backend requires explicit paths")

    options = dict(target.handle.get("options", {}) or {})
    return file_format, paths, options


def _select_sql(
    file_format: str,
    paths: tuple[str, ...],
    options: dict[str, object],
    columns: list[str] | None,
) -> str:
    scan_sql = _scan_sql(file_format, paths, options)
    if columns:
        select_list = ", ".join(f'"{column.replace(chr(34), chr(34) * 2)}"' for column in columns)
    else:
        select_list = "*"
    return f"SELECT {select_list} FROM {scan_sql}"


def _scan_sql(file_format: str, paths: tuple[str, ...], options: dict[str, object]) -> str:
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
    return "'" + value.replace("'", "''") + "'"


def _int_option(options: dict[str, object], key: str, default: int) -> int:
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
    if not paths:
        return [[]]
    max_tickets = max(1, max_tickets)
    group_count = min(max_tickets, len(paths))
    groups: list[list[str]] = [[] for _ in range(group_count)]
    for index, path in enumerate(paths):
        groups[index % group_count].append(path)
    return groups
