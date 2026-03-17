from __future__ import annotations

import pickle
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import duckdb
import pyarrow as pa

from dal_obscura.domain.query_planning.models import (
    BackendBinding,
    BackendDescriptor,
    BoundBackendTarget,
    DatasetSelector,
    Plan,
    ReadPayload,
    ReadSpec,
)
from dal_obscura.infrastructure.adapters.implementation_base import BackendImplementation
from dal_obscura.infrastructure.adapters.service_config import (
    DEFAULT_SAMPLE_FILES,
    DEFAULT_SAMPLE_ROWS,
)

_FILE_ARROW_BATCH_SIZE = 8_192


@dataclass(frozen=True)
class FileTableDescriptor:
    """Resolved descriptor for datasets read through the DuckDB file backend."""

    dataset_identity: DatasetSelector
    format: str
    paths: tuple[str, ...]
    options: dict[str, object]
    backend_id: str = field(init=False, default="duckdb_file")


@dataclass(frozen=True)
class FileBinding:
    """DuckDB file binding materialized from a resolved descriptor."""

    format: str
    paths: tuple[str, ...]
    options: dict[str, object]
    backend_id: str = field(init=False, default="duckdb_file")


@dataclass(frozen=True)
class FileReadSpec:
    """Serialized file-scan instructions embedded inside a signed ticket."""

    dataset: DatasetSelector
    format: str
    paths: tuple[str, ...]
    columns: list[str]
    options: dict[str, object]
    schema: pa.Schema


class DuckDBFileBackend(BackendImplementation):
    """Backend that reads CSV, JSON, and Parquet files through DuckDB."""

    def bind(self, descriptor: BackendDescriptor) -> BackendBinding:
        """Validates a resolved descriptor and materializes the file binding."""
        if not isinstance(descriptor, FileTableDescriptor):
            raise ValueError("File backend requires a FileTableDescriptor")
        return FileBinding(
            format=descriptor.format,
            paths=tuple(descriptor.paths),
            options=dict(descriptor.options),
        )

    def get_schema(self, bound_target: BoundBackendTarget) -> pa.Schema:
        """Infers the output schema without reading the full dataset."""
        file_format, paths, options = _file_config(_file_binding(bound_target.binding))
        con = duckdb.connect()
        try:
            relation = con.sql(_select_sql(file_format, paths, options, columns=None))
            return relation.limit(0).to_arrow_table().schema
        finally:
            con.close()

    def plan(
        self, bound_target: BoundBackendTarget, columns: Iterable[str], max_tickets: int
    ) -> Plan:
        """Splits the resolved file set into balanced ticket payloads."""
        file_format, paths, options = _file_config(_file_binding(bound_target.binding))
        column_list = list(columns)
        schema = self.get_schema(bound_target)
        groups = _chunk_by_max_tickets(list(paths), max_tickets)
        tasks = [
            ReadPayload(
                payload=pickle.dumps(
                    FileReadSpec(
                        dataset=bound_target.dataset_identity,
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
        """Reads metadata from a file ticket without opening the files."""
        spec = _decode_spec(read_payload)
        return ReadSpec(dataset=spec.dataset, columns=list(spec.columns), schema=spec.schema)

    def read_stream(self, read_payload: bytes) -> Iterable[Any]:
        """Streams the ticket's subset of files as Arrow record batches."""
        spec = _decode_spec(read_payload)
        con = duckdb.connect()
        try:
            relation = con.sql(
                _select_sql(spec.format, spec.paths, spec.options, columns=spec.columns)
            )
            reader = relation.to_arrow_reader(batch_size=_FILE_ARROW_BATCH_SIZE)
            yield from reader
        finally:
            con.close()


def _decode_spec(read_payload: bytes) -> FileReadSpec:
    """Deserializes and validates the file read specification."""
    spec = pickle.loads(read_payload)
    if not isinstance(spec, FileReadSpec):
        raise ValueError("Invalid read payload for file backend")
    return spec


def _file_binding(binding: object) -> FileBinding:
    """Downcasts the backend binding to the file-specific shape."""
    if not isinstance(binding, FileBinding):
        raise ValueError("File backend received a non-file binding")
    return binding


def _file_config(binding: FileBinding) -> tuple[str, tuple[str, ...], dict[str, object]]:
    """Normalizes the backend binding into concrete file scan parameters."""
    file_format = str(binding.format).strip().lower()
    if file_format not in {"csv", "json", "parquet"}:
        raise ValueError("File backend requires format to be csv, json, or parquet")

    paths = tuple(str(path) for path in binding.paths)
    if not paths:
        raise ValueError("File backend requires explicit paths")

    options = dict(binding.options or {})
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
