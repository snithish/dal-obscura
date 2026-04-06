from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

import pyarrow as pa
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError


@dataclass(frozen=True)
class RowFilter:
    sql: str
    expression: exp.Expression = field(repr=False, compare=False)


def parse_row_filter(expression: str, schema: pa.Schema) -> RowFilter:
    """Parses a DuckDB filter expression and validates referenced schema paths."""
    parsed = _parse_filter_expression(expression)
    _validate_filter_root(parsed)
    _validate_column_references(parsed, schema)
    return _build_row_filter(parsed)


def validate_row_filter_against_schema(row_filter: RowFilter, schema: pa.Schema) -> RowFilter:
    """Checks that a previously parsed row filter only references schema columns."""
    _validate_column_references(row_filter.expression, schema)
    return row_filter


def row_filter_to_sql(row_filter: RowFilter) -> str:
    """Renders the canonical DuckDB SQL string for a validated filter."""
    return row_filter.sql


def extract_row_filter_dependencies(row_filter: RowFilter) -> list[str]:
    """Returns referenced field paths in stable first-seen order."""
    ordered: list[str] = []
    seen: set[str] = set()

    for column in row_filter.expression.find_all(exp.Column):
        path = _column_path(column)
        if path and path not in seen:
            seen.add(path)
            ordered.append(path)

    return ordered


def serialize_row_filter(row_filter: RowFilter) -> str:
    """Converts a validated row filter into its canonical SQL string."""
    return row_filter.sql


def deserialize_row_filter(payload: object) -> RowFilter:
    """Restores a canonical row filter from a signed payload."""
    if not isinstance(payload, str) or not payload.strip():
        raise ValueError("Invalid row filter payload in ticket")

    parsed = _parse_filter_expression(payload)
    _validate_filter_root(parsed)
    return _build_row_filter(parsed)


def _build_row_filter(expression: exp.Expression) -> RowFilter:
    normalized_sql = expression.sql(dialect="duckdb")
    normalized_expression = cast(exp.Expression, parse_one(normalized_sql, dialect="duckdb"))
    return RowFilter(sql=normalized_sql, expression=normalized_expression)


def _parse_filter_expression(expression: str) -> exp.Expression:
    try:
        parsed = parse_one(expression, dialect="duckdb")
    except ParseError as exc:
        raise ValueError(f"Invalid row filter syntax: {exc}") from exc

    if parsed is None:
        raise ValueError("Invalid row filter syntax")

    return cast(exp.Expression, parsed)


def _validate_filter_root(expression: exp.Expression) -> None:
    if isinstance(expression, exp.Query):
        raise ValueError("Row filter must be a DuckDB expression, not a query statement")


def _validate_column_references(expression: exp.Expression, schema: pa.Schema) -> None:
    for column in expression.find_all(exp.Column):
        path = _column_path(column)
        if not _schema_has_path(schema, path):
            raise ValueError(f"Unknown column in row filter: {path}")


def _column_path(column: exp.Column) -> str:
    return ".".join(part.name for part in column.parts)


def _schema_has_path(schema: pa.Schema, column: str) -> bool:
    parts = column.split(".")
    try:
        field = schema.field(parts[0])
    except KeyError:
        return False

    for part in parts[1:]:
        data_type = field.type
        if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
            field = data_type.value_field
            data_type = field.type
        if not pa.types.is_struct(data_type):
            return False
        try:
            field = data_type.field(part)
        except KeyError:
            return False

    return True
