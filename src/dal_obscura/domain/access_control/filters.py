from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

import pyarrow as pa
from sqlglot import exp, parse
from sqlglot.errors import ParseError


@dataclass(frozen=True)
class RowFilter:
    sql: str
    expression: exp.Expr = field(repr=False, compare=False)


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


def combine_row_filters(*row_filters: RowFilter | None) -> RowFilter | None:
    """Combines multiple row filters with `AND` while preserving SQL semantics."""
    active_filters = [row_filter for row_filter in row_filters if row_filter is not None]
    if not active_filters:
        return None
    if len(active_filters) == 1:
        return active_filters[0]

    expression = active_filters[0].expression.copy()
    for row_filter in active_filters[1:]:
        expression = exp.and_(expression, row_filter.expression.copy())
    return _build_row_filter(expression)


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


def _build_row_filter(expression: exp.Expr) -> RowFilter:
    _validate_filter_root(expression)
    _validate_filter_shape(expression)
    normalized_sql = expression.sql(dialect="duckdb")
    normalized_expression = _parse_filter_expression(normalized_sql)
    _validate_filter_shape(normalized_expression)
    return RowFilter(sql=normalized_sql, expression=normalized_expression)


def _parse_filter_expression(expression: str) -> exp.Expr:
    try:
        parsed_expressions = [
            item for item in parse(expression, dialect="duckdb") if item is not None
        ]
    except ParseError as exc:
        raise ValueError(f"Invalid row filter syntax: {exc}") from exc

    if len(parsed_expressions) != 1:
        raise ValueError("Row filter must contain a single DuckDB expression")

    return parsed_expressions[0]


def _validate_filter_root(expression: exp.Expr) -> None:
    if isinstance(expression, exp.Query):
        raise ValueError("Row filter must be a DuckDB expression, not a query statement")


_COMPARISON_TYPES = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)
_ARITHMETIC_TYPES = (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)


def _validate_filter_shape(expression: exp.Expr) -> None:
    expression = _strip_parens(expression)

    if isinstance(expression, exp.Column):
        return

    if isinstance(expression, (exp.And, exp.Or)):
        _validate_filter_shape(cast(exp.Expr, expression.this))
        _validate_filter_shape(cast(exp.Expr, expression.expression))
        return

    if isinstance(expression, exp.Not):
        _validate_filter_shape(cast(exp.Expr, expression.this))
        return

    if isinstance(expression, _COMPARISON_TYPES):
        _validate_scalar_expression(cast(exp.Expr, expression.this))
        _validate_scalar_expression(cast(exp.Expr, expression.expression))
        return

    if isinstance(expression, exp.In):
        if expression.args.get("query") is not None:
            raise _unsupported_filter_expression(expression)
        _validate_scalar_expression(cast(exp.Expr, expression.this))
        for item in expression.expressions:
            _validate_scalar_expression(cast(exp.Expr, item))
        return

    if isinstance(expression, exp.Is):
        _validate_scalar_expression(cast(exp.Expr, expression.this))
        if not isinstance(_strip_parens(cast(exp.Expr, expression.expression)), exp.Null):
            raise _unsupported_filter_expression(expression)
        return

    raise _unsupported_filter_expression(expression)


def _validate_scalar_expression(expression: exp.Expr) -> None:
    expression = _strip_parens(expression)

    if isinstance(expression, (exp.Column, exp.Literal, exp.Boolean, exp.Null)):
        return

    if isinstance(expression, _ARITHMETIC_TYPES):
        _validate_scalar_expression(cast(exp.Expr, expression.this))
        _validate_scalar_expression(cast(exp.Expr, expression.expression))
        return

    if isinstance(expression, exp.Neg):
        _validate_scalar_expression(cast(exp.Expr, expression.this))
        return

    if isinstance(expression, exp.Lower):
        _validate_single_scalar_argument(expression)
        return

    if isinstance(expression, exp.Coalesce):
        _validate_scalar_expression(cast(exp.Expr, expression.this))
        for item in expression.expressions:
            _validate_scalar_expression(cast(exp.Expr, item))
        return

    if isinstance(expression, exp.Cast):
        _validate_scalar_expression(cast(exp.Expr, expression.this))
        return

    raise _unsupported_filter_expression(expression)


def _validate_single_scalar_argument(expression: exp.Expr) -> None:
    argument = expression.this
    if not isinstance(argument, exp.Expr):
        raise _unsupported_filter_expression(expression)
    _validate_scalar_expression(argument)


def _unsupported_filter_expression(expression: exp.Expr) -> ValueError:
    return ValueError("Unsupported row filter expression: " + expression.sql(dialect="duckdb"))


def _validate_column_references(expression: exp.Expr, schema: pa.Schema) -> None:
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


def _strip_parens(node: exp.Expr) -> exp.Expr:
    while isinstance(node, exp.Paren):
        node = cast(exp.Expr, node.this)
    return node
