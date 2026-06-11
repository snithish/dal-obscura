from __future__ import annotations

from typing import Any, cast

import pyarrow.dataset as ds
from sqlglot import exp

from dal_obscura.common.access_control.filters import RowFilter


def row_filter_to_arrow_expression(row_filter: RowFilter | None) -> ds.Expression | None:
    if row_filter is None:
        return None
    return _expression(row_filter.expression)


def _expression(expression: exp.Expr) -> ds.Expression | None:
    expression = _strip_expression_parens(expression)

    if isinstance(expression, _BOOLEAN_TYPES):
        return _boolean_expression(expression)

    if isinstance(expression, (exp.In, exp.Is)):
        return _set_or_null_expression(expression)

    if isinstance(expression, _COMPARISON_TYPES):
        return _comparison(expression)

    if isinstance(expression, exp.Column):
        return ds.field(_column_path(expression))

    return None


def _boolean_expression(expression: exp.Expr) -> ds.Expression | None:
    if isinstance(expression, exp.Not):
        inner = _expression(cast(exp.Expr, expression.this))
        return None if inner is None else ~inner

    left = _expression(cast(exp.Expr, expression.this))
    right = _expression(cast(exp.Expr, expression.expression))
    if left is None or right is None:
        return None
    if isinstance(expression, exp.And):
        return left & right
    return left | right


def _set_or_null_expression(expression: exp.Expr) -> ds.Expression | None:
    field = _field(expression.this)
    if field is None:
        return None

    if isinstance(expression, exp.In):
        if expression.args.get("query") is not None:
            return None
        values = [_literal(item) for item in expression.expressions]
        if any(value is _UNSUPPORTED for value in values):
            return None
        return field.isin(values)

    value = _strip_parens(cast(exp.Expr, expression.expression))
    if isinstance(value, exp.Null):
        return field.is_null()
    return None


def _comparison(expression: exp.Expr) -> ds.Expression | None:
    left_field = _field(expression.this)
    right_value = _literal(expression.expression)
    if left_field is not None and right_value is not _UNSUPPORTED:
        return _compare(expression, left_field, right_value)

    right_field = _field(expression.expression)
    left_value = _literal(expression.this)
    if right_field is not None and left_value is not _UNSUPPORTED:
        return _compare_reversed(expression, right_field, left_value)

    return None


def _compare(expression: exp.Expr, field: ds.Expression, value: object) -> ds.Expression | None:
    if isinstance(expression, exp.EQ):
        return field == value
    if isinstance(expression, exp.NEQ):
        return field != value
    if isinstance(expression, exp.GT):
        return field > value
    if isinstance(expression, exp.GTE):
        return field >= value
    if isinstance(expression, exp.LT):
        return field < value
    if isinstance(expression, exp.LTE):
        return field <= value
    return None


def _compare_reversed(
    expression: exp.Expr,
    field: ds.Expression,
    value: object,
) -> ds.Expression | None:
    if isinstance(expression, exp.EQ):
        return field == value
    if isinstance(expression, exp.NEQ):
        return field != value
    if isinstance(expression, exp.GT):
        return field < value
    if isinstance(expression, exp.GTE):
        return field <= value
    if isinstance(expression, exp.LT):
        return field > value
    if isinstance(expression, exp.LTE):
        return field >= value
    return None


def _field(value: object) -> ds.Expression | None:
    value = _strip_parens(value)
    if isinstance(value, exp.Column):
        return ds.field(_column_path(value))
    return None


def _literal(value: object) -> object:
    value = _strip_parens(value)
    if isinstance(value, exp.Literal):
        if value.is_string:
            return value.this
        try:
            if any(marker in str(value.this).lower() for marker in (".", "e")):
                return float(value.this)
            return int(value.this)
        except ValueError:
            return _UNSUPPORTED
    if isinstance(value, exp.Boolean):
        return bool(value.this)
    if isinstance(value, exp.Null):
        return None
    return _UNSUPPORTED


def _strip_parens(value: object) -> object:
    while isinstance(value, exp.Paren):
        value = value.this
    return value


def _strip_expression_parens(value: exp.Expr) -> exp.Expr:
    while isinstance(value, exp.Paren):
        value = cast(exp.Expr, value.this)
    return value


def _column_path(column: exp.Column) -> str:
    return ".".join(part.name for part in column.parts)


_BOOLEAN_TYPES = (exp.And, exp.Or, exp.Not)
_COMPARISON_TYPES = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)
_UNSUPPORTED: Any = object()
