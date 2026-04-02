from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast

import pyarrow as pa

ScalarValue = str | int | float | bool | None
ComparisonOperator = Literal["=", "!=", "<", "<=", ">", ">="]
BooleanOperator = Literal["and", "or"]


@dataclass(frozen=True)
class FieldReference:
    path: str


@dataclass(frozen=True)
class LiteralValue:
    value: ScalarValue


@dataclass(frozen=True)
class ComparisonFilter:
    field: FieldReference
    operator: ComparisonOperator
    value: LiteralValue


@dataclass(frozen=True)
class InFilter:
    field: FieldReference
    values: tuple[LiteralValue, ...]


@dataclass(frozen=True)
class NullFilter:
    field: FieldReference
    is_null: bool


@dataclass(frozen=True)
class BooleanFilter:
    operator: BooleanOperator
    clauses: tuple[RowFilter, ...]


RowFilter = ComparisonFilter | InFilter | NullFilter | BooleanFilter
RowFilterPayload = dict[str, object]


def parse_row_filter(expression: str, schema: pa.Schema) -> RowFilter:
    """Parses and validates a policy row filter against the dataset schema."""
    parser = _Parser(expression)
    parsed = parser.parse()
    _validate_row_filter(parsed, schema)
    return parsed


def row_filter_to_sql(row_filter: RowFilter) -> str:
    """Renders a validated row filter into DuckDB-compatible SQL."""
    if isinstance(row_filter, ComparisonFilter):
        return (
            f"{row_filter.field.path} {row_filter.operator} "
            f"{_literal_to_sql(row_filter.value.value)}"
        )
    if isinstance(row_filter, InFilter):
        values = ", ".join(_literal_to_sql(value.value) for value in row_filter.values)
        return f"{row_filter.field.path} IN ({values})"
    if isinstance(row_filter, NullFilter):
        suffix = "NULL" if row_filter.is_null else "NOT NULL"
        return f"{row_filter.field.path} IS {suffix}"
    return f" {row_filter.operator.upper()} ".join(
        f"({row_filter_to_sql(clause)})" for clause in row_filter.clauses
    )


def extract_row_filter_dependencies(row_filter: RowFilter) -> list[str]:
    """Returns referenced field paths in stable first-seen order."""
    ordered: list[str] = []
    seen: set[str] = set()

    def visit(node: RowFilter) -> None:
        if isinstance(node, BooleanFilter):
            for clause in node.clauses:
                visit(clause)
            return
        path = node.field.path
        if path not in seen:
            seen.add(path)
            ordered.append(path)

    visit(row_filter)
    return ordered


def serialize_row_filter(row_filter: RowFilter) -> RowFilterPayload:
    """Converts a validated row filter into a JSON-friendly payload."""
    if isinstance(row_filter, ComparisonFilter):
        return {
            "type": "comparison",
            "field": row_filter.field.path,
            "operator": row_filter.operator,
            "value": row_filter.value.value,
        }
    if isinstance(row_filter, InFilter):
        return {
            "type": "in",
            "field": row_filter.field.path,
            "values": [value.value for value in row_filter.values],
        }
    if isinstance(row_filter, NullFilter):
        return {
            "type": "null",
            "field": row_filter.field.path,
            "is_null": row_filter.is_null,
        }
    return {
        "type": "boolean",
        "operator": row_filter.operator,
        "clauses": [serialize_row_filter(clause) for clause in row_filter.clauses],
    }


def deserialize_row_filter(payload: object) -> RowFilter:
    """Restores a validated row filter from a ticket payload."""
    if not isinstance(payload, Mapping):
        raise ValueError("Invalid row filter payload in ticket")
    payload_mapping = cast(Mapping[str, object], payload)

    node_type = payload_mapping.get("type")
    if node_type == "comparison":
        operator = str(payload_mapping.get("operator"))
        if operator not in {"=", "!=", "<", "<=", ">", ">="}:
            raise ValueError("Invalid row filter payload in ticket")
        return ComparisonFilter(
            field=FieldReference(path=_read_field_path(payload_mapping)),
            operator=cast(ComparisonOperator, operator),
            value=LiteralValue(value=_read_scalar(payload_mapping.get("value"))),
        )
    if node_type == "in":
        raw_values = payload_mapping.get("values")
        if not isinstance(raw_values, list) or not raw_values:
            raise ValueError("Invalid row filter payload in ticket")
        return InFilter(
            field=FieldReference(path=_read_field_path(payload_mapping)),
            values=tuple(LiteralValue(value=_read_scalar(value)) for value in raw_values),
        )
    if node_type == "null":
        raw_is_null = payload_mapping.get("is_null")
        if not isinstance(raw_is_null, bool):
            raise ValueError("Invalid row filter payload in ticket")
        return NullFilter(
            field=FieldReference(path=_read_field_path(payload_mapping)),
            is_null=raw_is_null,
        )
    if node_type == "boolean":
        operator = str(payload_mapping.get("operator"))
        raw_clauses = payload_mapping.get("clauses")
        if operator not in {"and", "or"} or not isinstance(raw_clauses, list) or not raw_clauses:
            raise ValueError("Invalid row filter payload in ticket")
        return BooleanFilter(
            operator=cast(BooleanOperator, operator),
            clauses=tuple(deserialize_row_filter(clause) for clause in raw_clauses),
        )
    raise ValueError("Invalid row filter payload in ticket")


def _validate_row_filter(row_filter: RowFilter, schema: pa.Schema) -> None:
    if isinstance(row_filter, BooleanFilter):
        for clause in row_filter.clauses:
            _validate_row_filter(clause, schema)
        return
    if not _schema_has_path(schema, row_filter.field.path):
        raise ValueError(f"Unknown column in row filter: {row_filter.field.path}")


def _schema_has_path(schema: pa.Schema, column: str) -> bool:
    parts = column.split(".")
    current_type: pa.DataType | None = None

    for index, part in enumerate(parts):
        if index == 0:
            try:
                current_type = schema.field(part).type
            except KeyError:
                return False
            continue

        if current_type is None or not pa.types.is_struct(current_type):
            return False
        try:
            current_type = current_type.field(part).type
        except KeyError:
            return False

    return True


def _literal_to_sql(value: ScalarValue) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _read_field_path(payload: Mapping[str, object]) -> str:
    field = payload.get("field")
    if not isinstance(field, str) or not field:
        raise ValueError("Invalid row filter payload in ticket")
    return field


def _read_scalar(value: object) -> ScalarValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ValueError("Invalid row filter payload in ticket")


@dataclass(frozen=True)
class _Token:
    kind: str
    value: str


class _Parser:
    def __init__(self, expression: str) -> None:
        self._expression = expression
        self._tokens = self._tokenize(expression)
        self._index = 0

    def parse(self) -> RowFilter:
        expression = self._parse_or()
        if self._peek().kind != "eof":
            raise ValueError(f"Unsupported row filter syntax near: {self._peek().value}")
        return expression

    def _parse_or(self) -> RowFilter:
        clauses = [self._parse_and()]
        while self._match_keyword("OR"):
            clauses.append(self._parse_and())
        if len(clauses) == 1:
            return clauses[0]
        return BooleanFilter(operator="or", clauses=tuple(clauses))

    def _parse_and(self) -> RowFilter:
        clauses = [self._parse_primary()]
        while self._match_keyword("AND"):
            clauses.append(self._parse_primary())
        if len(clauses) == 1:
            return clauses[0]
        return BooleanFilter(operator="and", clauses=tuple(clauses))

    def _parse_primary(self) -> RowFilter:
        if self._match_kind("lparen"):
            expression = self._parse_or()
            self._expect("rparen", ")")
            return expression
        return self._parse_predicate()

    def _parse_predicate(self) -> RowFilter:
        field = FieldReference(path=self._expect("identifier", "field reference").value)
        if self._match_keyword("IS"):
            is_not = self._match_keyword("NOT")
            self._expect_keyword("NULL")
            return NullFilter(field=field, is_null=not is_not)
        if self._match_keyword("IN"):
            self._expect("lparen", "(")
            values = [LiteralValue(value=self._parse_literal())]
            while self._match_kind("comma"):
                values.append(LiteralValue(value=self._parse_literal()))
            self._expect("rparen", ")")
            return InFilter(field=field, values=tuple(values))

        operator = self._expect("operator", "comparison operator").value
        if operator not in {"=", "!=", "<", "<=", ">", ">="}:
            raise ValueError(f"Unsupported operator in row filter: {operator}")
        return ComparisonFilter(
            field=field,
            operator=cast(ComparisonOperator, operator),
            value=LiteralValue(value=self._parse_literal()),
        )

    def _parse_literal(self) -> ScalarValue:
        token = self._advance()
        if token.kind == "string":
            return token.value[1:-1].replace("''", "'")
        if token.kind == "number":
            return float(token.value) if "." in token.value else int(token.value)
        if token.kind == "keyword":
            if token.value == "TRUE":
                return True
            if token.value == "FALSE":
                return False
            if token.value == "NULL":
                return None
        raise ValueError(f"Expected literal in row filter near: {token.value}")

    def _match_keyword(self, value: str) -> bool:
        token = self._peek()
        if token.kind == "keyword" and token.value == value:
            self._index += 1
            return True
        return False

    def _expect_keyword(self, value: str) -> _Token:
        token = self._peek()
        if token.kind == "keyword" and token.value == value:
            self._index += 1
            return token
        raise ValueError(f"Expected {value} in row filter near: {token.value}")

    def _match_kind(self, kind: str) -> bool:
        if self._peek().kind == kind:
            self._index += 1
            return True
        return False

    def _expect(self, kind: str, label: str) -> _Token:
        token = self._peek()
        if token.kind != kind:
            raise ValueError(f"Expected {label} in row filter near: {token.value}")
        self._index += 1
        return token

    def _peek(self) -> _Token:
        return self._tokens[self._index]

    def _advance(self) -> _Token:
        token = self._tokens[self._index]
        self._index += 1
        return token

    def _tokenize(self, expression: str) -> list[_Token]:
        tokens: list[_Token] = []
        index = 0
        while index < len(expression):
            char = expression[index]
            if char.isspace():
                index += 1
                continue
            if char == "(":
                tokens.append(_Token("lparen", char))
                index += 1
                continue
            if char == ")":
                tokens.append(_Token("rparen", char))
                index += 1
                continue
            if char == ",":
                tokens.append(_Token("comma", char))
                index += 1
                continue
            operator = _scan_operator(expression, index)
            if operator is not None:
                tokens.append(_Token("operator", operator))
                index += len(operator)
                continue
            if char == "'":
                token, index = _scan_string(expression, index)
                tokens.append(token)
                continue
            if char.isdigit():
                token, index = _scan_number(expression, index)
                tokens.append(token)
                continue
            if char.isalpha() or char == "_":
                token, index = _scan_identifier_or_keyword(expression, index)
                tokens.append(token)
                continue
            raise ValueError(f"Unsupported row filter syntax near: {char}")

        tokens.append(_Token("eof", "EOF"))
        return tokens


def _scan_operator(expression: str, index: int) -> str | None:
    if expression.startswith((">=", "<=", "!="), index):
        return expression[index : index + 2]
    if expression[index] in "=<>":
        return expression[index]
    return None


def _scan_string(expression: str, index: int) -> tuple[_Token, int]:
    end = index + 1
    while end < len(expression):
        if expression[end] == "'" and (end + 1 == len(expression) or expression[end + 1] != "'"):
            break
        if expression[end] == "'" and expression[end + 1] == "'":
            end += 2
            continue
        end += 1
    if end >= len(expression) or expression[end] != "'":
        raise ValueError("Unterminated string literal in row filter")
    return _Token("string", expression[index : end + 1]), end + 1


def _scan_number(expression: str, index: int) -> tuple[_Token, int]:
    end = index + 1
    while end < len(expression) and (expression[end].isdigit() or expression[end] == "."):
        end += 1
    return _Token("number", expression[index:end]), end


def _scan_identifier_or_keyword(expression: str, index: int) -> tuple[_Token, int]:
    end = index + 1
    while end < len(expression) and (expression[end].isalnum() or expression[end] in {"_", "."}):
        end += 1
    value = expression[index:end]
    upper_value = value.upper()
    if upper_value in {"AND", "OR", "IN", "IS", "NOT", "NULL", "TRUE", "FALSE"}:
        return _Token("keyword", upper_value), end
    return _Token("identifier", value), end
