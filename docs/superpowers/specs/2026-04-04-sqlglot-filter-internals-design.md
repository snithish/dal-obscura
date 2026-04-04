# SQLGlot Filter Internals Design

## Goal

Replace the handwritten row-filter parser and compiler internals with SQLGlot while preserving the existing high-level planning and execution flow:

- DuckDB remains the exact executor for residual row filtering.
- Iceberg continues to receive only a pushdown-safe subset of the filter.
- Policy validation still checks referenced columns against the Arrow schema.

Backward compatibility is not required. Ticket and partition payloads may change.

## Problem

The current implementation in `src/dal_obscura/domain/access_control/filters.py` maintains a custom filter AST, custom parser, custom serializer, and custom DuckDB SQL renderer. `src/dal_obscura/infrastructure/table_formats/iceberg.py` then compiles that custom AST again into PyIceberg expressions.

This has two problems:

1. The handwritten parser is fragile and expensive to maintain.
2. The codebase owns a filter language implementation that overlaps with what SQLGlot already provides.

The design goal is to remove the custom parser/compiler logic, adopt SQLGlot with the DuckDB dialect as the canonical filter representation, and keep backend-specific execution and pushdown decisions at the appropriate boundaries.

## Non-Goals

- Supporting arbitrary SQL functions or full DuckDB expression semantics for Iceberg pushdown.
- Preserving the current JSON tree payload shape used for serialized row filters.
- Refactoring masking behavior beyond the minimum integration changes required by the new filter representation.

## Decision

Use SQLGlot as the only internal AST for row filters.

- Filters are parsed with SQLGlot using the DuckDB dialect.
- The domain filter module owns validation, normalization, dependency extraction, and string serialization of SQLGlot expressions.
- DuckDB execution consumes normalized DuckDB SQL derived from SQLGlot.
- Iceberg pushdown transpiles SQLGlot expressions into PyIceberg `BooleanExpression` objects for the pushdown-safe subset only.
- Tickets and format partitions serialize row filters as normalized SQL strings rather than the current custom JSON tree.

## Architecture

### Canonical Filter Object

`RowFilter` becomes a lightweight wrapper around a validated SQLGlot expression and its normalized DuckDB SQL string. The object is canonical only after:

1. Parsing with SQLGlot.
2. Validating supported expression shapes.
3. Validating referenced columns against the Arrow schema.
4. Normalizing SQL for deterministic serialization.

This replaces the current `ComparisonFilter`, `InFilter`, `NullFilter`, and `BooleanFilter` dataclasses as the internal model.

Schema validation is required when parsing raw policy input during planning. Deserialization during ticket execution restores a previously validated canonical filter from signed payload data and therefore does not repeat schema-path validation.

### Domain Responsibilities

`src/dal_obscura/domain/access_control/filters.py` becomes the single place for generic filter operations:

- parse a filter string into a validated `RowFilter`
- render canonical DuckDB SQL
- extract referenced column paths in stable first-seen order
- serialize and deserialize filter payloads as strings
- walk SQLGlot expressions to validate allowed shapes
- walk SQLGlot column references to validate schema paths

This module must not include Iceberg-specific pushdown rules or DuckDB execution concerns beyond canonical SQL rendering.

### DuckDB Responsibilities

`src/dal_obscura/infrastructure/adapters/duckdb_transform.py` continues to build the final query used for masking and residual filtering. It receives `RowFilter | None` and appends `WHERE <normalized DuckDB SQL>` when a filter is present.

DuckDB remains the source of exact filtering semantics after planning.

### Iceberg Responsibilities

`src/dal_obscura/infrastructure/table_formats/iceberg.py` becomes responsible for:

- determining which SQLGlot predicates are pushdown-safe for Iceberg
- splitting a validated filter into pushdown and residual subtrees
- transpiling pushdown-safe SQLGlot predicates into PyIceberg `BooleanExpression`
- preserving the residual filter as a `RowFilter` for DuckDB execution

This keeps format-specific pushdown rules inside the format implementation.

## Filter Semantics

### Supported Filter Shapes

The supported policy/filter language remains intentionally narrow even though SQLGlot can parse more:

- comparison predicates on column references and scalar literals
- `AND` / `OR`
- `IN`
- `IS NULL` / `IS NOT NULL`
- parenthesized boolean composition
- top-level and nested dotted column references

Anything outside this supported subset must fail validation in the domain filter layer even if SQLGlot can parse it.

### Unsupported Shapes

The domain filter layer must reject:

- function calls
- arithmetic expressions
- subqueries
- unary boolean negation for now
- arbitrary computed expressions on either side of a predicate

This keeps the filter model aligned with current behavior and avoids ambiguous pushdown semantics.

## Data Flow

### Planning

1. `PlanAccessUseCase` receives the raw row-filter string from policy resolution.
2. The domain filter module parses and validates it into `RowFilter`.
3. Dependency extraction walks the SQLGlot tree to include hidden execution columns.
4. `PlanRequest` carries `RowFilter | None` into the table format.
5. `IcebergTableFormat.plan` splits the filter into:
   - pushdown-safe subtree for Iceberg file planning
   - residual subtree for DuckDB exact filtering
6. The returned `Plan` stores the residual filter.

### Ticket Minting

Ticket payloads serialize any residual filter as a normalized SQL string.

This replaces the current structured payload with a string representation and removes custom tree serialization/deserialization.

### Ticket Execution

1. `FetchStreamUseCase` deserializes the normalized SQL string into `RowFilter`.
2. The table format executes any pre-planned backend work using its format-specific partition payload.
3. `DuckDBRowTransformAdapter` applies the residual filter and masking using normalized DuckDB SQL.

## Iceberg String Filters

PyIceberg in the local environment accepts either `str` or `BooleanExpression` at `Table.scan`, but string inputs are parsed by PyIceberg's own expression parser rather than passed through unchanged.

That is not sufficient to make strings the main Iceberg interface because:

- `ArrowScan` still requires a `BooleanExpression`
- PyIceberg's parser is not the same as DuckDB SQL
- relying on string parsing at the Iceberg boundary would reintroduce a second parser and dialect drift

The implementation must therefore transpile SQLGlot expressions into PyIceberg expressions directly for the pushdown-safe subset.

## Serialization Changes

The following payloads change from structured filter trees to normalized SQL strings:

- ticket `scan.row_filter`
- `IcebergInputPartition.pushdown_row_filter`

This is acceptable because backward compatibility is explicitly out of scope.

## Interface Changes

### `domain/access_control/filters.py`

Expected public surface after refactor:

- `parse_row_filter(expression: str, schema: pa.Schema) -> RowFilter`
- `row_filter_to_sql(row_filter: RowFilter) -> str`
- `extract_row_filter_dependencies(row_filter: RowFilter) -> list[str]`
- `serialize_row_filter(row_filter: RowFilter) -> str`
- `deserialize_row_filter(payload: object) -> RowFilter`

`deserialize_row_filter` should accept the normalized SQL string payload and recreate a normalized `RowFilter` from signed ticket data without repeating schema-path validation.

### `domain/table_format/ports.py`

`Plan.residual_row_filter` continues to use `RowFilter | None`.

### `infrastructure/table_formats/iceberg.py`

`IcebergInputPartition.pushdown_row_filter` changes from `RowFilterPayload | None` to `str | None`.

## Validation Rules

Validation must remain strict and explicit:

- all referenced columns must exist in the Arrow schema
- nested dotted paths must resolve correctly through Arrow structs
- only the supported predicate shapes listed above are accepted
- serialization must be deterministic so tickets are stable

The implementation should validate all raw policy input before planning consumes a filter. Execution-time deserialization should re-parse and validate supported expression shape, but it may trust schema-path validation from the signed ticket issuance path.

## Testing Strategy

### Domain Tests

Update `tests/domain/access_control/test_row_filters.py` to cover:

- valid SQLGlot-backed parsing for the supported subset
- rejection of unsupported expressions such as functions and unary `NOT`
- stable dependency extraction from SQLGlot trees
- deterministic SQL serialization
- string-based serialization and deserialization round-trips

### Iceberg Tests

Update `tests/infrastructure/adapters/test_iceberg_phase0_regressions.py` to cover:

- pushdown-safe predicates compiling from SQLGlot to PyIceberg expressions
- mixed `AND` predicates splitting into pushdown and residual filters
- partition payloads storing normalized SQL strings
- execution recompiling string payloads into PyIceberg expressions

### DuckDB Tests

Update `tests/infrastructure/adapters/test_duckdb_transform.py` to ensure:

- DuckDB query generation still appends canonical `WHERE` SQL
- residual filters continue to behave correctly with masking

### Use Case Tests

Update `tests/application/use_cases/test_access_flow_use_cases.py` to cover:

- hidden dependency columns still being inferred from the SQLGlot-backed filter representation
- ticket payloads carrying string filters instead of structured trees

## Risks And Mitigations

### Risk: SQLGlot accepts more syntax than the system should support

Mitigation: add an explicit expression-shape validator after parsing and fail closed for anything outside the supported subset.

### Risk: SQL normalization changes string formatting in tests or payloads

Mitigation: treat SQLGlot-normalized SQL as canonical and update tests to assert against normalized output.

### Risk: Iceberg pushdown semantics drift from DuckDB semantics

Mitigation: keep pushdown conservative and always preserve a residual DuckDB filter whenever a predicate cannot be proven safe.

## Implementation Summary

The implementation should remove the handwritten filter parser/compiler internals, keep the public planning/execution flow intact, serialize filters as normalized SQL strings, and use SQLGlot as the single internal filter AST while Iceberg remains responsible for backend-specific pushdown transpilation.
