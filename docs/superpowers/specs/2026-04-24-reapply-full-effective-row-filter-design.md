# Reapply Full Effective Row Filter Design

## Goal

Ensure the full effective row filter is always enforced in DuckDB during
`do_get`, even when some or all of that filter was already pushed down to the
backend.

Backend pushdown must remain a performance optimization only. It must not be
part of the final correctness or security boundary.

## Problem

The current planning flow combines policy and requested row filters into one
effective filter, then passes that filter into table-format planning.
`IcebergTableFormat` splits the effective filter into:

- a pushdown-safe subset for backend execution
- a residual subset for DuckDB execution

The bug is in how that split is carried across the ticket boundary.
`PlanAccessUseCase` currently stores only `plan.residual_row_filter` in the
ticket scan payload. During `do_get`, `FetchStreamUseCase` decodes that ticket
filter and passes it to `DuckDBRowTransformAdapter`.

As a result:

- a fully pushed filter is not re-checked by DuckDB
- a partially pushed filter is only partially enforced by DuckDB
- backend correctness becomes part of the security boundary

That is the wrong trust model. The signed ticket should carry the full filter
that the service intends to enforce, not only the portion that happened to
remain residual after backend planning.

## Non-Goals

- Changing the accepted row-filter language.
- Changing masking semantics or the visible output schema.
- Removing backend pushdown or making it more aggressive.
- Preserving backward compatibility for old ticket payloads or old plan field
  names.
- Introducing a larger filter-planning abstraction beyond what this bug needs.

## Decision

Make filter roles explicit in the planning model and keep the ticket minimal.

`Plan` should carry three filters:

- `full_row_filter`: the complete effective filter after combining policy and
  requested predicates
- `backend_pushdown_row_filter`: the subset the backend may apply as an
  optimization
- `residual_row_filter`: the subset not pushed down, kept for observability and
  backend planning diagnostics

The ticket scan payload should carry only `full_row_filter`.

At fetch time, DuckDB always evaluates `full_row_filter` over the backend
result stream before rows are returned to the client. Any backend pushdown
metadata is execution context only and is never trusted as the final
enforcement point.

## Model And Payload Changes

### Planning Model

`src/dal_obscura/domain/table_format/ports.py` should make `Plan` explicit:

- `full_row_filter: RowFilter | None`
- `backend_pushdown_row_filter: RowFilter | None`
- `residual_row_filter: RowFilter | None`

`PlanRequest.row_filter` remains the full effective filter on input to the
table-format planner.

This keeps the request boundary stable while making the planner output explicit
about what each filter means.

### Iceberg Partition Metadata

`src/dal_obscura/infrastructure/table_formats/iceberg.py` should rename the
partition field from `pushdown_row_filter` to `backend_pushdown_row_filter`.

That field continues to store only the backend-executable subset as serialized
SQL, because it is execution metadata for the backend adapter rather than the
security contract for `do_get`.

### Ticket Scan Payload

`src/dal_obscura/domain/ticket_delivery/models.py` should replace
`scan.row_filter` with `scan.full_row_filter`.

The ticket should not duplicate backend pushdown metadata. The ticket exists to
carry the signed fetch-time enforcement contract plus the scan task and masks.
For this change, that means:

- `read_payload`
- `full_row_filter`
- `masks`

`full_row_filter` remains optional and should be `null` when neither policy nor
requested filtering is present.

No compatibility alias is required because breaking changes are acceptable.

## Planning And Execution Flow

### PlanAccessUseCase

`src/dal_obscura/application/use_cases/plan_access.py` already computes:

```text
effective_row_filter = combine_row_filters(policy_row_filter, requested_row_filter)
```

That combined filter remains the single source of truth for correctness.

The flow should be:

1. validate the requested filter against the base schema
2. authorize requested columns and filter references
3. validate the policy filter against the base schema
4. combine policy and requested filters into `effective_row_filter`
5. build the execution projection from `effective_row_filter`
6. call `table_format.plan(...)` with `row_filter=effective_row_filter`
7. write `plan.full_row_filter` into the ticket scan payload as
   `scan.full_row_filter`

`PlanAccessUseCase` must no longer write only the residual filter into the
ticket.

### Execution Projection And Hidden Dependencies

Execution projection should continue to derive hidden dependency columns from
the full effective filter, not from the residual subset.

That preserves columns needed for DuckDB enforcement even when a predicate is
fully pushed down by the backend. A column used only by pushed predicates may
still be required in the streamed Arrow batches because DuckDB must re-evaluate
the full filter independently of backend behavior.

Visible client columns remain unchanged:

- execution columns include visible columns plus hidden filter dependencies
- output columns remain only the authorized visible columns

This preserves the current rule that hidden dependency columns are available for
execution but are never returned to the client unless explicitly visible and
requested.

### Table-Format Planning

`IcebergTableFormat.plan(...)` should continue splitting the full filter into
pushdown and residual subsets.

The returned `Plan` should contain:

- `full_row_filter`: the same effective filter received in the request
- `backend_pushdown_row_filter`: the pushdown-safe subset
- `residual_row_filter`: the non-pushdown subset

`IcebergInputPartition` should continue carrying only the backend pushdown
filter because that is the only filter the Iceberg executor needs directly.

### FetchStreamUseCase

`src/dal_obscura/application/use_cases/fetch_stream.py` should decode
`scan.full_row_filter` and pass that filter into
`RowTransformPort.apply_filters_and_masks_stream(...)`.

The fetch-time sequence should be:

1. verify the signed ticket
2. authenticate the caller
3. re-check policy version freshness
4. execute the backend task using partition-level
   `backend_pushdown_row_filter`
5. apply `full_row_filter` and masks in DuckDB
6. return only the visible masked columns

This makes DuckDB the final enforcement point for row filtering regardless of
what the backend claimed to enforce.

### DuckDBRowTransformAdapter

`src/dal_obscura/infrastructure/adapters/duckdb_transform.py` does not require
new semantics. It already accepts a single `RowFilter | None` and appends a
`WHERE` clause to the DuckDB query.

The important change is the caller contract:

- today it may receive only the residual filter
- after this change it must always receive the full effective filter

Masking behavior remains unchanged because filter evaluation still happens in
the same DuckDB query pipeline before the client-visible projection is returned.

## Behavioral Guarantees

After this change:

- a filter fully pushed to Iceberg is still enforced by DuckDB
- a partially pushed filter is enforced in full by DuckDB, not only the
  residual portion
- both policy filters and client filters are included in the full enforced
  filter
- hidden dependency columns needed for filter evaluation remain available to
  DuckDB
- hidden dependency columns are not exposed to the client
- existing masking behavior is unchanged

## Failure Modes

Planning should continue to fail when the combined filter or its dependencies
are invalid under the existing validation rules.

Fetch-time failures should remain straightforward:

- malformed non-string `scan.full_row_filter` fails ticket decoding
- invalid mask payloads fail ticket decoding
- invalid read payloads fail before backend execution

An absent or `null` `scan.full_row_filter` remains valid and means no row
filter is applied at fetch time.

There should be no fallback path that silently swaps back from full enforcement
to residual-only enforcement.

## Testing Strategy

The primary regression coverage should live in
`tests/application/use_cases/test_access_flow_use_cases.py`, because the bug is
in the handoff from planning to ticketing to fetch execution.

### Use Case Regression Tests

Add a backend double that can simulate backend pushdown behavior while still
returning violating rows.

Required cases:

1. Full pushdown regression:
   - effective filter: `region = 'us'`
   - planner/backend claim that predicate is pushed down
   - backend returns both matching and violating rows
   - fetch path returns only `region = 'us'` rows because DuckDB re-applies
     the full filter

2. Partial pushdown regression:
   - effective filter: `region = 'us' AND lower(status) = 'active'`
   - planner/backend push down only `region = 'us'`
   - backend returns rows violating either clause
   - fetch path returns only rows satisfying the full conjunction

Also add assertions that:

- the ticket stores `scan.full_row_filter`, not a residual-only filter
- `scan.full_row_filter` includes both policy and requested predicates when
  both are present
- fetch-time row transformation receives the full filter

### Planning Tests

Update planning-focused tests to assert that:

- `Plan.full_row_filter` is the combined effective filter
- `Plan.backend_pushdown_row_filter` matches the pushdown-safe subset
- `Plan.residual_row_filter` matches the non-pushdown subset
- execution columns still include dependencies referenced only by pushed
  predicates

### Iceberg Tests

Update `tests/infrastructure/adapters/test_iceberg_phase0_regressions.py` for
field renames and explicit plan semantics:

- partition field rename to `backend_pushdown_row_filter`
- returned `Plan` exposes full, pushdown, and residual filters separately
- execution still recompiles the backend pushdown filter correctly

### Masking And Interface Tests

Masking-specific tests should remain behaviorally unchanged. Only payload field
renames should need updates where tests inspect ticket contents directly.

Flight-level tests do not need new behavior beyond whatever payload contract
inspection is currently covered, because the security fix is exercised most
directly through the use-case path.

## Implementation Notes

- Prefer renaming `scan.row_filter` to `scan.full_row_filter` rather than
  keeping an ambiguous name.
- Prefer renaming Iceberg partition metadata to
  `backend_pushdown_row_filter` to make the optimization-only role obvious.
- Keep the ticket minimal: do not duplicate pushdown or residual filter fields
  into the scan payload unless a later observability need justifies it.

## Open Questions

None for this iteration. The design assumes breaking changes are acceptable and
there is no requirement to preserve compatibility with old tickets.
