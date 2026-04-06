# Engine Filter Pushdown Design

## Goal

Allow query engines using this service to provide their own row filter during
`get_flight_info` so the service can:

- narrow results beyond policy-imposed filtering
- reduce the amount of data streamed back to the query engine
- push safe predicates into backend planning when the table format supports it

The service must preserve the current security model: policy remains the upper
bound on what data may be observed, and engine filters may only narrow results.

## Problem

Today the planning path only accepts policy-derived row filters.
`PlanAccessUseCase` validates and forwards a single filter from policy
resolution, and `IcebergTableFormat` can already split that filter into a
pushdown-safe portion and a residual DuckDB filter.

This leaves two gaps:

1. Query engines cannot express their own narrowing predicates to this service.
2. The service may stream more data than necessary because engine intent is not
   available during backend planning.

The feature should add engine-supplied predicates without weakening policy
enforcement or requiring a larger redesign of tickets and execution flow.

## Non-Goals

- Allowing clients to widen, replace, or bypass policy row filters.
- Introducing an engine-specific predicate format in the first version.
- Adding a separate "filterable but not selectable" permission model.
- Requiring full backend pushdown for a filter to be accepted.
- Redesigning ticket payloads beyond the minimum needed to carry the existing
  combined residual filter.

## Decision

Add an optional raw DuckDB SQL `row_filter` string to the Flight planning
request. The service validates that filter during `get_flight_info`, authorizes
its referenced columns against the policy decision, combines it with the policy
filter using `AND`, and then reuses the existing planning and execution flow.

The combined filter becomes the only runtime filter after authorization:

- planning receives one combined `RowFilter`
- Iceberg pushdown operates on the combined filter
- any unsupported clauses remain residual DuckDB filtering
- the ticket stores only the residual combined filter, not separate policy and
  engine filters

This keeps the client contract additive while minimizing internal model churn.

## Request Contract

### Flight Descriptor

`src/dal_obscura/interfaces/flight/contracts.py` should accept this shape in
the JSON command payload:

```json
{
  "catalog": "analytics",
  "target": "users",
  "columns": ["id", "region"],
  "row_filter": "region = 'us' AND active = true"
}
```

`row_filter` is optional. Requests that omit it keep the current behavior.

### Domain Model

`src/dal_obscura/domain/query_planning/models.py` should continue to use
`PlanRequest.row_filter`, but that field now represents the validated
engine-supplied request filter on entry to `PlanAccessUseCase`, and the combined
policy-plus-engine filter when passed into table-format planning.

Backward compatibility is preserved at the request boundary because the new
field is optional.

## Authorization And Validation

### Validation Order

`src/dal_obscura/application/use_cases/plan_access.py` should validate the
engine filter in this order:

1. Authenticate the caller.
2. Resolve the table format and load the base schema.
3. Expand and validate requested output columns.
4. Run authorization to obtain:
   - allowed visible columns
   - masks
   - policy row filter
5. Parse the engine `row_filter` against the base schema using the existing
   SQLGlot-backed filter validation path.
6. Extract the engine filter's referenced columns.
7. Reject the engine filter if any referenced column is not in the plainly
   exposed visible columns from policy.
8. Reject the engine filter if any referenced column has a mask applied.
9. Combine the policy filter and engine filter with `AND`.

All validation failures should occur during `get_flight_info`, before tickets
are minted.

### Security Rules

Engine filters have these constraints:

- They may only narrow results.
- They may reference columns not requested in the output projection.
- They may only reference columns that policy exposes plainly.
- They may not reference masked columns.
- They may not reference columns that are only available as hidden policy
  dependency columns.

This intentionally ties engine filterability to visible unmasked columns because
the current policy model has no first-class filter-only permission.

### Combination Semantics

The final planning filter is:

```text
combined_filter = policy_filter AND engine_filter
```

If either side is absent, the other side becomes the combined filter. If both
are absent, planning proceeds without a filter.

The combined filter should be canonicalized through the existing `RowFilter`
normalization path so downstream code continues to operate on normalized SQL.

## Planning And Execution Flow

### Execution Projection

`_build_execution_projection(...)` in
`src/dal_obscura/application/use_cases/plan_access.py` should run against the
combined filter.

That means internal dependency columns may come from:

- policy filters
- engine filters
- both at once

Dependency columns remain available for planning and execution but are not added
to the returned schema unless they were explicitly requested and allowed.

### Table-Format Planning

`table_format.plan(...)` should continue to receive a single `PlanRequest` with
one combined `row_filter`.

For Iceberg, `src/dal_obscura/infrastructure/table_formats/iceberg.py` should
keep the current behavior:

- split the combined filter into pushdown-safe and residual clauses
- compile the pushdown-safe subset into PyIceberg expressions
- preserve the residual subset for DuckDB execution

This means engine predicates can benefit from partition/file pruning when they
fit the existing safe pushdown subset, and still remain correct when they do
not.

### Ticket Payloads

`src/dal_obscura/domain/ticket_delivery/models.py` should keep the existing
ticket shape for `scan.row_filter`.

The ticket stores only the residual combined filter as a normalized SQL string.
It does not store separate policy and engine filters.

This keeps `do_get` simple and avoids expanding the ticket trust surface.

### Fetch-Time Execution

`src/dal_obscura/application/use_cases/fetch_stream.py` should remain
structurally unchanged:

1. verify the signed ticket
2. re-authenticate the caller
3. re-check policy version freshness
4. execute the pre-planned partition
5. apply the residual combined filter and masks in DuckDB

`do_get` must not accept or trust a fresh client predicate. The ticket remains
the only source of truth for fetch execution.

## Failure Modes

Planning should fail immediately when:

- the engine filter is invalid DuckDB SQL
- the engine filter references an unknown column
- the engine filter references a masked column
- the engine filter references a column not plainly exposed by policy

Execution should not introduce new filter-specific failure modes beyond the
existing ticket validation and backend execution errors. If a filter is accepted
at planning time but not fully pushdown-safe, the unsupported portion should run
as a residual DuckDB filter instead of being rejected.

## Observability

Add structured logging that makes engine-filter behavior visible without logging
raw predicate text by default.

Useful fields include:

- whether an engine filter was present
- whether validation succeeded or failed
- number of engine filter dependency columns
- whether the combined filter produced pushdown work
- whether residual filtering remained

The default should avoid logging full filter SQL because literal values may be
sensitive.

## Testing Strategy

### Interface Tests

Update `tests/interfaces/flight/test_service_streaming.py` to cover:

- descriptor parsing accepts optional `row_filter`
- invalid descriptor filter payloads fail planning
- end-to-end requests with engine filters narrow results correctly

### Use Case Tests

Update `tests/application/use_cases/test_access_flow_use_cases.py` to cover:

- engine filters are parsed and combined with policy filters
- engine filters may reference visible unmasked columns that are not projected
- engine filters are rejected for masked columns
- engine filters are rejected for non-visible columns
- execution projection includes hidden dependency columns from engine filters
- ticket payloads carry the residual combined filter

### Iceberg Tests

Update `tests/infrastructure/adapters/test_iceberg_phase0_regressions.py` to
cover:

- combined policy and engine filters still push down safe clauses
- mixed pushdown/residual combined filters split correctly
- residual-only engine predicates remain accepted and execute later

### Flight Integration Tests

Update `tests/interfaces/flight/test_integration_flight_backends.py` to cover:

- a simple engine predicate that pushes down cleanly
- a mixed predicate where only part of the combined filter pushes down
- correct results when the engine filter references an allowed but unprojected
  column

## Risks And Mitigations

### Risk: Filter authorization is inferred from visible-column policy

Mitigation: make that rule explicit in code and docs. If the product later needs
filter-only permissions, add a separate policy concept instead of overloading
this feature.

### Risk: Combined filters make debugging harder

Mitigation: keep canonical SQL normalization and add structured logging for
filter presence, dependency counts, and pushdown-versus-residual behavior.

### Risk: Backend pushdown semantics diverge from DuckDB semantics

Mitigation: keep backend pushdown conservative and preserve a residual DuckDB
filter whenever a clause cannot be proven safe.

### Risk: Users expect "predicate pushdown" to mean full backend execution

Mitigation: document that accepted engine filters may be partly pushed down and
partly evaluated residually, with correctness taking priority over full
pushdown.

## Implementation Summary

The implementation should add an optional engine `row_filter` to the planning
request, validate and authorize it during `get_flight_info`, combine it with the
policy filter using `AND`, and reuse the existing Iceberg pushdown plus residual
DuckDB execution path. The main policy guardrail is that engine predicates may
only reference visible, unmasked policy-exposed columns, even when those columns
are not included in the returned projection.
