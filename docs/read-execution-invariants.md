# Governed read execution invariants

The correctness boundary is the final DuckDB transform, before Flight emits any
batch. Backend pushdown is an optimization. A signed ticket captures the planned
policy, execution dependencies, scan tasks, principal context, and visible fields.
Routine policy edits preserve captured access; explicit owner revocation stops it.
Configuration has one namespace per deployment, with no tenant/cell routing.
Tickets remain bound to asset identity, issuer, subject, groups, and attributes.

## Invariants and executable evidence

- **Bind one configuration snapshot per request.** Catalog, asset, policy rules,
  and schema admission come from the same database view. Schema and planning use
  that captured policy through concurrent edits. Reads fail closed without stale
  cache fallback; provider IO holds no configuration database connection. Evidence:
  `tests/infrastructure/adapters/test_live_request_context.py` and
  `tests/integration/test_live_snapshot.py`.
- **Bound and lease provider instances.** Resource-local catalog configuration
  keys provider reuse; policy edits never rebuild providers. Coalesce concurrent
  misses, release failed construction slots, evict only idle providers, and defer
  shutdown close until active leases finish. Saturation has a bounded wait.
  Evidence: `test_live_request_context.py` lifecycle/concurrency tests.
- **Authorize before reading.** Planning expands nested leaves, authorizes caller
  filter dependencies, and rejects unauthorized or masked caller predicates.
  Policy-only dependencies remain internal execution columns. Evidence:
  `tests/application/access_flow/test_planning_admission.py`,
  `test_planning_projection.py`, `test_planning_filters.py`, and `test_fetch.py`.
- **Enforce the full restriction.** Combine policy and caller predicates with
  AND; retain every dependency; apply the full filter to original values before
  output masking, including when the backend claims complete pushdown. Evidence:
  `test_fetch_stream_reapplies_fully_pushed_row_filter_after_backend_execution`,
  `test_duckdb_transform_filters_on_hidden_execution_column`, and
  `test_duckdb_transform_filters_original_masked_values_before_output_mask`.
- **Preserve SQL NULL semantics.** Only rows for which WHERE evaluates TRUE may
  leave the transform. NULL comparison/IN literals and IS NULL/IS NOT NULL use DuckDB SQL at both
  native hints and the mandatory core filter. Evidence: domain row-filter tests
  and `test_native_filter_hint_retains_hidden_delete_keys`.
- **Ancestor masks dominate descendant projections.** Selecting a deeper child
  cannot bypass a mask on its parent, including within lists and map values.
  Whole-container selection must enforce all descendant masks. Hidden map keys
  hide the map rather than expose keys or construct invalid NULL keys. Evidence:
  `test_nested_parent_mask_governs_pruned_descendant`,
  `test_full_map_selection_enforces_descendant_masks`, and
  `test_null_map_key_hides_entire_map_and_preserves_projected_value_schema`.
- **Declare the exact emitted schema.** SQL projections and schema construction
  use the same mask precedence and DuckDB type normalization. Plugin declarations
  and each lazy input batch are validated. Evidence: adapter schema assertions,
  Flight `test_flight_info_schema_matches_mask_output_types`, and public-plugin
  `test_public_format_validates_lazy_batch_schema_before_streaming`.
- **Bind execution to the ticket.** Reject identity/issuer/context mismatches,
  corrupt payloads, expired tickets, excess exchanges, and explicit revocation.
  Check stream expiry/deadline/revocation before each emitted batch. Exchange
  exhaustion blocks new fetches, not an already reserved stream. Cleanup retains
  exhausted records until expiry so revocation remains checkable. Evidence:
  `tests/application/access_flow/test_fetch.py`, `test_tickets.py`, and
  `test_cleanup_preserves_last_reserved_stream_until_expiry`.
- **Bound streaming resources.** Acquire admission before pulling input; execute
  one input batch at a time; enforce logical and retained-buffer byte limits;
  close readers, sources, and DuckDB connections on success, error, or cancellation;
  release admission slots on failures. Evidence: DuckDB adapter admission, slice
  buffer, OOM, subprocess memory, cancellation, and first-output tests.
- **Pin and split scans.** SDK handles pin metadata locations or table versions.
  Native libraries interpret deletes. COW and position-delete Iceberg use disjoint
  file groups; equality deletes use one native scan with bounded internal threads.
  Delta and Parquet split row groups. Evidence: owning native-format tests,
  cloud-format tests and installed-wheel qualification. See [cloud execution](cloud-formats.md).

## Performance scope and limits

The independent [Delta plugin](../packages/delta-plugin/README.md) pins table UUID
and version, splits row groups, and carries kernel-produced deletion masks in
passive JSON. Inline/UUID/absolute DVs and copy-on-write or merge-on-read updates
retain exact row coverage across restored parallel tasks. Its public reader
materializes snapshot-wide DV masks, so DV-enabled snapshots have an explicit
16-million-physical-row planning cap. Delta retention must exceed ticket lifetime.
Tests in `packages/delta-plugin/tests` and the real Delta Flight consumer lane
qualify these contracts; see the package guide for supported features and budgets.

DuckDB uses one thread per admitted stream and transient in-memory connections.
Queries consume Arrow batches, with no whole-result accumulation in production.
Ticket fan-out provides file-level parallelism; clients must consume endpoints
concurrently to realize it. Iceberg COW/position tasks assign largest data-file byte costs first to the
least-loaded ticket, with deterministic ties. Equality-delete tasks use up to four
native threads instead of distributed fan-out. Native reads retain all schema
fields to preserve hidden equality keys. These choices have documented IO and
parallelism costs; no production speedup is claimed. Planning materializes bounded
file membership and tickets, and upstream metadata APIs may materialize manifests.
Planning memory therefore grows with file count; native delete buffers are owned
by DuckDB's execution engine, not a custom per-file Python loader.

Admission and DuckDB memory limits are per process. Arrow allocations and backend
buffers are not all charged to DuckDB's memory limit. Input/output checks reject
oversized batches after their allocation; they are not hard process-RSS limits.
Deadline checks prevent late output but cannot interrupt an arbitrary blocking
backend call. Revocation requires a database lookup per emitted batch, which needs
measurement under deployment-level concurrency. Exhausted tickets remain in
the database until TTL expiry, trading bounded retention for correct active reads.

The masking benchmarks verify output while timing scalar/nested transforms; the
multi-file and ticket-to-response benchmarks cover adjacent costs. These establish
local regression evidence, not cluster capacity or a production throughput SLA.
Use representative file skew, delete density, wide nested rows, and concurrent
clients for deployment sizing. Native engine/extension upgrades require delete, nested schema and installed-wheel
conformance checks.

## Verification

Run `uv run pytest`, `uv run ruff check .`, and `uv run ty check`.
Run masking, multi-file, and ticket-to-response benchmarks with `--benchmark-only`
and save benchmark JSON for same-host comparisons. Flight tests require local
socket binding. See [development](development.md) for benchmark commands and
[security](security.md) for ticket revocation semantics.
