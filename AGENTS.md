<!-- caveman-autowire-begin -->
Use the `caveman` skill at the start of every Codex session and keep it active by default. Default level: full. Respect the skill boundaries: write code, commit messages, PR text, safety warnings, irreversible-action confirmations, and user-requested clarifications normally. User can disable with "normal mode" or "stop caveman".
<!-- caveman-autowire-end -->

<!-- ccc-autowire-begin -->
Use the `ccc` skill whenever code search, codebase exploration, semantic search, indexing, or CocoIndex Code would be helpful. Prefer `ccc search` for conceptual code lookup when exact-text search is insufficient, and keep indexes fresh with `ccc search --refresh` or `ccc index` after meaningful code changes. Run CocoIndex Code commands with usage tracking disabled; preserve `COCOINDEX_DISABLE_USAGE_TRACKING=1`.
<!-- ccc-autowire-end -->

# Agent Guide (dal-obscura)

## Purpose and working agreement

This repository implements governed data access through Arrow Flight, with an
authenticated governance UI, SQL row filters, column masking, and admitted catalog
and table-format plugins. Optimize for correctness, readability, maintainability,
bounded resource use, and a polished user experience.

- For a major project-wide rewrite, analyze the complete system, present a concrete
  plan and tradeoffs, and wait for approval. Once approved, execute through
  implementation, qualification, documentation, and requested Git delivery. Do not
  repeatedly ask for approval for work already authorized.
- The current project has no production users or external plugin consumers. Within
  authorized work, prefer clean breaking contracts to compatibility scaffolding.
  Update all bundled plugins and affected callers together; preserve features and
  document intentional changes. Reassess this assumption if deployment usage changes.
- Optimize across the project rather than moving complexity between modules.
  Remove dead code, forwarding facades, duplicate representations, obsolete shims,
  and abstractions that do not clarify ownership or remove meaningful duplication.
- Keep functions small and names concrete. Share behavior at its owning boundary;
  avoid speculative frameworks, parallel implementations, and generic wrappers.
- Keep workers stateless apart from existing shared database records and bounded
  caches. Adding a persistent subsystem requires explicit approval.
- Use `uv` for Python environments, dependencies, scripts, tests, and builds.
  Use the UI's pinned Node and pnpm versions from `package.json`.
- Preserve unrelated working-tree changes. When asked to commit and push, finish
  relevant checks, stage the intended files, use Conventional Commits, push the
  current authorized branch, and report the commit and remaining limitations.

## Current ownership

The old `common/`, `data_plane/`, and `control_plane/` production trees are gone.
Do not recreate them or introduce compatibility imports. Existing test directory
names do not define the production architecture.

- `src/dal_obscura/policy/`: immutable policy/principal models, authorization,
  typed paths, SQL filter validation, schema traversal, and the shared projection
  compiler. Mask SQL and emitted Arrow schemas have one implementation here.
- `src/dal_obscura/read/`: `ReadService.schema/plan/fetch`, signing, tickets,
  exchange reservations, stream ownership, admission limits, and DuckDB execution.
- `src/dal_obscura/sources/`: catalog resolution, published configuration,
  admitted plugin lifecycle, immutable handles, passive scan envelopes, native
  Iceberg execution, path admission, and bounded discovery.
- `src/dal_obscura/identity/`: JWT/JWKS/OIDC validation, normalized claims,
  browser sessions, and local/federated logout.
- `src/dal_obscura/storage/`: feature-owned SQL queries, atomic snapshots,
  revisions, audit, session/ticket records, and explicit database migrations.
- `src/dal_obscura/control/`: authorized administrative commands, policy
  publication, schema admission, evaluation, and identity-attribute management.
- `src/dal_obscura/interfaces/`: Flight, HTTP, and CLI translation/composition.
  HTTP routes own explicit transaction boundaries; transport types stay out of
  policy and read contracts.
- `apps/governance-ui/src/`: React UI. Navigation owns URLs and leave guards,
  query hooks own session-scoped server state, and `policy_draft.ts` /
  `usePolicyDraft.ts` own draft edits, baselines, and save fences. HTTP types come
  from checked-in OpenAPI generation, not handwritten copies.
- `packages/plugin-api/` and `packages/plugin-conformance/`: independent public
  SDK and conformance kit. External plugins must not import service internals.

See [architecture atlas](docs/architecture/architecture-atlas.md),
[execution invariants](docs/read-execution-invariants.md), and
[cutover runbook](docs/core-cutover.md) for deeper contracts.

## Governed read invariants

- Authenticate and capture one immutable configuration snapshot for source
  resolution, policy, and schema admission. Release database transactions before
  provider IO; retain one opened format and schema descriptor during planning.
- Validate every planned task and output schema before atomically issuing all
  tickets. A partial failure must not leave a usable subset of tickets.
- Ticket contents are the source of truth for fetch. Verify signature, expiry,
  captured principal/issuer/context, exchange limits, and explicit revocation;
  never trust client replay of `PlanRequest`.
- Existing tickets retain their captured policy until expiry or explicit
  revocation. Do not silently replace it with the latest policy or add a global
  policy-generation counter. Check identity/ticket expiry, stream deadline, and
  revocation before each emitted batch.
- Express masks and row filters as DuckDB SQL. Combine policy and caller filters
  with AND, retain hidden predicate dependencies, and apply the complete filter
  to original values before masking. Backend pushdown is only an optimization.
- Preserve SQL NULL behavior, ancestor-mask precedence, nested list/map semantics,
  and distinct literal dotted or collection-token field names. A deeper selection
  must never bypass a parent mask or reveal hidden map keys.
- Use the shared projection compiler for both emitted values and declared types.
  Mask changes must update SQL and Arrow schema behavior together. Compare plugin
  output schemas and batches with metadata included.
- Prefer Arrow buffer reuse and bounded DuckDB streaming. Do not accumulate whole
  results or copy batches unnecessarily. Enforce logical and retained-buffer
  limits; close streams, iterators, readers, providers, and connections on success,
  failure, cancellation, and early stop while preserving the primary error.
- Plan parallel tasks whenever files, fragments, partitions, or row groups are
  splittable. Cover work exactly once within the task budget, group excess work,
  and return zero tasks for an empty scan. Document any unsplittable backend's
  limitation and performance cost. Preserve pinned snapshots and native deletes.
- Catalog resolution and plugin admission must be deterministic. Never mutate
  shared configuration or registry state during a request.
- Production tickets contain bounded passive JSON only; no pickle, arbitrary
  classes, live resources, callbacks, or executable serialization.

## Plugin contract and release discipline

Release 0.2.0 uses plugin API 2 and configuration version 1. The SDK is the sole
contract; do not define another factory or task contract inside the service.

- Catalog factories accept `(CatalogConfig, ExecutionContext)` and validate
  configuration during construction. Catalogs expose discovery, resolution, and
  cleanup; there is no separate `validate_config` hook.
- Format factories bind `(TableHandle, ExecutionContext)` once. Subsequent calls
  are `schema(context)`, `plan(ScanRequest, context)`, and
  `execute(ScanTask, context)`. Do not repeat the handle or pass a separate schema
  and projection list to planning.
- `ScanRequest` contains the exact projected Arrow schema, order, complete nested
  types, metadata, task budget, and optional declared pushdown hint.
  `ScanTask` recursively freezes validated JSON objects; use `to_json()` and
  `from_json()` for detached wire data. Generic object/null tasks are unsupported.
- Configuration, descriptors, and handles are immutable. Use descriptor
  `to_json()` for admission and HTTP metadata; core derives schema fingerprints.
  Use `ExecutionContext.check_active()` before and after provider work and before
  requesting more lazy work. Close planning iterators on budget rejection.
- Declare output formats, capabilities, and handle versions in static descriptor
  metadata. Load plugins through exact distribution/version/API/descriptor/artifact
  locks and registered entry points, never by arbitrary module path.
- Contract changes must update native SQL/Iceberg, manifest/Parquet, REST Iceberg,
  core routing, the conformance kit, documentation, and owning tests together.
- Rebuild affected wheels, install them in a fresh environment, qualify with
  repository source paths disabled (`pytest -o pythonpath=`), and regenerate the
  lock from those exact installed artifacts. Verify SDK independence, locked
  admission, CLIs, and packaged migrations. Keep release output outside Git.

See [SDK contract](packages/plugin-api/README.md) and
[plugin qualification](docs/plugin-rebuild.md).

## Governance UX and identity

- Research established interaction patterns before a substantial UX redesign;
  Immuta is a reference for governance and identity-attribute authoring. Apply
  patterns that fit this product and explain the reasoning.
- Use meaningful labels, readable tags/badges, consistent alignment and spacing,
  and clear hierarchy. Reserve button styling for actual actions. Keep long
  owner/issuer identifiers from distorting inventory rows; retain access to detail.
- Column search and selection must show scope, selected counts, and clear effects
  for select-all, exclusions, and prefixes. Support keyboard use, useful empty
  states, and visible feedback. Keep schema browsing available in a collapsible
  sidebar; expanding nested fields must preserve focus and scroll position.
- Keep adding a rule discoverable without scrolling through a long rule list.
  Avoid duplicate primary actions and presenting an empty new-rule form as if it
  were an existing rule.
- Preserve unsaved drafts, guard navigation, handle concurrent revisions, and
  ignore stale asynchronous responses. Scope caches to the authenticated session
  and clear identity-specific state on session changes.
- Use the existing component/design system; verify keyboard focus, accessibility,
  responsive layout, and real rendered states. Deliver before/after images for UX
  changes with a concise explanation of what changed and why. Review captures
  belong in `docs/ui-review/`, separately from correctness tests.
- Map configured source claim paths from verified identities to canonical internal
  attribute keys. Expose labels, descriptions, source information, and allowed
  values in policy authoring; use the same mapper and domain enforcement at runtime
  and in mapping previews. Missing attributes cannot satisfy policy conditions.
- Attribute discovery contains provider configuration, not user records, secrets,
  or observed claim values. Do not add a user directory, SCIM, or value harvesting
  without explicit scope. Synthetic mapping/policy previews do not authenticate
  identities or verify JWTs; label them accordingly.
- Sign-out must revoke the local session and exercise provider logout when
  supported. Verify SSO behavior with a live identity provider; synthetic browser
  routes alone do not establish federated sign-out correctness.

See [policy authoring](docs/policy-authoring.md) and
[UI guide](apps/governance-ui/README.md).

## Testing and qualification

Follow the Testing on the Toilet principles recorded in
[testing guide](docs/testing.md): test observable behaviors through their owning
public boundary, keep cases focused, and use independent expected results.

- Follow TDD for new behavior: extend the owning behavioral test, observe the
  intended failure, implement the smallest change, then run focused and relevant
  broader checks.
- Aggressively remove redundant tests and duplicate validation within the same
  boundary. Preserve validation at distinct trust boundaries and proof that each
  boundary enforces it; a lower-level validator test does not replace admission,
  HTTP, signed-ticket, or plugin-routing coverage.
- Parameterize equivalent scenarios with named cases. Keep distinct behaviors
  separate; do not combine unrelated assertions into one large workflow merely
  to reduce test counts. Prefer clear tests over excessive DRY abstractions.
- Put shared fakes/builders in `tests/support`; tests must not import other test
  modules. Fixtures own setup and cleanup, not assertions or hidden expectations.
  Give each test fresh mutable state and explicit resource ownership.
- Trigger cancellation and races using provider events or explicit release
  signals, not validation-call counts, arbitrary sleeps, or timing luck.
- Assert exact schemas and meaningful scalar/nested values, not just row counts.
  Test success, failure, cancellation, and early-close resource ownership.
- Keep helper, component, browser, real-provider, consumer, database, installed-wheel,
  JVM, and performance lanes distinct. Qualify affected contracts at their owning
  lanes; do not replace real-service evidence with mocks or silent skips.
- Full mandatory qualification uses disposable PostgreSQL, all real consumer
  backends, and the no-skips wrapper. Do not disturb existing user demo services.
  Run relevant UI, live SSO, and JVM lanes when their contracts change.
- Performance claims require comparable same-host baselines and representative
  file counts, nested data, deletes, and streaming/RSS measurements. State measured
  limits honestly; per-worker budgets do not establish a global RSS or capacity SLA.
- Documentation-only changes need path/link/command checks, not a full runtime
  suite. Once appropriate checks pass, rerun only for new changes or concerns.

## Commands

Bootstrap, migrate, and run:

```bash
uv sync --dev --extra server --extra sqlite --extra postgres
uv run --no-sync dal-obscura-migrate upgrade
uv run --no-sync dal-obscura --help
uv run --no-sync dal-obscura-control-plane --help
pnpm --dir apps/governance-ui install --frozen-lockfile
pnpm --dir apps/governance-ui dev
```

The data plane uses `DAL_OBSCURA_*` environment variables and an already migrated
and published database. Start the authenticated UI with the control-plane service.
Use bootstrap tokens only for disposable development profiles; services do not
migrate at startup.

Fast and focused checks:

```bash
uv run --no-sync pytest -m 'not heavy and not integration and not socket'
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check
uv run --no-sync pytest tests/domain/access_control/test_row_filters.py tests/interfaces/flight/test_descriptors.py tests/infrastructure/adapters/test_duckdb_filters.py
uv run --no-sync pytest tests/plugin_platform packages/plugin-conformance/tests packages/manifest-parquet-plugin/tests packages/iceberg-rest-plugin/tests
uv run --no-sync pre-commit run --all-files
```

Complete Python qualification requires `DAL_OBSCURA_POSTGRES_TEST_URL` pointing
to a disposable `postgresql+psycopg` database and permission to bind local sockets:

```bash
DAL_OBSCURA_RUN_CONSUMER_TESTS=1 uv run --no-sync python scripts/require_no_skips.py \
  --junitxml /tmp/dal-python.xml -- \
  uv run --no-sync pytest --ignore=tests/benchmarks
```

UI and JVM checks:

```bash
pnpm --dir apps/governance-ui test
pnpm --dir apps/governance-ui test:stories
pnpm --dir apps/governance-ui test:e2e
pnpm --dir apps/governance-ui check
pnpm --dir apps/governance-ui check:api-types
pnpm --dir apps/governance-ui build
mvn -f connectors/jvm/pom.xml verify
```

Live SSO uses the demo runner in
[Keycloak guide](examples/demo/keycloak/README.md). Review captures use
`pnpm --dir apps/governance-ui capture:review` with the relevant review spec.

Benchmarks and release builds:

```bash
uv run --no-sync pytest tests/benchmarks --benchmark-only
uv run --no-sync pytest tests/benchmarks/test_masking_row_filter_benchmarks.py --benchmark-only --benchmark-json .benchmarks/row-filter-mask.json
uv run --no-sync pytest tests/benchmarks/test_iceberg_multifile_benchmark.py --benchmark-only --benchmark-json .benchmarks/iceberg-multifile.json
uv run --no-sync pytest tests/benchmarks/test_ticket_to_response_benchmark.py --benchmark-only
uv build --wheel --out-dir dist/plugins-0.2.0 .
uv build --wheel --out-dir dist/plugins-0.2.0 packages/plugin-api
uv build --wheel --out-dir dist/plugins-0.2.0 packages/plugin-conformance
uv build --wheel --out-dir dist/plugins-0.2.0 packages/manifest-parquet-plugin
uv build --wheel --out-dir dist/plugins-0.2.0 packages/iceberg-rest-plugin
```

The complete benchmark lane includes large subprocess RSS probes; select focused
cases for routine checks. Generate the plugin lock with `scripts/build_plugin_lock.py`
against installed wheels, not source imports, and rebuild the lock whenever an
artifact changes. Never hand-edit generated OpenAPI, client types, or protobuf code.
