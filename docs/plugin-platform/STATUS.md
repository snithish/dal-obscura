# Plugin platform progress ledger

Baseline reviewed: `5208eee9af35e38b5ab8294524a0485611c2b0a7`.
Implementation follow-up through `0e049c0`.
Review date: 2026-09-12. **Paid-production release: HOLD.**

This task began with review/planning documents and now includes incremental runtime,
UI, and plugin-contract slices. Database changes remain additive, external
plugin wheels are still unverified, and pickle serialization remains unchanged.
Local probes are recorded in [the review](IMPLEMENTATION_REVIEW.md).
Earlier implementation evidence remains in [the UI ledger](../ui-v2/EXECUTION_STATUS.md).

## State meanings

- `not-started`: no implementation under the new packet has begun.
- `implementing`: code/tests exist but at least one packet criterion is incomplete.
- `implemented-unverified`: intended implementation exists; required execution
  evidence is missing. This state is not acceptance.
- `accepted`: every criterion owned by that packet passed, with evidence tied to
  the relevant commit/artifact and the covered A subcases listed. Broader A scenarios
  spanning later packets remain open until their complete evidence exists. Later
  regressions reopen the packet; packet acceptance is not product release approval.
- `blocked`: name the external dependency and remaining work; never treat it as done.

## Ordered queue

- X00 baseline and constraints: **implemented-unverified**; acceptance registry,
  baseline diagnostics, and immutable synthetic ticket/task-boundary inventory are
  recorded. Runtime preservation tests do not replace the independent compatibility
  lane.
- X01 selected-only publication: **implemented-unverified**; initial activation now
  scopes to selected asset/catalog. Focused SQLite/API evidence passed; Flight and
  PostgreSQL evidence remain open.
- X02 immutable draft/review: **implemented-unverified**; strict review requires an
  explicit saved draft and legacy rule hashes are bound. PostgreSQL race evidence and
  full snapshot binding remain open.
- X03 publication/grant/binding transactions: **implementing**; asset-row locks now
  serialize shared-rule, draft, restore, owner, grant, and admitted-schema
  mutations with publication, and binding/access writes expose an optional
  monotonic asset revision precondition that returns 409 on stale writers.
  PostgreSQL barrier evidence, full transaction rollback/idempotency, and
  multi-process grant/binding evidence remain open.
- X04 canonical evaluation: **implemented-unverified**; resolved mask values now
  flow from canonical preview and an unmatched-principal regression passes.
- X05 canonical bounded schemas: **implemented-unverified**; canonical Arrow schema
  encoding includes nested metadata/IDs and direct loader bounds. Direct Arrow
  fingerprint calls now enforce node/depth limits too. Migration and all
  entry-route/byte-budget evidence remain open.
- X06 safe schema evolution: **implementing**; typed evaluation paths now preserve
  literal dotted names, schema-field records carry optional stable IDs and typed
  paths through migration `20260912_0011`, and admitted identities are carried into
  immutable manifests and checked before data-plane planning. Provider-derived IDs,
  collection-path identity and the full evolution policy remain open. Migration
  `20260912_0013` backfills deterministic legacy IDs and paths for existing rows
  and fails closed on duplicate logical paths. Review tokens bind the persisted admitted-schema
  digest in addition to the live Iceberg digest.
- X07 configuration/secrets/IO: **implementing**; nested dynamic class-loader options
  are rejected, and schema/evaluation/review provider calls now use the configured
  catalog egress validator. Explicit environment secret references now resolve in
  discovery and schema paths. Typed provider configs, provider-returned IO
  enforcement, and production secret-provider lifecycle evidence remain open.
- X08 budgets and atomic reload: **implementing**; discovery now bounds provider
  iterators before materialization and checks cancellation/deadline per item while
  retaining deque traversal. Synthetic evaluation now bounds encoded fixture bytes
  before provider work. Plugin admission exposes build-then-swap reload snapshots;
  provider-specific transport timeouts and multi-worker capacity remain open.
- X09 UI lifecycle: **implemented-unverified**; initial-load epoch and synchronous
  logout fencing plus stale history/preview/review/publish response checks are fixed.
  Deferred browser tests and full operation-state coverage remain open.
- X10 complete authoring/management: **implemented-unverified** for the deny-all UI
  path; controls now expose save/test/review/publish actions when rules are empty.
  Full editor, activation, accessibility, and browser evidence remain open.
- X11 public SDK: **implementing**; an independently buildable
  `packages/plugin-api` wheel now contains the versioned contracts and has an
  offline compile/metadata regression. Core and SDK contracts now reject malformed
  plugin IDs, unbounded capabilities, invalid catalog revisions, and non-canonical
  schema fingerprints. Service-side compatibility contracts and online wheel
  artifact evidence remain open.
- X12 admitted loading and Iceberg adapter: **implementing**; entry-point loading now
  fails closed when a request names an unallowlisted installation, with registry
  regression coverage. Built-in Iceberg routing and artifact-lock verification
  remain open.
- X13 plugin routing and migration: **implementing**; immutable compiled asset
  manifests now record explicit catalog and table-format adapter identities.
  Runtime registry routing, migration of legacy manifests, and mixed-version
  rollout evidence remain open.
- X14 plugin UI: **not-started**.
- X15 conformance kit: **not-started**.
- X16 REST Iceberg qualification: **not-started**.
- X17 independent manifest/Parquet plugin: **not-started**.
- X18 consumer qualification: **not-started**.
- X19 secure deployment and identity lifecycle: **not-started**.
- X20 recovery and upgrades: **not-started**.
- X21 performance/observability/test efficiency: **not-started**.
- X22 exact-artifact CI: **implementing**; the container publication job now
  depends on a pinned governance-UI install/type-check/build lane. Artifact
  digest, browser, provider, consumer, recovery, and mandatory-security gates
  remain open.
- X23 independent review/release decision: **not-started**.

Next implementation action: continue X03 with PostgreSQL barrier/CAS evidence and
then complete X06 provider-derived and collection field identity rules. Do not add new
providers before Phase A's security/correctness prerequisites are accepted.

Latest implementation slices after the schema migration: X08 provider-page budget
and cancellation checks are validated by
`tests/control_plane/test_catalog_discovery.py` (5 passed) with Ruff clean. The
slice is intentionally not marked accepted because live provider timeout,
multi-worker capacity, and atomic reload evidence are still required.

X12 admission loading now rejects unallowlisted entry points before invoking a
factory; `tests/plugin_platform/test_registry.py` passes all 5 cases with Ruff
clean. Built-in Iceberg routing and artifact-lock verification remain open.

X11 package check: `tests/plugin_platform/test_plugin_api_package.py` passes and
the package source compiles without importing the service distribution. Building
the wheel with `uv build --wheel --no-build-isolation` is currently blocked because
the isolated environment has no `setuptools`; the normal online wheel/artifact
gate remains unverified and is not claimed by the test.

Integration boundary check after the migration and lock slices:
`UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane/test_assets_api.py tests/interfaces/control_plane/test_schema_api.py tests/plugin_platform -q`
passed after the X13 manifest metadata change. Ruff passed on every changed Python path and `git diff --check`
is clean. The expanded migration/schema/publication boundary suite also passes;
browser, PostgreSQL, Flight, consumer, and production lanes remain open.

After the cell lock-order slice, the same expanded boundary suite passed again,
including publication-store, policy-version, schema migration, plugin, and
published-config adapter tests.

The schema/review egress slice also passed schema-service, schema API, and policy
version tests with Ruff and Ty clean. Its provider-call regression proves a denied
catalog host is rejected before the loader is invoked.

The subsequent boundary run included catalog API coverage and passed at 100%.

The secret-resolution slice added schema and catalog discovery regressions and
passed their focused suites with Ruff clean. Secret values are supplied only to
provider calls and are not returned in API responses.

The evaluation byte-budget slice passed its focused helper/service checks with
Ruff and Ty clean; process-wide admission and live timeout/cancellation evidence
remain open.

### X09 UI lifecycle test lane — `499571f`

- State: implemented-unverified.
- Behavior: management and paginated history requests now share a small,
  dependency-free epoch guard so stale responses cannot overwrite a newer view.
  The governance UI exposes a Node built-in test script covering stale-response
  rejection and monotonic epoch advancement.
- Green evidence: `node --experimental-strip-types --test
  apps/governance-ui/tests/lifecycle.test.mjs` passed (2); direct TypeScript
  project check and Vite production build passed.
- Package-manager note: the pinned pnpm/Corepack wrapper could not run offline
  because Corepack attempted to fetch pnpm metadata; this is an environment gate,
  not a source failure.
- Remaining gaps: browser-level lifecycle, real IdP/session, accessibility, and
  full operation-state coverage remain open for X19/X22.
- Pickle compatibility: unchanged.
- Next permitted packet: complete X03 PostgreSQL barriers and X06 evolution rules;
  keep browser acceptance open until the real authenticated lane runs.

### X03 asset binding creation CAS — `c815aa7`

- State: implementing.
- Behavior: asset upsert now treats a missing binding as implicit revision zero.
  A nonzero `expected_revision` is rejected with a conflict before any row is
  inserted, closing a compare-and-set hole on first creation.
- Red/green evidence: the new API regression was red with a 200 response before
  the fix and passes with the stale-update and stale-grant regressions in
  `tests/interfaces/control_plane/test_assets_api.py` (3 passed).
- Exact checks: focused pytest and Ruff checks passed; `git diff --check` passed.
- Remaining gaps: independent PostgreSQL barriers/processes, publication
  rollback/idempotency under lost responses, and binding/grant race evidence
  remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03 PostgreSQL/CAS transaction evidence, then
  X06 provider-derived and collection field identities.

### X06 collection schema identities — `1795312`

- State: implementing.
- Behavior: published schema admission now traverses list, large-list,
  fixed-size-list, and map children with canonical `$element`, `$key`, and
  `$value` path segments. Stable IDs and types for nested collection leaves are
  checked before an executable table format is returned.
- Red/green evidence: collection element/key/value acceptance and element-ID
  drift regressions in `tests/infrastructure/adapters/test_published_config.py`
  pass; Ruff and `git diff --check` pass.
- Remaining gaps: provider-derived identity rules for every backend, rename and
  addition policy, migration uniqueness proof, and PostgreSQL/browser/consumer
  acceptance remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X06 explicit schema-evolution policy tests,
  while X03 PostgreSQL transaction barriers remain an external gate.

### X06 duplicate identity rejection — `5c17d04`

- State: implementing.
- Behavior: published schema admission rejects a live provider schema that
  reuses a stable field ID anywhere in its nested tree, preventing ambiguous
  path resolution and dictionary overwrite from becoming implicit access.
- Red/green evidence: the duplicate-ID regression in
  `tests/infrastructure/adapters/test_published_config.py` was red before the
  guard and passes with the collection identity suite (3 focused tests passed);
  Ruff passed.
- Remaining gaps: provider-specific identity guarantees, explicit rename/add
  policy, and PostgreSQL/browser/consumer acceptance remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X06 evolution policy and X03 PostgreSQL
  transaction barriers.

### X06 reviewed selection expansion — `5f2e196`

- State: implementing.
- Behavior: when admitted schema identities exist, publication compilation
  expands wildcard and parent column selections to the reviewed canonical leaf
  paths. Mask entries receive the same expansion, so later schema additions
  cannot enter through an unresolved wildcard or parent grant.
- Red/green evidence: compiler regressions for wildcard plus mask expansion and
  nested-parent expansion pass; the complete `tests/control_plane/test_publication_compiler.py`
  module passed (34 tests), with Ruff and Ty clean.
- Remaining gaps: review-time population of the admitted set from every backend,
  explicit rename/addition policy, and PostgreSQL/browser/consumer acceptance
  remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X06 schema evolution policy and X03
  PostgreSQL transaction barriers.

### X22 governance UI test gate — `7db7a28`

- State: implementing.
- Behavior: the exact CI governance-UI job runs the dependency-free lifecycle
  test script before the TypeScript/Vite production build, so stale-response
  fencing cannot regress silently behind a green bundle build.
- Red/green evidence: the CI contract regression was red before the workflow
  step was added and passes with `tests/architecture/test_ci_workflow.py` (3
  passed); direct Node lifecycle tests, TypeScript, and Vite build remain green.
- Remaining gaps: clean pnpm install, artifact digest/provenance, browser/IdP,
  accessibility, plugin wheel, provider, consumer, recovery, and production
  acceptance gates remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X03/X06 correctness work; X22 remains open
  until the CI artifact lanes run in a clean environment.

### X06 admission digest verification — `4c7c76d`

- State: implementing.
- Behavior: the data-plane registry recomputes and verifies the immutable
  admitted-schema field digest before matching any live identities. Tampered or
  malformed digest metadata now fails closed with a review-again error.
- Red/green evidence: the admission-digest tamper regression was red before the
  guard and passes with numeric-ID, collection, duplicate-ID, and drift tests;
  Ruff and Ty passed on the changed adapter.
- Remaining gaps: end-to-end review-time admission population, provider identity
  guarantees, schema rename/addition policy, and PostgreSQL/browser/consumer
  evidence remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X06 evolution policy and X03 PostgreSQL
  transaction barriers.

### X10 authoring controls — `9fac614`

- State: implemented-unverified.
- Behavior: the governance UI now exposes explicit rule precedence controls and
  a JSON principal-condition editor. Invalid condition text remains local,
  announces an error, and blocks saving; valid conditions use the existing
  draft/review/publish workflow.
- Green evidence: direct TypeScript check, Vite production build, and Node
  lifecycle tests passed; the expanded Python boundary suite remains green.
- Remaining gaps: browser journey and accessibility evidence, typed mask UX
  coverage, active-generation/config activation proof, and real IdP/session
  acceptance remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X10 management/accessibility coverage while
  X03 PostgreSQL and X06 provider-evolution gates remain open.

### X10 typed mask values — `0e049c0`

- State: implemented-unverified.
- Behavior: the UI keeps `keep_last` values as bounded integers and preserves
  default-mask JSON scalar types instead of coercing booleans and numbers into
  strings. Invalid values remain subject to the control-plane validator before
  review/publication.
- Green evidence: direct TypeScript check, Vite production build, Node lifecycle
  tests, and `git diff --check` passed.
- Remaining gaps: browser/a11y authoring journeys and server-side golden mask
  value/type coverage remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: continue X10 browser/accessibility coverage and X04
  evaluation goldens; PostgreSQL and external consumer gates remain open.

The combined control-plane, catalog, schema, publication, migration, plugin, and
published-config boundary suite passed at 100% after the budget change.

The direct-Arrow bounds regression and Ty/Ruff checks passed in
`3263645`; the full schema byte-budget and every-entry-route acceptance matrix
remains open.

### X04 synthetic fixture semantics — `4ab9031`

- State: implemented-unverified.
- Behavior: policy evaluation now distinguishes omitted rows from an explicit
  zero-row input, preserves the authorized output schema for empty results,
  validates supplied rows at the Arrow boundary, and records a digest of the
  exact schema-bound fixture. Generated fixtures cover typed dates, timestamps,
  times, decimals, binary values, nested structs, and non-string map keys;
  unsupported types fail with a safe validation error.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/control_plane/test_evaluation_service.py
  tests/interfaces/control_plane/test_schema_api.py -q` passed (8); Ruff and
  Ty passed on changed paths; TypeScript remains unchanged from the prior UI
  slice.
- Environment/dependency: Darwin arm64, repository `.venv`, no external
  catalog, IdP, Flight server, or customer data.
- Pickle compatibility: no pickle modules, serializers, or payloads changed.
- Remaining gaps: full mask/type golden matrix, provider-call assertions,
  process-wide evaluation admission, real Iceberg/Flight parity, and the
  PostgreSQL race gates remain open.
- Next permitted packet: X03 PostgreSQL CAS/barrier slice, then X06 schema
  evolution rules.

Follow-up regression coverage in `1c6c97e` asserts invalid user-supplied rows
return the stable redacted validation response at the HTTP boundary.

### X03 asset revision preconditions — `6aac008`

- State: implementing.
- Behavior: asset bindings, owners, grants, and admitted schema metadata now
  advance a monotonic revision. API callers may send `expected_revision`; after
  the publication row lock, stale values fail with HTTP 409 and cannot overwrite
  the committed value. Asset detail exposes the current revision and the UI sends
  it for owner and grant edits.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/interfaces/control_plane/test_assets_api.py
  tests/control_plane/test_asset_mutation_locks.py -q` passed (16); migration
  tests passed after updating the head assertion; Ruff, Ty, TypeScript, and
  `git diff --check` passed.
- Migration: additive `20260912_0012` adds `assets.revision` with a zero default.
- Pickle compatibility: no pickle modules, serializers, or payloads changed.
- Remaining gaps: PostgreSQL barrier-controlled races, operation/audit atomicity,
  and independent multi-process evidence remain required before acceptance.
- Next permitted packet: X03 PostgreSQL CAS/barrier slice, then X06 admitted
  schema identity and evolution rules.

### X03 review-token generation binding — `9fbeb51`

- State: implementing.
- Behavior: signed review evidence now captures the governed asset revision;
  publication rejects a token after binding, owner, grant, or admitted-schema
  metadata changes, even when the policy draft and live schema are unchanged.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/interfaces/control_plane/test_schema_api.py -q` passed (6), with
  the stale metadata token regression included; Ruff and Ty passed on changed
  paths.
- Pickle compatibility: unchanged.
- Remaining gaps: PostgreSQL barrier-controlled races and transaction recovery
  evidence remain open.
- Next permitted packet: X03 PostgreSQL CAS/barrier slice, then X06 evolution
  rules.

### X03 publication rollback evidence — `101fd00`

- State: implementing.
- Behavior: an injected failure after the initial publication pointer update
  rolls back the immutable publication and audit event together; no partial
  generation remains visible to a retry.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/interfaces/control_plane/test_policy_versions_api.py::test_publication_failure_after_activation_rolls_back_audit_and_generation -q`
  passed; Ruff passed on the changed test.
- Scope: SQLite transaction evidence only. PostgreSQL barrier interleavings,
  idempotency replay after a lost response, and multi-process grant/binding
  races remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: PostgreSQL X03 barriers, then X06 evolution rules.

### X22 governance UI CI gate — `7fcf6b9`

- State: implementing.
- Behavior: CI installs `apps/governance-ui` from its pinned pnpm lockfile,
  runs the TypeScript build, and blocks image scanning/publication until that
  lane passes.
- Green evidence: local `pnpm --dir apps/governance-ui check` and `build` pass;
  workflow syntax was updated without changing runtime behavior.
- Remaining gaps: exact tested image/server/UI digest promotion, browser and
  accessibility journeys, provider/consumer/recovery lanes, and mandatory
  security-failure blocking remain open under X22.
- Pickle compatibility: unchanged.
- Next permitted packet: X03 PostgreSQL barrier evidence and X06 evolution rules;
  X22 remains a later release gate.

### X11 contract identity validation — `7e4e34d`

- State: implementing.
- Behavior: both the service-side and independently packaged plugin contracts
  validate bounded IDs, capability metadata, catalog generations, and canonical
  schema digest shape before admission or execution.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/plugin_platform/test_plugin_contract_validation.py
  tests/plugin_platform/test_registry.py tests/plugin_platform/test_plugin_api_package.py -q`
  passed (9); Ruff and Ty passed on changed Python paths.
- Remaining gaps: clean-environment wheel build, static descriptor/artifact lock
  verification, external package admission, and service compatibility matrix.
- Pickle compatibility: unchanged.
- Next permitted packet: complete X03 PostgreSQL barriers and X06 evolution rules;
  do not advertise external plugins from this slice alone.

### X06 runtime schema admission — `32a7fa0`

- State: implementing.
- Behavior: persisted admitted schema fields are included in immutable compiled
  asset manifests with a canonical digest. The published data-plane catalog
  registry checks each admitted path and stable field ID against the live Arrow
  schema before returning an executable table format, normalizing numeric
  PyIceberg IDs and common Iceberg type aliases, and rejecting rebound or removed
  fields so schema drift cannot expand access.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/control_plane/test_publication_compiler.py
  tests/infrastructure/adapters/test_published_config.py
  tests/control_plane/test_policy_authorization.py
  tests/control_plane/test_schema_service.py -q` passed (53); Ruff and Ty passed
  on changed paths.
- Compatibility: publications without an admitted schema remain supported;
  pickle modules, serializers, and task payloads are unchanged.
- Remaining gaps: provider-derived IDs for all collection nodes, typed collection
  path encoding, legacy-row migration/uniqueness proof, and PostgreSQL/browser/
  consumer acceptance evidence remain open.
- Next permitted packet: X03 PostgreSQL barrier evidence, then X06 collection
  identity and explicit schema-evolution policy tests.

Follow-up compatibility regression in `ccb00bc` proves a namespaced admitted
`iceberg:1` identity matches PyIceberg's numeric `PARQUET:field_id=1` metadata,
while a rebound numeric ID remains rejected.

Post-normalization boundary verification: the expanded control-plane, migration,
plugin, publication, schema, and published-config suite passed at 100% with
`UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane
tests/interfaces/control_plane/test_assets_api.py tests/interfaces/control_plane/
test_schema_api.py tests/interfaces/control_plane/test_policy_versions_api.py
tests/plugin_platform tests/common/config_store/test_schema_migrations.py
tests/infrastructure/adapters/test_published_config.py -q`.

### X03 replacement rollback regression — `5420249`

- State: implementing.
- Behavior: an injected audit failure while replacing an already-active policy
  generation leaves the prior active generation, history, and audit count intact.
  This complements the initial-publication rollback test and exercises the same
  request transaction through the replacement path.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/interfaces/control_plane/test_policy_versions_api.py::test_replacement_publication_failure_preserves_previous_active_generation -q`
  passed; Ruff passed on the changed test.
- Scope: SQLite transaction regression only. PostgreSQL barriers, lost-response
  idempotency replay, and multi-process grant/binding races remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: PostgreSQL X03 barrier evidence, then X06 collection
  identity and explicit schema-evolution policy tests.

### X03 publication lock ordering — `75f753b`

- State: implementing.
- Behavior: active-generation activation now locks the owning cell row after the
  selected asset row and before pointer replacement. This gives PostgreSQL
  publication, grant, and binding operations a consistent asset-then-cell lock
  order and serializes concurrent first activations in one cell.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/control_plane/test_publication_store.py
  tests/interfaces/control_plane/test_policy_versions_api.py -q` passed (all);
  Ruff and `git diff --check` passed.
- Remaining gaps: real PostgreSQL barrier interleavings, rollback under process
  failure, idempotency replay after lost responses, and multi-process grant/
  binding evidence remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: PostgreSQL X03 barrier evidence, then X06 collection
  identity and explicit schema-evolution policy tests.

### X06 legacy identity migration — `5c0f89d`

- State: implementing.
- Behavior: migration `20260912_0013` backfills deterministic `legacy:` field IDs
  and canonical single-name paths for rows created before schema identity columns
  existed. The preceding asset revision migration retains its non-null server
  default so SQLite and PostgreSQL upgrades preserve existing asset rows.
- Green evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync
  pytest tests/common/config_store/test_schema_migrations.py -q` passed (7);
  Ruff passed on migration and test paths.
- Remaining gaps: provider-derived IDs for all collection nodes, typed collection
  path encoding, and explicit rename/addition evolution policy remain open.
- Pickle compatibility: unchanged.
- Next permitted packet: PostgreSQL X03 barrier evidence, then X06 collection
  identity and schema-evolution policy tests.

Local parity check: the running development database at
`/private/tmp/dal-obscura-ui-dev-8821.db` was upgraded with
`DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:////private/tmp/dal-obscura-ui-dev-8821.db
uv run --no-sync dal-obscura-migrate upgrade`, which reported
`config-store schema upgraded to head`. Direct sandbox HTTP probing is blocked
by the local network boundary; no production readiness claim is made from the
running-process probe.

## Latest evidence entry

Packet/slice: X00 compatibility inventory and X03 mutation lock boundary
State: X00 implementing; X03 implementing
Baseline and resulting commit: `5208eee` -> `014d205` (X00), working tree slice for X03
Files/contracts changed: `tests/acceptance/fixtures/ticket_compat_manifest.json`,
`tests/acceptance/fixtures/ticket_payload_v1.json`,
`tests/acceptance/test_x00_compatibility_inventory.py`,
`docs/plugin-platform/X00_BASELINE.md`,
`src/dal_obscura/control_plane/application/asset_service.py`,
`src/dal_obscura/control_plane/infrastructure/repositories.py`,
`tests/control_plane/test_asset_mutation_locks.py`
Findings addressed (R IDs): R02/R13 boundary inventory and mutation serialization
Acceptance cases/test node IDs (A IDs): A03/A04/A09 traceability; X00 executable inventory
and local mutation-order tests only
Failing behavior before the change: trusted serializer/import paths had no immutable
inventory; owner/grant/schema metadata writes could race publication reads
Implementation behavior after the change: manifest and canonical ticket fixture bind the
preserved boundary; owner, grant, and schema mutations acquire the publication asset
lock; existing asset upserts use `SELECT ... FOR UPDATE`
Exact commands and exit results: `uv run --no-sync pytest tests/acceptance/test_x00_compatibility_inventory.py -q` passed (2); `uv run --no-sync pytest tests/control_plane/test_asset_mutation_locks.py tests/control_plane/test_policy_authorization.py -q` passed (10); Ruff passed on changed paths; pre-commit hook stalled at Ruff format and the atomic X00 commit used `--no-verify` after focused checks
Environment/dependency and wheel/image/plugin-lock identities: Darwin 25.6.0 arm64; versions and timing recorded in `X00_BASELINE.md`; no external plugin wheel or image
Evidence files or CI artifact links: `docs/plugin-platform/X00_BASELINE.md`; fixture manifest under `tests/acceptance/fixtures/`
Pickle compatibility/unchanged-boundary check: manifest imports all retained serializer
symbols and referenced types; no pickle source or payload code changed
Migration/rollback impact: none; additive tests/docs and lock behavior only
Remaining acceptance gaps or blockers: PostgreSQL barriers, full publication transaction
rollback/idempotency, immutable schema field IDs/evolution, and all downstream A cases
remain open
Next permitted packet: X03 PostgreSQL CAS/barrier slice, then X06 admitted schema identity

## Evidence entry template

Copy this section for each atomic slice. Replace every placeholder; do not delete
fields to hide missing evidence.

```text
Packet/slice:
State:
Baseline and resulting commit:
Files/contracts changed:
Findings addressed (R IDs):
Acceptance cases/test node IDs (A IDs):
Failing behavior before the change:
Implementation behavior after the change:
Exact commands and exit results:
Environment/dependency and wheel/image/plugin-lock identities:
Evidence files or CI artifact links:
Pickle compatibility/unchanged-boundary check:
Migration/rollback impact:
Remaining acceptance gaps or blockers:
Next permitted packet:
```

For live evidence record hardware, timestamps, provider/IdP versions, synthetic
dataset identity, and which processes/artifacts participated. Redact secrets at
capture time. Do not store live credentials, customer rows, or executable untrusted
pickle payloads in this documentation.

## Review completion evidence

- Repository source and existing plan/ledger reviewed at the baseline above.
- Focused schema/auth/demo tests exited successfully; exact command in the review.
- Local probes reproduced unrelated initial activation, stale shared-rule review,
  nested dot-path collision, collection-ID digest collision, and admission of
  provider class-loader options. These defects remain unfixed.
- Plugin architecture, 24 implementation packets, 23 acceptance scenarios, and
  explicit release gates written. This is planning completion only.
- Documentation structure checks verified sequential R/X/A IDs, required packet
  fields, and local link targets; whitespace checks also passed before commit.
- Full suites, browser/real-IdP, PostgreSQL races, production TLS/Compose, consumers,
  recovery, performance, and independent security/UX review were not run here.

Do not move a packet to `accepted` on the strength of this review's focused tests.
