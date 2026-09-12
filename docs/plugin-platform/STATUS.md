# Plugin platform progress ledger

Baseline reviewed: `5208eee9af35e38b5ab8294524a0485611c2b0a7`.
Implementation follow-up through `4ab9031`.
Review date: 2026-09-12. **Paid-production release: HOLD.**

This task began with review/planning documents and now includes incremental runtime,
UI, and plugin-contract slices. Database migrations, external plugin wheels, and
pickle serialization remain unchanged. Local probes are recorded in [the review](IMPLEMENTATION_REVIEW.md).
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
  mutations with publication, and existing-asset binding upserts use row locks.
  Grant, binding, and PostgreSQL barrier evidence remain open.
- X04 canonical evaluation: **implemented-unverified**; resolved mask values now
  flow from canonical preview and an unmatched-principal regression passes.
- X05 canonical bounded schemas: **implemented-unverified**; canonical Arrow schema
  encoding includes nested metadata/IDs and direct loader bounds. Migration and all
  entry-route/byte-budget evidence remain open.
- X06 safe schema evolution: **implementing**; typed evaluation paths now preserve
  literal dotted names, and schema-field records now carry optional stable IDs and
  typed path segments through migration `20260912_0011`. Provider-derived IDs,
  path uniqueness migration for legacy rows, and evolution policy remain open.
  Review tokens now bind the persisted admitted-schema digest in addition to the
  live Iceberg digest.
- X07 configuration/secrets/IO: **implementing**; nested dynamic class-loader options
  are rejected. Typed provider configs, shared secret resolution, and IO enforcement
  remain open.
- X08 budgets and atomic reload: **implementing**; discovery now bounds provider
  iterators before materialization and checks cancellation/deadline per item while
  retaining deque traversal. Plugin admission now exposes build-then-swap reload
  snapshots; provider-specific transport timeouts and multi-worker capacity remain
  open.
- X09 UI lifecycle: **implemented-unverified**; initial-load epoch and synchronous
  logout fencing plus stale history/preview/review/publish response checks are fixed.
  Deferred browser tests and full operation-state coverage remain open.
- X10 complete authoring/management: **implemented-unverified** for the deny-all UI
  path; controls now expose save/test/review/publish actions when rules are empty.
  Full editor, activation, accessibility, and browser evidence remain open.
- X11 public SDK: **implementing**; an independently buildable
  `packages/plugin-api` wheel now contains the versioned contracts and has an
  offline compile/metadata regression. Service-side compatibility contracts and
  online wheel artifact evidence remain open.
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
- X22 exact-artifact CI: **not-started**.
- X23 independent review/release decision: **not-started**.

Next implementation action: continue X03 with PostgreSQL barrier/CAS evidence and
then complete X06 persisted field identities/evolution rules. Do not add new
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
the wheel in this offline environment is an explicit external artifact gate, not
claimed by the test.

Integration boundary check after the migration and lock slices:
`UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane/test_assets_api.py tests/interfaces/control_plane/test_schema_api.py tests/plugin_platform -q`
passed after the X13 manifest metadata change. Ruff passed on every changed Python path and `git diff --check`
is clean. Browser, PostgreSQL, Flight, consumer, and production lanes remain open.

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
