# Implementation reconciliation — 2026-09-13

Reviewed source baseline: `c46415282a2a796cf737a9c3b7f7ba941f19bf7b`.
Documentation-only review. No runtime, UI, dependency, database or executable test
changes. Paid-production release: **HOLD**.

## Conclusion and evidence limits

The project has substantial governed gateway and plugin functionality. Recreating
publication, schema identity, plugin loading or session storage would duplicate
completed work. Priority now: close identity/pairing/UI defects, delete obsolete
runtime branches, finish management journeys, qualify real deployments/consumers.

Source, tests, manifests, CI and previous ledgers were inspected. Application,
integration suites, benchmarks and hosted CI were not rerun. “Implemented” means
source plus relevant tests exist, not a newly observed pass. Archived evidence
records a socket-enabled Python suite pass, PostgreSQL checks and a local
Python/DuckDB probe. The consumer probe uses StubTableFormat and local JWTs;
descriptor intersection checks do not qualify independent wheels/providers.

The old plan said all X packets were not-started, while the ledger recorded many
implementations. Its conclusion still called repaired baseline defects unfixed.
Only remaining N01–N16 work is active now. Old A01–A23 guarantees remain regression
or release obligations. Archived documents are evidence, not competing queues.

## Reconciliation of all 24 old packets

Remove the following completed implementation steps from the active backlog.
None of these statements certifies the entire original packet or release.

- **X00:** baseline fixtures, acceptance registry and constraints exist in
  tests/acceptance. Remove baseline creation. N01 records the new candidate.
- **X01:** selected-only publication exists in policy_version_service and API tests.
  Remove initial repair. Retain G01 regression and N12 live qualification.
- **X02:** saved-draft review hashes, schema and catalog revisions exist. Remove
  initial implementation. N03/N09/N12 own mandatory preconditions, UX and races.
- **X03:** row locks, revision CAS, operation idempotency and PostgreSQL barrier
  tests exist. Remove primitive creation. N03/N12 close remaining contracts/proof.
- **X04:** canonical masks and typed synthetic fixtures exist. Remove evaluator
  repair. Retain G02 regression; qualify actual reads in N12/N13.
- **X05:** shared schema budgets/fingerprints exist. Remove original digest work.
  Retain G02; measure complete entry paths in N14.
- **X06:** admitted IDs, provider IDs, synthetic scoping and drift rejection exist.
  Remove original path/ID implementation. N12/N13 prove live evolution/tickets.
- **X07:** loader rejection, bounded options, scoped references and egress validators
  exist. N02/N04 remove fallback and qualify transport enforcement.
- **X08:** discovery budgets, principal/process admission, atomic reload and cleanup
  exist. Remove initial semaphore/reload work. N04/N14 prove provider/worker bounds.
- **X09:** private-page gates, 401 clearing, hash restoration and some epoch guards
  exist. Remove those initial tasks. N05/N07 fix uncovered operations and test UI.
- **X10:** conditions JSON, reordering, six masks, deny-all, grant/connection forms
  and activation controls exist. N08–N11 complete usable, lossless workflows.
- **X11:** standalone SDK/conformance packages exist. Remove scaffolding.
  N02 consolidates duplicate contracts; N13 qualifies built artifacts.
- **X12:** admission, lock builder, descriptors and lifecycle exist. Remove
  scaffolding. N02 removes short locks; N03 fixes pairs; N13/N15 qualify artifacts.
- **X13:** both planes route through registry/public adapters and publish IDs/
  revisions. Remove original routing/backfill work. N02/N03 finish canonical APIs.
- **X14:** descriptor forms, lifecycle status and scoped secret inputs exist.
  Remove initial form/status tasks. N10 completes editing, types and lifecycle.
- **X15:** reusable conformance runner and wheel CI exist. N13 executes real and
  deliberately faulty distributions; do not create another harness.
- **X16:** REST Iceberg package/lifecycle exists. Remove package creation.
  N13 owns live provider/TLS/delete/snapshot qualification.
- **X17:** manifest/Parquet package, path bounds and cleanup exist. Remove package
  creation. N13 owns independent-wheel/provider/consumer qualification.
- **X18:** Python/DuckDB smoke and CI lane exist. Remove scaffolding.
  All real three-pair/TLS/OIDC/Spark cells remain N13 work.
- **X19:** local/production profile validation, TLS edge, OIDC/session and bootstrap
  paths exist. N05/N15 close identity/parity requirements.
- **X20:** encrypted-backup helpers, checksum tests and access invalidation exist.
  Remove helper creation. N15 performs timed real restore/rotation.
- **X21:** capacity runner/inventory exist. Remove harness creation.
  N01/N02 enable real cleanup; N14 measures capacity and simplifies tests.
- **X22:** no-skip/audit/wheel/image/manifest CI exists. Remove scaffolding.
  N15 links all mandatory lanes to exact candidate artifacts.
- **X23:** release remains open. N16 owns independent security/UX acceptance.

## Findings still requiring action

### F01 — Identity encoding violates exact-issuer intent (high)

src/dal_obscura/control_plane/application/access.py::owner_principals and
identity_key strip issuer trailing slashes and concatenate delimiter strings.
Exact issuer identity must be preserved and encoding must be unambiguous.
Provider admission guards may limit exploitability; no exploit is claimed here.
N05 requires structured issuer/type/subject identities and explicit migration.
The UI currently repeats the same normalization in AccessView identity hints.

### F02 — Shared capabilities incorrectly imply compatible formats (high)

routes/plugins.py::_pair_payload and application/asset_service.py admit pairs
based on any shared capability. main.tsx::ConnectionsView.govern chooses the first
admitted format. Nested-schema support does not prove format compatibility.
N03 must declare catalog output formats, verify returned handles and require
explicit selection when multiple formats are supported.

### F03 — Stale mutation responses remain possible (high)

main.tsx::restorePolicyVersion has no captured session/asset/edit guard.
publishAsset checks scope before operation lookup but not after successful lookup;
finally clears pending state unconditionally. Connections discovery applies tables
without checking that the selected catalog is unchanged. Initial-load error handling
awaits options without another epoch check. N07 must test these rendered workflows.

### F04 — Permission/error and management workflows remain incomplete (high)

loadManagement converts failure to cleared data; child views can show empty lists
or default runtime values. SettingsView lists identity providers but cannot edit
them despite an existing backend PUT. Grant controls are always rendered; owner
controls inspect only platform_admin. N05/N11 require effective capabilities,
distinct forbidden/error/empty states and last-admin-safe identity management.

### F05 — UI remains a functional prototype (medium)

main.tsx has 769 lines with many large one-line components and manual form,
routing and request state. styles.css has 98 dense global-rule lines; under 760px
it hides .actor, including sign-out. Navigation has no icon library or semantic
theme tokens. Conditions use raw JSON; connection editing requires retyping values.
N06–N11 specify the complete replacement experience. No live visual review was
performed in this review.

### F06 — Duplicate contracts and fallback paths increase complexity (medium)

common/plugin_api/contracts.py and the standalone SDK duplicate descriptor/config/
identifier/handle/schema/context validation. Registry accepts three- and five-part
locks. UI/API use module aliases and built-in fallback descriptors. Secret resolution
accepts scope-less references. N02 consolidates non-serialized contracts and removes
old paths with their callers. Keep the public adapter's security validation function;
do not confuse necessary boundary enforcement with a compatibility shim.

### F07 — Cleanup tests protect obsolete implementation (medium)

test_dead_code_inventory.py permits only KEEP dispositions and asserts the sentence
“No module currently has sufficient evidence for deletion.” Tests for operator docs
and capacity runbooks inspect prose/script substrings. UI lifecycle tests prove epoch
arithmetic, not component behavior. N01/N14 replace redundant tests with owned
behavioral checks; never delete a unique security oracle merely to lower counts.

### F08 — IO qualification does not prove actual destinations (high release gap)

tests/integration/test_io_boundary.py exercises validators without live redirects,
DNS changes or denied-destination counters. Deadline checks cannot alone interrupt
blocking provider IO. N04 requires transport/network enforcement; N14 measures
multi-worker bounds. Installed Python plugins are trusted code, not SDK-sandboxed.

### F09 — Several evidence labels exceed the exercised scope (high release gap)

test_pairs.py compares descriptors. The consumer probe uses a stub format, two rows
and a DuckDB count. PostgreSQL barriers use threads/independent sessions at repository
level rather than competing API processes. Recovery checks exercise helper/invalidation
behavior rather than timed encrypted restore. N12/N13/N15 retain the missing evidence.

### F10 — Toolchain and testing require consolidation (medium)

Python metadata advertises >=3.10; CI uses 3.12. UI ranges start at React 19.1,
Vite 7.1 and TypeScript 5.9; the lock determines resolved versions. CI uses Node 22.
Pre-commit invokes broad tests and type checks on every commit. N01/N14 align the
support matrix and shorten feedback. Do not infer installed age from lower bounds.

## Handoff

Use [remaining packets](IMPLEMENTATION_PLAN.md), [acceptance](ACCEPTANCE.md),
[UX requirements](UX_REQUIREMENTS.md), [technology](TECHNOLOGY.md),
[cleanup](CLEANUP_PLAN.md) and [status](STATUS.md).
[Previous review](IMPLEMENTATION_REVIEW_ARCHIVE_20260913.md) and
[previous ledger](STATUS_ARCHIVE_20260913.md) retain historical evidence.
