# Ordered implementation packets

Baseline: `5208eee`. Read [architecture](ARCHITECTURE.md),
[review](IMPLEMENTATION_REVIEW.md), and [acceptance cases](ACCEPTANCE.md) first.
All X packets start **not-started**. Existing code is reusable input, not completion.

## Execution rules

**Packet acceptance versus product acceptance:** a packet's “Acceptance” paragraph
defines exactly the work it owns. Its A IDs are traceability links to broader
scenarios, which may span several packets. Record the covered subcases explicitly;
do not mark an entire A scenario passed from a partial packet. Component tests can
accept X09's local lifecycle behavior while its real-browser/IdP subcases remain
open for X19/X22. X22 assembles all complete scenarios; X23 alone closes the product
release gate. A later gate is never a prerequisite for an earlier packet unless
expressly listed in that packet.

1. Execute in numerical order unless the prerequisites explicitly allow otherwise.
   Do not start a dependent packet on an unaccepted abstraction. Splitting a packet
   into X02.1/X02.2 is allowed; deleting an acceptance criterion is not.
2. Before each slice, inspect current callers and existing tests; record the
   expected behavior and negative cases. Add a meaningful failing regression first.
   Respect existing owner implementation authorization; request clarification only
   for a genuinely unresolved scope/security decision, not routine implementation.
3. Use existing application services and adapters where correct. Never write a second
   policy evaluator, hidden browser-only authorization path, or parallel config store.
4. Make one coherent behavior change per commit, including its tests and evidence.
   Use Conventional Commits. Do not combine unrelated cleanup with security fixes.
5. Run the focused test paths named by the packet, Ruff on changed Python, and type
   checks when contracts change. Update the ledger with commands, outcomes, commit,
   and remaining evidence. A green source-string assertion cannot replace a runtime
   scenario. A skipped integration test is **unverified**, not passed.
6. Do not repeatedly rerun the full suite after documentation or unrelated changes.
   Run broader checks at integration boundaries and for the final candidate. Use
   temporary isolated fixtures; no tests against customer catalogs or production DBs.
7. If blocked by a stopped container runtime or missing credentials, complete
   independent local work, record the exact blocked gate, and leave it open. Do not
   replace PostgreSQL with SQLite or OIDC with a fake for the final acceptance gate.

Existing command conventions:

```sh
uv sync --dev --frozen
uv run pytest <focused-test-path> -q
uv run ruff check <changed-python-paths>
uv run ty check
pnpm --dir apps/governance-ui check
pnpm --dir apps/governance-ui build
mvn -f connectors/jvm/pom.xml verify
```

`<...>` is a placeholder to replace, never a literal shell argument. Use the
repository's pinned package manager. `pnpm test`, `test:e2e`, and the plugin
conformance runner below are **new deliverables**, not commands already available.

## Phase A — Repair the existing governed workflow

### X00 — Establish reproducible baseline and immutable constraints

**Prerequisites:** none. **Findings:** all; especially R10/R15.

- Read existing UI P00–P16, gateway status, routes, config migrations, and tests.
  Record which requirements are satisfied, partial, and contradicted by this review.
- Create isolated review regression fixtures. Preserve trusted baseline ticket/task
  fixtures and a manifest of serializer functions and pickle-referenced import paths.
  Record dependency versions, hardware, test timing, and exact baseline commit.
- Add an acceptance-case registry under `tests/acceptance/` mapping the A IDs below
  to executable tests or explicit unverified integration gates. X00 must not mark
  expected-to-fail defects fixed. Keep baseline diagnostic probes separate from the
  default passing suite; add red/green repair tests in their implementation slice.
  Add no empty tests or blanket `xfail` closure, and do not commit a broken default suite.
- Record the default resource limits in the architecture contract as targets; obtain
  baseline measurements before changing them. Document single-customer deployment
  and the trusted-plugin/pickle boundary in the security guide.

**Acceptance:** every R01–R15 has an owner packet; all A cases have a planned test
location; baseline fixtures use only synthetic data; existing pickle code is
unchanged; an isolated diagnostic reproduces R01 or R02 and records the expected
fixed outcome for its next red/green implementation slice.
Commit: `test(review): establish governed workflow regression baseline`.

### X01 — Publish only the selected asset

**Prerequisites:** X00. **Findings:** R01. **Cases:** A01.

- Extend `tests/interfaces/control_plane/test_policy_versions_api.py` to publish A
  in a workspace containing B, including B with incomplete rules/owners.
- Change `policy_version_service.create_asset_policy_version()` so the first
  generation includes only the selected asset and necessary validated configuration.
  Preserve subsequent selected-asset behavior and immutable historical records.
- Validate readiness for the actual candidate, not all workspace drafts. Add an
  audit assertion for the exact activated target set.

**Acceptance:** A01 passes for initial/subsequent publication and intentional empty
draft; B never appears in the active generation or becomes readable until separately
published. Required missing config fails without inserting/activating a generation.
Commit: `fix(publication): restrict initial activation to the reviewed asset`.

### X02 — Bind evaluation and review to an immutable saved draft

**Prerequisites:** X01. **Findings:** R02. **Cases:** A02/A03.

- Add regressions in `test_schema_api.py` and a new
  `tests/control_plane/test_review_integrity.py` for the shared-rule mutation probe.
- Require an explicitly saved personal draft for strict review/publication. Migrate
  legacy edit workflows to revisioned drafts or reject them in strict mode; do not
  silently import shared rules after issuing review.
- Load a single evaluation input snapshot. Pass it through canonical evaluation,
  token issuance, and candidate construction. Verify evidence hashes match the exact
  snapshot rather than rereading and relabeling an earlier evaluation result.
- Bind actor/asset, draft revision/hash, persona/fixture/evaluator, schema digest,
  active publication, connection/asset revisions, and supported plugin requirements.
  Reserve additive metadata now; X05/X13 complete canonical schema/plugin values.
- Define omitted-vs-empty rows and explicit deny-all review semantics in API docs.

**Acceptance:** A02/A03 sequential interleavings fail closed; token body matches
evaluated input; no-personal-draft strict review is rejected; old/tampered/cross-actor
tokens fail; a reviewed intentional empty draft remains publishable. X03 must still
close multi-process transaction races. Commit: `fix(review): bind approval to saved policy content`.

### X03 — Serialize publication with draft, grant, and binding changes

**Prerequisites:** X02. **Findings:** R02/R13. **Cases:** A03/A04/A12.

- Add real PostgreSQL barrier-controlled tests under
  `tests/integration/control_plane/test_publication_races.py`; use independent
  sessions/processes. Specify a consistent lock order to avoid deadlocks.
- Add revision/CAS protection for full-list grant and asset-binding updates. All
  mutations participating in review validity must share a transaction/generation
  protocol. Recheck publish permission and expected generations before commit.
- Keep remote schema/IO outside long DB locks; validate the resulting identity with
  the immutable candidate and enforce the admitted schema again when planning reads.
- Make activation, audit, and idempotency operation recording one transaction.
  Distinguish successful replay from a new request reusing the key with other content.
- Record policy for grant-manager delegation; enforce its bounded capabilities in
  backend tests. Preserve existing explicit publish capability requirements.

**Acceptance:** in every A03 interleaving exactly one compatible result commits or
the operation conflicts; no stale candidate activates; grant revocation ordered
before commit prevents publication; transaction failure leaves no partial audit or
operation. Two concurrent first publications do not lose/activate unrelated assets.
Commit in separate draft/grant CAS and publication-transaction units.

### X04 — Make synthetic evaluation match the data plane

**Prerequisites:** X02; X03 may remain in progress only for independent tests.
**Findings:** R03/R04. **Cases:** A05/A06.

- Add `tests/control_plane/test_evaluation_service.py` with explicit golden rows,
  unmatched principal masks, conflicting `keep_last`, every supported mask, row
  restrictions, empty outputs, and invalid typed fixtures.
- Reuse complete canonical resolved masks; remove raw-rule `setdefault` reconstruction.
  Use existing typed field paths. Do not introduce separate SQL escaping logic.
- Generate valid fixture values for each supported scalar/container type. Preserve
  empty output schema, distinguish no fixture from an intentionally empty fixture,
  and catch Arrow construction/serialization failures at a redacted boundary.
- Hash exact fixture input and persona into evidence. Admit evaluation through
  shared bounded execution; X08 supplies common process-wide machinery.

**Acceptance:** A05/A06 compare values AND types, not merely decision strings.
Unmatched masks cannot affect output. Unsupported values return a safe 4xx/error
code and issue no review token. No real customer row scan occurs during evaluation.
Commit: `fix(evaluation): reuse canonical policy and typed synthetic fixtures`.

### X05 — Canonicalize and bound all schema operations

**Prerequisites:** X04. **Findings:** R04/R05. **Cases:** A06/A07.

- Extend `test_schema_service.py`, `test_field_paths.py`, and schema API tests.
- Implement the versioned schema encoding in the architecture contract; separate
  response version, schema identity, and data snapshot identity. Include collection
  child IDs and precise types. API, evaluator, reviewer, and planner use the same digest.
- Enforce node/depth/byte limits before recursive projection/fingerprinting/Arrow
  conversion at the common loader boundary. Preserve escaped literal names.
- Introduce additive metadata migrations if required; reject old review evidence
  with a clear “review again” outcome rather than treating old digests as equivalent.

**Acceptance:** A07 detects each individual semantic change; equivalent schemas
produce identical digests; every entry route enforces bounds. Dot/reserved-character
paths remain distinct. Old published data remains readable under the documented
compatibility policy; no pickle class or blob changes. Commit schema encoding and
migration as separate coherent units if necessary.

### X06 — Prevent implicit access expansion during schema evolution

**Prerequisites:** X03/X05. **Findings:** R05. **Cases:** A08.

- Persist reviewed admitted field identities, schema digest, and target binding
  through an additive migration and compiler/publication metadata.
- Expand wildcard/parent selections at approval. Before planning, validate current
  identity and admit only that set. Reject changed IDs/types/binding; do not silently
  grant newly added children. Define rename behavior explicitly in tests.
- Update both authorization and current-policy-version paths. Expose schema drift
  and reapproval in the UI without a browser-generated security decision.

**Acceptance:** A08 proves forbidden newly added/rebound fields never reach any
consumer; harmless data snapshot advances remain consistent under the chosen
schema policy. Formats lacking stable IDs require reapproval on schema change.
Commit: `fix(policy): bind schema grants to reviewed field identities`.

### X07 — Close configuration-loader, secret, and IO boundary gaps

**Prerequisites:** X00; integrate with X05/X06 before acceptance.
**Findings:** R06/R07. **Cases:** A09/A10.

- Add adversarial catalog/config tests for loader keys and sensitive provider-specific
  fields, including nested metadata overrides. Define typed SQL/REST Iceberg options;
  reject unknown keys instead of forwarding arbitrary dictionaries.
- Create one scoped connection/secret/IO service, used first by existing Iceberg
  discovery and schema paths. Use safe provider-specific credential construction.
- Apply network/path policy to initial and returned locations, redirects, file roots,
  DNS resolution, alternate endpoints, and retries. Add OS/container egress proof for
  constraints the SDK cannot enforce. Do not claim Python wrappers isolate bad code.
- Test sentinel secret redaction in HTTP/Flight, logs, traces, audit, and UI; keep
  trusted legacy serialized objects unchanged and document their exposure separately.

**Acceptance:** A09/A10 pass without importing the probe class or accessing a denied
destination; valid secret-reference SQL/REST connections work in both planes;
unauthorized actors cannot resolve a secret or discover metadata. Commit loader
rejection, shared secret resolution, and IO enforcement in separate units.

### X08 — Bound discovery/evaluation and make reload atomic

**Prerequisites:** X04/X07. **Findings:** R04/R08. **Cases:** A11/A17.

- Route the real discovery entry point through bounded incremental traversal. Remove
  or consolidate the bypassed helper only after its callers/tests are migrated.
- Add page/byte/deadline/cancellation and shared admission primitives; use a deque
  for bounded traversal. Test cyclic/repeated provider continuation tokens.
- Build immutable catalog generations before swapping. Add failure cleanup and
  concurrent request tests. Explicit revoke cannot fall back to an old allowed generation.
- Ensure provider timeout stops or terminates work and releases resources; a timed-out
  response with an accumulating background thread is a failing result.

**Acceptance:** A11/A17 stop upstream work at configured limits, release slots, and
serve subsequent valid requests. Failed reload exposes no mixed configuration;
revocation prevents new admissions. Commit resource admission separately from registry reload.

### X09 — Repair UI lifecycle with behavioral tests

**Prerequisites:** X00; integrate with X02/X03 before acceptance.
**Findings:** R11/R15. **Cases:** A12/A13.

- Add a pinned behavioral test runner and `pnpm test` command. Extract API/session,
  asset loading, draft editing, and publication state from `main.tsx` into focused
  modules without redesigning the whole application again.
- Define operation identities including session, asset, draft edit revision, and
  persona. Ignore stale success and failure responses. Fix nested load epochs.
- Clear and fence private UI state synchronously on logout; retry server revocation
  honestly when it fails. Guard management requests, pagination, saves, tests,
  reviews, restores, and publication against stale scope and duplicate actions.

**Acceptance:** controlled deferred promises demonstrate every A13 ordering; first
load reaches ready; older saves cannot mark newer edits saved; logout cannot be
undone by a late response. Add browser coverage in X22; mocks alone do not close it.
Commit lifecycle fixes separately from mechanical component extraction.

### X10 — Complete the existing policy editor and management semantics

**Prerequisites:** X03/X04/X06/X09. **Findings:** R12. **Cases:** A14/A15.

- Enable deliberate empty-policy save/review/publication with clear deny-all wording.
  Implement rule order, condition editing, typed masks, selected-rule schema state,
  conflict recovery, and complete lossless draft roundtrips.
- Define backend staged/active states for connection, runtime, and auth configuration;
  implement revisioned activation, impact display, safe rollback, and audit. Include
  bootstrap recovery for auth changes; never strand the only administrator silently.
- Distinguish failed fetch, loading, empty, disabled, and permission-denied views.
  Add conflict-safe grants and repair/disable workflows without source-data deletion.
- Provide measured virtualized nested navigation and keyboard/focus behavior. Keep
  React/TypeScript/Vite and the current application server/database.

**Acceptance:** A14/A15 exercise UI actions against authoritative backend behavior;
no visible action is a no-op; all supported policy constructs roundtrip unchanged;
deny-all revokes reads; activation changes the running generation or clearly reports
it has not been observed. Existing UI P04–P10 requirements remain mandatory.

## Phase B — Extract an extensible, governed plugin platform

### X11 — Publish a versioned SDK contract and dependency boundary

**Prerequisites:** X01–X10 accepted. **Findings:** R09. **Cases:** A16/A18.

- Create the independently buildable `packages/plugin-api/` and implement exactly
  the architecture contract's descriptors, identifiers, handles, schema data,
  contexts, errors, catalog Protocol, and format factory interface.
- Add conversion adapters to existing core ports; keep policy/SQL path parsing in
  core. Document each field, nullability, lifetime, ownership, and failure outcome.
- Specify request-context injection by the core wrapper: live connections/contexts
  never become serialized task fields. Third-party tasks use bounded internal plan
  data and scoped references through the unchanged serializer. Test this contract
  without altering legacy task classes or captured baseline fixtures.
- Add architecture tests proving the SDK imports without PyIceberg/FastAPI/SQLAlchemy
  or private core implementation modules. Create an API compatibility/version policy.

**Acceptance:** SDK wheel installs and imports in a clean environment with only its
declared dependencies; fixture catalog/format contracts type-check independently;
unsupported required capabilities fail before planning. No runtime behavior or
pickle path is removed. Commit: `feat(plugins): define versioned catalog and format SDK`.

### X12 — Implement admitted entry-point loading and wrap built-in Iceberg

**Prerequisites:** X11. **Findings:** R09/R10. **Cases:** A16/A18/A22.

- Implement static descriptor discovery, plugin lock validation, duplicate detection,
  allowlisted factory loading, lifecycle cleanup, and atomic registry generations.
- Register the current SQL Iceberg adapter through these factories. Preserve exact
  legacy task import paths with a tested compatibility facade; no serializer edits.
- Build the Iceberg distribution independently or provide a transitional bundled
  distribution with the same public entry-point contract. Record that transitional
  packaging does not yet prove independent external extensibility.
- Report supported/installed/enabled/incompatible states safely; startup cannot
  silently choose a different plugin when one is missing or incompatible.

**Acceptance:** A16 proves unapproved packages never import, conflicts fail closed,
and no runtime downloads occur; old trusted task fixtures still execute unchanged;
Iceberg semantic and parallel-planning suites pass. Commit loader and adapter wiring
separately. Default user-visible backend behavior remains qualified Iceberg.

### X13 — Route both planes through plugins and migrate stable IDs

**Prerequisites:** X07/X12. **Findings:** R07/R09. **Cases:** A09/A15/A18/A22.

- Add plugin/config/binding revision columns or typed records in the existing store;
  update compiler, published config, API models, catalog discovery, schema loading,
  diagnostics, and planner to resolve the same admitted plugin instances.
- Replace hardcoded Iceberg branches with capability/registry calls outside the
  compatibility adapter. Keep only exact legacy mappings and their tests.
- Provide explicit migration dry-run/apply with unsupported-row reporting. Preserve
  historical publications and restart behavior. Bind review evidence to relevant
  plugin/config generations; resolve secrets by the same scoped path in both planes.

**Acceptance:** the same config yields identical schema/table identity in UI review
and runtime reads; configuration switches invalidate stale evidence; missing/disabled
plugins fail closed; migration is restart-safe and reversible through documented
backup/forward-compatibility procedure. No caller-controlled module string is imported.

### X14 — Make plugin configuration and capabilities usable in the UI

**Prerequisites:** X10/X13. **Findings:** R09/R12. **Cases:** A14/A15/A16.

- Add authenticated descriptor/pair-capability API with bounded declarative form
  data. Render admitted connection types, secret references, errors, unsupported
  features, staged activation, and lifecycle status without hardcoded SQL forms.
- Backend revalidates every field/capability; UI labels never grant authority.
  Reject remote form references, script/HTML content, excessive nesting, and fields
  outside the supported form subset. Never expose package installation controls.
- Use the same authoring/evaluation/review/history components for every qualified
  pair. Display schema IDs/drift and plugin compatibility in appropriate operator views.

**Acceptance:** adding the X16/X17 wheel and descriptor makes its form usable without
editing UI source; unauthorized management returns the documented denial; unsupported
capabilities cannot be submitted by bypassing the form. Browser tests use real APIs.

### X15 — Deliver a reusable plugin conformance kit

**Prerequisites:** X11–X14. **Findings:** R09/R15. **Cases:** A05–A11/A16–A19.

- Create `packages/plugin-conformance/` with reusable parametrized fixtures, golden
  nested datasets, forbidden outputs, capability-negative cases, planning counters,
  failure injection, cleanup assertions, and machine-readable result output.
- Separate pure contract tests, real provider tests, governed Flight tests, and
  consumer tests. Publish exact local commands and fixture lifecycle; never require
  access to a maintainer's private environment.
- Include deliberately incorrect fixture plugins that overclaim capabilities,
  change schema, duplicate/omit partitions, ignore cancellation, or return deleted
  rows. This tests the suite and core defenses; it is not a hostile-code sandbox test.

**Acceptance:** conforming Iceberg passes; each deliberately incorrect behavior
fails the corresponding named test; output records package/core/Arrow versions,
capability matrix, failures, skips, and artifact identities. An external package can
invoke the kit without importing private core internals.

### X16 — Qualify a real REST Iceberg catalog package

**Prerequisites:** X15. **Findings:** R09. **Cases:** A09/A10/A18/A19.

- Implement `iceberg.rest` using public SDK/catalog handle contracts and the shared
  Iceberg format. Add bounded authenticated REST fixtures and safe credential/endpoint
  handling. Provider-returned locations remain subject to scoped IO rules.
- Test SQL and REST catalogs serving equivalent nested datasets; include missing
  namespaces, expired credentials, bad metadata, snapshot changes, and delete files.
- Install the built wheel into clean control/data-plane images with pinned dependencies.

**Acceptance:** both real catalog paths pass conformance, UI onboarding, policy
publication, and governed reads; no core/compiler/router/UI branch added for REST.
Unsupported pair requests fail explicitly. Do not advertise REST support before its
live evidence and X18 consumer results are recorded.

### X17 — Prove independent format extensibility with manifest/Parquet

**Prerequisites:** X16. **Findings:** R09. **Cases:** A08/A10/A18/A19.

- Build a separate distribution outside the core source tree containing `manifest`
  catalog and `parquet.dataset` format entry points, depending only on public SDK
  and declared provider libraries. Use operator-controlled immutable membership
  manifests, scoped roots, stable dataset revisions, and pinned schema.
- Implement true file/row-group task splitting; preserve membership consistency and
  Arrow nested types. Reject unsupported transactional/delete/time-travel semantics.
- Use schema-scoped synthetic field IDs and mandatory reapproval on schema change.
  Add traversal, symlink, changed membership, mixed schema, corrupt file, and partial
  scan failure tests. No ungoverned arbitrary-path endpoint is acceptable.

**Acceptance:** the wheel installs, is admitted, appears in UI, passes conformance,
and serves exact governed results without editing the core or UI. Multiple row groups
produce parallelizable tasks with no omission/duplication. Unsupported capabilities
fail before tickets are minted. This proves an extensible API, not universal format support.

## Phase C — Qualify the complete product for operators and consumers

### X18 — Qualify consumer behavior for every advertised pair

**Prerequisites:** X16/X17. **Findings:** R12. **Cases:** A19.

- Run Python/Arrow, DuckDB, and Spark/JVM clients through real TLS/OIDC Flight using
  the same nested golden datasets across all three required pairs.
- Verify every endpoint/partition is consumed once, all six supported masks and
  row restrictions match goldens, denied paths never arrive, and empty/schema-only
  results, timeouts, early close, retries, expiry, and revocation behave correctly.
- Replace snippets with runnable scripts containing dependency versions, CA/client
  certificate requirements, auth input, JAR setup, and stream cleanup. Do not include secrets.

**Acceptance:** exact row multisets, values, and nested types agree across consumers;
resources are released on early cancellation. Record only executed version cells
in `docs/compatibility.md`. Additional frameworks require their own evidence; generic
Arrow interoperability alone is not a tested-support claim.

### X19 — Complete secure local/production startup and identity lifecycle

**Prerequisites:** X03/X10/X13; use X18 artifacts before acceptance.
**Findings:** R13/R14. **Cases:** A12/A20.

- Finish existing UI P00–P03/P11/P13: exact issuer/subject identity migration, real
  IdP login/logout/reauth, role freshness, revocation, trusted proxy/origin/CSRF,
  bootstrap closure, and explicit administration recovery. Record chosen freshness
  bound and demonstrate it across independent processes.
- Separate migration/control/data database roles and prove allowed/denied SQL
  operations. Separate Flight bind and advertised locations; test actual TLS mounts,
  uid permissions, readiness, installed wheels, server/UI images, and startup order.
- Provide one secure local profile using the same auth, TLS/cookie, authorization,
  plugin locks, limits, and migration code. Clearly label any optional insecure demo
  shortcuts; they cannot supply parity evidence.

**Acceptance:** A20 works from clean checkout and installed artifacts without source
patching; restarts preserve records; wrong issuer/audience/origin/role/CA are rejected;
runtime roles cannot migrate; local and production security matrices match. External
IdP operations/contact remain subject to owner authorization.

### X20 — Prove backup, restore, rotation, and plugin upgrades

**Prerequisites:** X12/X13/X19. **Findings:** R10/R14. **Cases:** A22.

- Finish existing P14: encrypted backup and PostgreSQL recovery, isolated restore,
  session/login/ticket invalidation before ingress, key/secret rotation, and rollback.
- Define plugin enable/disable/revoke/drain/remove transitions and immutable release
  environment locks. Removal cannot strand outstanding tickets or historical config
  without an explicit compatibility/drain strategy.
- Test old/new worker mixtures using trusted baseline ticket fixtures and additive
  migrations. Keep serialized object/class paths unchanged; document incompatible
  package combinations and reject unsafe startup instead of changing serialization.

**Acceptance:** restored data and audit history match expected hashes; pre-restore
sessions/tickets cannot be replayed; approved new reads work; measured recovery
objectives are met; failed upgrades recover without policy weakening. An unexecuted
runbook does not close this packet.

### X21 — Measure capacity, simplify verified dead code, and improve test efficiency

**Prerequisites:** X08/X15/X18/X19. **Findings:** R08/R15. **Cases:** A11/A17/A21.

- Profile discovery, schema, planning, ticket exchange, stream/masking memory, and
  UI tree operations. Add bounded metrics/alerts without secrets or high-cardinality
  principal/table labels. Preserve Arrow streaming; avoid collecting all batches.
- Measure test durations and fixture setup. Separate fast unit, provider, PostgreSQL,
  browser, consumer, and benchmark lanes. Replace fixed sleeps with bounded readiness
  polling and avoid rerunning unchanged expensive matrices per small edit.
- Inventory candidate dead/compatibility modules with callers, entry points, docs,
  old-pickle paths, and downstream public API use. Remove only proven redundant code,
  duplicate helpers, and unused dependencies, with focused behavioral coverage.
- Use the architecture limits and A21 workload/thresholds; document bottlenecks and
  measured operational capacity instead of claiming “zero copy” from library choices.

**Acceptance:** A21 targets pass on recorded hardware; no aggregate leaks or unbounded
queues; every deletion has evidence and no pickle path disappears. Durations before/
after show test-efficiency changes without reducing behavioral coverage. Commit
performance, instrumentation, and cleanup as separate units.

### X22 — Gate exact release artifacts in CI

**Prerequisites:** X18–X21. **Findings:** R14/R15. **Cases:** A20–A23.

- Update `.github/workflows/ci.yml` with fast and integration lanes and explicit
  dependencies. Test built SDK/plugin/server wheels and UI/server images, not only
  source checkouts. Build once; test, scan, and promote the same immutable digests.
- Run PostgreSQL race, real IdP/browser, provider/consumer, local TLS parity, and
  recovery gates. Each advertised CPU architecture has executed evidence or is
  removed from the release matrix. Attach SBOM/dependency scan and plugin locks.
- Make missing secrets/runtime, skipped mandatory tests, and mismatched artifact
  hashes fail the release gate. PR quick lanes may report integration pending.

**Acceptance:** a deliberately failing security/conformance/browser case blocks
promotion; all mandatory A IDs have passing evidence for the candidate; UI and
server digests in the release manifest equal tested digests. Do not publish remotely
without existing authorization for that separate action.

### X23 — Complete independent review and release decision

**Prerequisites:** X00–X22 accepted. **Findings:** all. **Cases:** A23.

- Prepare a concise security/UX review bundle: threat model, plugin trust statement,
  unresolved pickle constraint, permission matrix, recorded browser journeys,
  conformance results, capacity, recovery, artifact manifest, and operator runbooks.
- Reconcile every original UI P00–P16/gateway requirement and every R/A ID. Owner
  visual acceptance, usability sessions, and independent security review remain
  explicit external evidence, not fabricated agent approvals.
- Record remaining unsupported integrations and intentional limitations. Keep HOLD
  for any failed mandatory test, open high-severity finding, missing artifact proof,
  or unresolved boundary decision. Prepare the release; deployment is a separate action.

**Acceptance:** all mandatory evidence links resolve to the exact candidate; no
unverified cell is called supported; independent review and owner acceptance are
recorded; operator can install, secure, use, upgrade, and restore the same qualified
artifacts. Mark production-ready only after this gate actually passes.

## Mapping to earlier work

- UI P00–P03: X00/X03/X19; P04: X09; P05–P07: X04–X06/X10.
- UI P08–P09: X01–X03/X10/X13; P10: X10/X14/X18.
- UI P11–P13: X19/X22/X23; P14: X20; P15: X08/X21; P16: X22/X23.
- Gateway planning/nested/consumer requirements: X05/X06/X15/X18/X21.
- Gateway pickle constraint: X00/X12/X20; it remains an explicit constraint,
  not a completed serialization rewrite.

No earlier mandatory acceptance item disappears merely because its source document
has not been copied verbatim. X00/X23 reconcile the inventories and record any
remaining unmatched requirement as an open blocker.
