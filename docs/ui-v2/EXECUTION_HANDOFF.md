# Remaining implementation plan

**2026-09-12 follow-up:** the owner requested a multi-catalog/multi-format plugin
architecture. The [plugin-platform handoff](../plugin-platform/README.md) now controls
the next execution sequence and supersedes this document's Iceberg-only planning
restriction. P00–P16 requirements and historical evidence remain applicable through
its explicit task mapping. The new review does not certify earlier work complete.

Rewritten 2026-09-12 against `8e23c98`. **Implementation incomplete; paid-production
release on hold.** This was the authoritative execution sequence before the plugin
follow-up above. It replaces the
previous handoff and the competing U00–U10 sequence. P00–P16 IDs remain unchanged
so existing evidence stays traceable.

Read [EXPERIENCE.md](EXPERIENCE.md) for product behavior,
[PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) for code-backed findings,
[P00_CONTRACT_REVIEW.md](P00_CONTRACT_REVIEW.md) for unresolved decisions, and
[EXECUTION_STATUS.md](EXECUTION_STATUS.md) for actual progress. This plan controls
execution order if those documents differ. Owner instructions prevail.

## 1. Deliverable and fixed constraints

Deliver a complete UI and backend for a governed Iceberg gateway that an operator
can install, secure, run, upgrade, recover, and support for paying customers. The
supported local environment must execute the same feature and security code.

- Keep Iceberg as the only backend; authoritative nested struct/list/map schemas,
  all six masks, DuckDB row restrictions, Python/DuckDB, Spark/JVM, and Arrow reads
  are required. Individually test other frameworks before advertising support.
- Policy authoring and management UI are required. No placeholder destinations,
  browser-side authorization/evaluation, or mocked acceptance workflows.
- Keep React/TypeScript/Vite and the Python control plane. Reuse PostgreSQL and
  stateless Flight workers; no second application server or database product.
- Preserve pickle-based logic exactly. Restrict and assess its trusted internal
  payload/DB boundary; report blockers without changing serialization.
- Planning scope: one workspace and isolated deployment/database/keys per customer.
  Shared multi-customer SaaS requires an additional isolation contract. Billing
  automation and IdP administration are outside this release.
- Preserve records. Routine start/restart cannot reset, reseed, republish or migrate
  implicitly. Source-data deletion is never an administrative UI action.
- Existing implementation and atomic-commit authorization persists. This rewrite
  is a planning task, not approval to deploy, contact participants, alter pickle,
  or adopt unresolved persistence/security decisions.

## 2. Reuse completed code; finish its missing evidence

Do not recreate the control-plane CLI/entry point (`734c018`, `5fc06a1`), package
manager pin (`02e143b`), image-context exclusions (`851d6b5`), readiness wiring
(`789f7d1`), or unprivileged UI image/asset routing (`b5c77a7`). Keep explicit smoke
failures (`136b508`) and server-wheel CI dependencies/command checks (`ffa69f4`).

These are partial foundations. Installed-wheel server startup, actual image
serving, Compose behavior, restart preservation, browser journeys and local
security parity remain unverified. Fix failing behavior instead of rebuilding
working pieces. Persistent policy rows are not personal draft revisions; cookie
demo login is not a revocable app session; CLI CAS does not secure HTTP publication.

## 3. Dependency order

1. **Foundation:** P00 final contracts; P01 remaining startup/state preservation;
   P04.1 UI test/shell extraction; P13.1 deployment specification. These bounded
   slices can proceed independently when they do not depend on pending decisions.
2. **Secure application:** P02 sessions → P03 capabilities → P04 remaining
   lifecycle and P10.1 basic connection/asset onboarding. No live management screen
   is accepted before authoritative backend permission checks pass.
3. **First complete workflow:** P05 nested schema → P06 durable drafts → P07
   synthetic evaluation → P08 review/publication. Run browser and consumer journeys
   immediately; do not postpone integration until every screen exists.
4. **Complete product:** P09 history/audit/observations and P10 remaining management.
   Finish gateway dependencies when a vertical slice needs them.
5. **Customer readiness:** finish P11 local parity, P12 product quality, P13
   deployment, P14 recovery/upgrades and P15 capacity/operations; then P16 release.

P10.1 precedes schema onboarding and does not depend on publication. Audit write
primitives land with P03/P08 mutations; P09 adds query/UI views later. P13/P15
instrumentation and P14 recovery design start early. P12 supplies candidate
evidence, not permission to ship before P13–P16.

A stopped VM blocks container evidence, not all implementation. Continue unblocked
code/tests/operational work. Never substitute static-string assertions for runtime
tests, or SQLite checks for PostgreSQL concurrency evidence.

## 4. Atomic slice protocol

For each numbered slice below:

1. Inspect its implementation, callers, tests and ledger. Specify expected behavior
   and failure outcomes. Reuse approved decisions; request review only for an
   unresolved decision that actually needs it.
2. Add meaningful failing tests first under applicable repository test-review
   rules. Missing imports or matching source strings alone do not prove behavior.
   Never weaken security expectations merely to get green tests.
3. Implement typed backend contracts/services and UI behavior together where the
   slice requires both. Keep services narrow and transport adapters thin.
4. Run focused checks and required real integration. Check scope, secret handling,
   state preservation and failure cleanup. Skipped checks remain outstanding.
5. Commit one coherent behavior and its tests using Conventional Commits. Record
   commit, exact commands/results, environment, evidence and gaps in the ledger.
   A focused check or hook override is not a full-suite/hook pass.
6. Continue the earliest unblocked slice. A packet is verified only when every
   acceptance condition has evidence.

Split a numbered slice further when it spans independent services or mutations.
Do not combine sessions, grants, drafts and the whole UI in one commit. Do not
delegate unless authorized. A backend response must be real before its screen
can be called complete.

## 5. Remaining packets

### P00 — Final contracts and acceptance fixtures

**Remaining:** proposal exists; final typed contracts, DB constraints, decisions
and acceptance fixtures do not. This plan does not approve new schema.

1. Complete the UI/API/CLI action and method inventory. Freeze issuer/subject
   identity, capability names/scopes, readable response fields, direct-ID/list/
   count/cursor behavior and safe errors. Separate reader, metadata viewer,
   editor, publisher, connection operator, grant administrator and auditor.
2. Resolve the ten corrections in `P00_CONTRACT_REVIEW.md`: single-use browser-bound
   login transactions; expiry/rotation/revocation and group freshness; cookie/CSRF/
   origin/proxy behavior; expired logout; bounded login abuse; bootstrap/emergency
   access; and supported bearer CLI authentication.
3. Deliver additive migration designs with concrete columns/types, constraints,
   indexes, foreign keys, transaction boundaries, retention/cleanup, key references
   and existing-data transition for login transactions, sessions, grants, drafts,
   evaluations, reviews, operations and audit. Specify restore invalidation and
   interruption recovery. Group migrations by dependency rather than table count.
4. Freeze collection/item, review, publication and operation-lookup request/response
   examples, ETags/revisions, idempotency, limits and cancellation lifecycle. Define
   independently expected synthetic schema/policy/persona/consumer fixtures.

**Acceptance:** every UI action has permission, backend contract, state owner,
failure state and test case. Record actual contract review and existing authority;
unresolved new-state decisions remain open. Generate TS from implemented API models.
**Start at:** control-plane routes/services, ORM/migrations, UI API client, CLI,
P00 proposal and gateway contracts. **Commit units:** contract examples; reviewed
migration design; negative fixtures. No blanket security sign-off is implied.

### P01 — Installation and restart-safe startup

**Dependencies:** existing packaging; independent of new session design.

1. Separate initialization from routine startup. Remove unconditional fixture-table
   dropping and policy/owner/settings replacement from startup. Check authoritative
   DB/catalog state, not only a marker file. Inconsistent/partial setup must stop
   with recovery instructions; retry must preserve authored state. Reset stays explicit.
2. Build/install a clean `[server,postgres]` wheel; run installed commands and a
   real control-plane process. Verify invalid/missing config, stale schema, DB
   outage, health/readiness and graceful stop. Preserve default client-wheel
   dependency isolation. Resolve dependency/hook issues rather than permanently
   disabling verification.
3. Build both images from a clean context. Test dependency order, bounded waits,
   actual Flight RPC admission, non-root operation, CSP, deep links, asset/API 404s,
   private cache behavior and upgrade cache invalidation. Run the pinned tools/images.

**Acceptance:** install succeeds; two restart cycles preserve changed policy,
draft and source fixture; stale markers cannot produce readiness. Record runtime
images/environment. Static Compose tests alone are insufficient.
**Start at:** demo `run`, prepare/seed/provision scripts, Compose, CLI, Dockerfiles,
NGINX and package CI. **Commit units:** non-destructive setup; installed runtime
proof/fixes; actual image/ingress proof/fixes.

### P02 — Real login and revocable sessions

**Dependencies:** P00 session/migration decision; P01 for real-stack acceptance.

1. Select a maintained OIDC library using current official documentation. Implement
   code flow with PKCE, state, nonce, issuer/audience/signature validation, safe
   return URLs and atomic single-use login transactions across API replicas.
   Reader-audience credentials cannot create an administrative session.
2. Implement approved session service/repository/migration, identifier rotation,
   idle/absolute expiry and next-request revocation across processes. Retain no
   provider token unless a feature needs it; protect retained material/key versions.
   Bound cleanup and fail closed on session-store failure.
3. Implement session-bound CSRF, origin/proxy validation, safe bearer CLI behavior,
   no-store responses and truthful logout including expiry/network failure.
   Disable demo shortcuts in supported profiles. Implement narrow bootstrap and
   audited emergency access under the approved contract.

**Acceptance:** real IdP happy path and wrong issuer/audience/nonce/state, replay,
open redirect, forged proxy headers, expiry, CSRF/origin failure, restored/revoked
session replay and two-process revocation. No secrets in browser-readable storage,
app JSON, URLs, logs or artifacts. Local TLS verification stays enabled.
**Start at:** session routes/helpers, dependencies, API/CLI config, ORM/migrations,
realm/proxy fixtures. **Commit units:** code flow; session lifecycle; boundary/logout.

### P03 — Capabilities on every backend entry point

**Dependencies:** approved P00 grants; P02 for browser integration.

1. Implement grant persistence and a shared capability service. Authorize before
   protected reads. Scope lists/counts/cursors/direct IDs/history/preview/settings/
   diagnostics/exports; split safe metadata from policy-bearing DTOs.
2. Separate edit/publish/grant/connection/audit privileges; recheck mutations and
   record safe audit attribution with their transaction. Close legacy bypasses.
   Supported CLI uses the same authorized services; unavoidable DB administration
   stays an explicitly privileged operator boundary.
3. Return safe UI capability metadata. Handle live grant revocation, self-escalation,
   group freshness, reassignment, final-admin removal and emergency recovery.

**Acceptance:** full role/action/resource matrix through direct HTTP and supported
CLI, including authenticated outsiders and cross-scope attempts. No protected
detail or counts in errors. Owner/editor permission alone cannot publish.
**Start at:** `application/access.py`, route dependencies, repositories, policy/
version/catalog/asset/workspace services and CLI. **Commit units:** capability
service; scoped reads; protected mutations/CLI; negative matrix.

### P04 — Reliable UI lifecycle and behavioral tests

**Dependencies:** P04.1 starts now; P02/P03 for final integration.

1. Extract shell, session, API client, asset workspace and policy features from
   `main.tsx`. Add maintained component/browser tooling with runnable scripts/CI.
   Fix header merge order, safe typed errors and API-type generation. Implement
   navigation/deep links and accessible primitives without a framework rewrite.
2. Add abortable loads and session/asset/request epochs. Ignore obsolete responses;
   invalidate private caches and headers. Keep edits scoped to identity/resource.
   Show actual actor/workspace/environment/capabilities, not hardcoded labels.
3. Implement loading/empty/denied/expired/error/conflict states, retry, duplicate
   submission protection, unsaved navigation/tab-close guards and truthful save/
   logout status. Hide private content while preserving failed-logout retry. Newer
   edits remain dirty when an earlier save finishes. Isolate explicit prototype mode.

**Acceptance:** visible component/browser behavior under delayed requests after
logout, rapid asset switching, 401/403/409/412, empty rules, last-rule removal,
malformed masks and rejected promises. Keyboard focus and recovery work.
**Start at:** UI `src/main.tsx`, `src/api.ts`, feature modules, package scripts and
CI. **Commit units:** harness/extraction; session/request lifecycle; editor recovery.

### P05 — Authoritative nested schema and selection

**Dependencies:** P03, P10.1 onboarding, gateway W02/W03/W07 semantics.

1. Admit Iceberg schemas from metadata without reading source rows. Preserve
   field/element/key/value IDs, hierarchy, types/nullability, version/fingerprint.
   Expose a bounded typed API; flat user-entered type strings are not admission.
2. Implement keyboard-accessible expansion/search/partial selection and inherited
   grants/masks with typed segments. Explain map-key and masked-parent constraints.
   Do not split field names on dots or flatten container/null structure.
3. Detect rename, deleted/rebound IDs and new fields. Invalidate stale evidence
   and block publication pending explicit revalidation; wildcards/parent grants
   cannot silently admit added fields.

**Acceptance:** literal `a.b` versus nested `a.b`; struct/list/list-of-list/map and
null-element cases preserve actual Arrow output. Test stable-ID rename/new-ID
rebinding, map values without keys, 5,000 fields and depth 12.
**Start at:** catalog/schema/asset services and DTOs, canonical paths, Iceberg
adapter, UI tree. **Commit units:** schema API/admission; tree; drift handling.

### P06 — Revisioned drafts and complete policy editing

**Dependencies:** P03/P05 and approved P00 persistence.

1. Implement draft service/repository/migration: author/scope, base generation,
   schema fingerprint, canonical digest, revision and timestamps. Transition
   existing authoring records additively without changing policy meaning.
2. Implement create/open/update/discard with revision preconditions and atomic CAS.
   Recover uncertain saves and restart state; preserve local edits on conflict.
   Disable legacy rule-replacement routes that bypass these semantics.
3. Complete rule add/remove/order, subjects/claims, restricted DuckDB SQL, six
   masks with typed/null values, inheritance/conflicts and advanced round-trip.
   Make empty-rule deny-all explicitly reviewable and publishable.

**Acceptance:** two writers at revision 4 yield one revision 5; loser keeps edits.
Save never activates; newer local edits remain dirty. Test restart/discard,
invalid masks, unsupported advanced content preservation and last-grant removal.
**Start at:** policy models/services, repository/migrations, draft routes/editor.
**Commit units:** draft persistence; CAS/recovery; complete editor/deny-all.

### P07 — Exact synthetic evaluation and explanations

**Dependencies:** P05/P06 and canonical gateway resolver/transform.

1. Implement bounded synthetic evaluation with canonical resolver and DuckDB
   transform. Return actual synthetic schema/values, decisions and explain
   references. No source-row credential or browser-authoritative calculation.
2. Bind evidence to actor/scope, draft revision/digest, schema fingerprint,
   persona/fixture digest and evaluator version. Implement limits, timeout,
   cancellation and a durable lifecycle if asynchronous work is required.
3. Connect policy tests/effective-access inspector; distinguish deny, failed,
   cancelled, stale and completed outcomes. Clearly label synthetic coverage.

**Acceptance:** independent two-group AND, nested masks, hidden dependencies,
conflict/null fixtures pass; deliberately skipped masks/filters fail tests.
Changed inputs and failed/partial evaluations cannot qualify for publication.
**Start at:** evaluation service/repository/routes and test/inspector UI; reuse
resolver/transform. **Commit units:** evaluator; evidence binding; tests UX.

### P08 — Exact review and atomic UI/CLI publication

**Dependencies:** P03/P05/P06/P07; gateway W05 generation/CAS semantics.

1. Implement authoritative review diff/digest over exact draft/schema/evaluation,
   affected assets and expected generation. Show ticket invalidation and unknown
   impact. First publication cannot bundle unrelated unreviewed drafts.
2. Implement one publication service with scoped publisher checks, freshness,
   expected-generation CAS and idempotency. Commit activation/audit/operation
   outcome atomically; define permission-change races in the transaction contract.
   Close HTTP/CLI activation bypasses and support reviewed deny-all publication.
3. Implement review/publish/operation-status UI. Reconcile lost responses through
   operation ID/key. Same key/body returns the same outcome; changed body conflicts.
   Distinguish committed publication from observed worker activation.

**Acceptance:** PostgreSQL concurrent publishers yield one winner. Changed draft/
schema/permission/generation and bootstrap scope cannot bypass review. API/CLI
behavior matches; real reads reflect publication and old tickets are invalidated
according to the gateway contract. A client `validated=true` has no authority.
**Start at:** version/compiler services, CAS repository, operation/audit storage,
routes, CLI, Changes/review UI. **Commit units:** review; atomic publish; reconciliation.

### P09 — History, restore, audit and runtime observations

**Dependencies:** P08; audit writes already land with P03/P08 mutations.

1. Expose scoped paginated history/audit and exact comparisons with safe actor/
   action/resource/outcome detail; sensitive detail requires separate permission.
2. Implement restore-as-new-draft via P06; require fresh validation/review before
   publication, never direct reactivation of historical content.
3. Implement runtime observations with generation/source/timestamp/freshness.
   Show partial rollout, pending restart, stale/unreachable state truthfully;
   support bounded redacted diagnostics/export where authorized.

**Acceptance:** no history/count leakage; restore cannot bypass validation;
activation/audit cannot diverge. DB readiness/Flight liveness alone cannot report
healthy end-to-end delivery. History/activity views use real services.
**Start at:** history/audit/observation services, routes and UI features.
**Commit units:** history; restore; audit views; runtime status.

### P10 — Management and consumer handoff

**Dependencies:** P10.1 after P03, before P05; remaining slices after P05/P08/P09.

1. Implement basic connection registration/diagnostics and asset inventory/binding
   onboarding with scoped permissions. Validate modules/options, secret references,
   endpoint/egress policy, bounded discovery and integrity. This enables a real
   user-created asset and must not depend on publishing its first policy.
2. Finish search/pagination, edits, dependency-aware disable/removal, schema-drift
   repair, owners/grants, settings and diagnostics. Explain draft-versus-active
   settings and actual activation/ticket effects of disabling connections/assets.
3. Provide executable Python/DuckDB, Spark/JVM and Arrow setup guidance with real
   contracts, verified TLS and separate reader credentials. Consume every ticket/
   partition. Never embed tokens or imply administrative roles permit data reads.

**Acceptance:** every required destination performs real operations. Test SSRF,
redirect/DNS rebinding/cloud metadata, traversal, module injection, secret outage,
in-use removal, cross-scope writes and absence of source-data deletion. Authorized
private Iceberg endpoints use explicit egress policy, not an indiscriminate ban.
Run examples against the supported clients.
**Start at:** catalog/asset/workspace/grant services and UI management/connectors.
**Commit units:** connection; asset; grants; settings; consumer guidance.

### P11 — Full local feature/security parity

**Dependencies:** P01–P10 and P13 security configuration; iterate after P08.

1. Provide documented prepare/start/verify commands for PostgreSQL, real IdP,
   verified local TLS, synthetic Iceberg, UI/admin, Flight and required consumers.
   Bind host ports to loopback; ordinary startup prints no credentials.
2. Run real API/browser journeys with the same sessions, capabilities, CSRF,
   validation and publication as deployment. Exclude impersonation shortcuts from
   supported startup and acceptance tests.
3. Exercise restart, expiry/revocation, concurrency, outage, uncertain operations,
   preservation and recovery. Keep HTTP smoke, browser and consumer evidence
   separate. Verification failures exit nonzero even under Python optimization.

**Acceptance:** clean checkout completes section 7 journeys. All clients, including
JVM executors, trust the configured CA without bypass flags.
**Start at:** Compose/realm/certificates/secrets, CLI/setup/smoke, browser fixtures.
**Commit units:** parity configuration; full journey; recovery/failure scenarios.

### P12 — Product quality, accessibility and usability

**Dependencies:** tooling begins P04; final acceptance needs P04–P11.

1. Add frontend/API/contract/browser/PostgreSQL-race/consumer CI as features land.
   Generate/check API types, lock supported versions and retain failure artifacts.
   Required integration runs in clean environments.
2. Verify keyboard/screen-reader workflows, focus, labels/errors, tree, 200% zoom,
   narrow layouts, contrast and reduced motion. Combine automated accessibility
   with manual checks; fix failures and retest.
3. Obtain owner visual review and five representative task sessions when authorized.
   Target four of five completing each core task unaided, no critical access/
   publication misunderstanding and no accidental activation. Unavailable
   participants remain an explicit acceptance gap, not a reason to stop coding.

**Acceptance:** real J1–J8 on supported browsers; no placeholders. Proposed warm
targets on documented 4-vCPU/8-GiB hardware: initial compressed JS <=250 KiB,
field interaction p95 <=100 ms, inventory p95 <=1 s, 100-row evaluation p95 <=2 s.
Use at least 30 samples; report cold results and memory separately. Performance
corpus: 10,000 assets, 5,000-field/depth-12 schema, 100 rules and adverse states.
**Commit units:** CI lanes; accessibility fixes; measured performance fixes.

### P13 — Supported production topology and startup validation

**Dependencies:** design now; runtime integration needs P01/P02/P03/P11.

1. Deliver one concrete reference under proposed `deployment/production/`: customer
   isolation, immutable UI/backend digests, PostgreSQL, separate IdP audiences,
   TLS HTTP/HTTP2 Flight ingress, secrets, capacity and operators. Specify IdP MFA
   and audited emergency access for privileged users.
2. Implement strict configuration validation and least-privilege migration/admin/
   Flight DB roles. Flight needs narrow ticket-store writes, not blanket access
   or an inaccurate read-only promise. Restrict source credentials, networks,
   health ports, proxy trust, users/filesystems and resource limits. Reject
   demo/insecure profiles and stale schema at production startup.
3. Test install/migrate/bootstrap/start/drain/stop with two admin and two Flight
   processes, rolling restart, bounded DB pools/timeouts, IdP/DB failure and actual
   HTTP2 Flight through ingress. Separate liveness/readiness from a governed read.

**Acceptance:** an operator installs exact artifacts with the supplied docs,
completes real journeys and restarts safely. Private services stay private and
invalid configuration fails closed. Do not claim arbitrary cloud support.
**Commit units:** deployment specification; configuration/privilege enforcement;
reference deployment; multi-process and upgrade smoke.

### P14 — Recovery, upgrades and credential rotation

**Dependencies:** design with P00; final tests need P06/P08/P09/P13.

1. Implement encrypted PostgreSQL backup/PITR with IdP/config/key-version/secret
   dependencies. Document customer Iceberg data and snapshot/metadata retention
   responsibility separately from control-plane recovery.
2. Restore in isolation; reconcile grants/generations/operations before ingress.
   Invalidate restored sessions/tickets so backups cannot resurrect revoked access.
   Use reviewed key/epoch mechanisms without modifying pickle. Verify recovered
   drafts and actual allowed/denied consumer values, not row counts alone.
3. Test previous-release upgrade with authored state, interrupted migrations,
   forward recovery, N/N-1 compatibility or explicit maintenance window, and safe
   rollback. Exercise TLS/IdP/backend/ticket/session/encryption-key rotation,
   compromise and unavailable-key failures.

**Acceptance:** timed restore meets agreed RPO/RTO; failure leaves ingress closed;
restored credentials cannot replay. Proposed RPO <=15 minutes and RTO <=60 minutes
are unmeasured engineering targets, not contractual promises.
**Commit units:** backup tooling; restore/invalidation; upgrade; rotation drills.

### P15 — Capacity, observability and customer operations

**Dependencies:** instrumentation early; acceptance needs P07/P09/P13 and gateway.

1. Bound bodies/schema/SQL/lists/login/discovery/evaluation/publication/reads by
   size, CPU/time, memory and concurrency. Control aggregate customer load across
   processes. Add retention cleanup with referential integrity and bounded metrics
   labels, preserving audit policy and active references.
2. Add redacted correlation/logging and metrics/alerts for errors/latency/auth,
   generations, capacity, DB pools, backup age and certificate expiry. Exercise
   alert delivery to the named operator without unapproved external messaging.
   Write incident/export/onboarding/offboarding/support runbooks.
3. Benchmark the UI corpus plus Iceberg bytes/files/deletes/evolution, fan-out,
   streams and Spark partitions. Run saturation/recovery, slow-consumer, cancellation,
   retry, OOM and 24-hour soak tests. Record hardware/versions/concurrency,
   p50/p95/memory/throughput and operating cost envelope.

**Acceptance:** bounded capacity with no unbounded growth or weakened policy;
failures are detected and recoverable. Separate admin/read availability SLIs;
report expected denials separately. Proposed 99.9% monthly objective needs
measured topology/dependency budgets before any contractual SLA.
**Commit units:** limits/cleanup; telemetry; load fixes; operational drills/docs.

### P16 — Candidate-specific release qualification

**Dependencies:** all preceding gates and applicable gateway obligations below.

1. Make required lanes promotion dependencies for both images: client/server
   wheels and installed process, frontend/types, real IdP/browser, PG races,
   consumers, containers, recovery/upgrades and capacity. Scan, generate SBOM/
   provenance and verify digests for the architectures/artifacts actually promoted.
   Development image publication is not supported-customer release approval.
2. Review dependencies/action/image pins, licenses and patch procedure. Map
   applicable ASVS controls and obtain independent security evaluation of browser,
   API/CLI, DB/task trust, nested enforcement, connectors and deployment. Resolve
   critical/high findings; do not silently waive pickle-boundary findings.
3. Produce proposed `evaluation/release/<candidate>/`: commit/digests, schema/config
   versions excluding secrets, tests/skips, browser/a11y/usability, capacity,
   restore/upgrade/rotation drills, review findings, limitations and operator/owner
   acceptance. Promote only the same immutable tested artifacts afterward.

**Acceptance:** no required workflow, backend enforcement, operational recovery or
independent security gate is missing. Record owner release acceptance; production
deployment still needs explicit authority. A plan or green unit suite is not proof.
**Commit units:** promotion gates; release artifact tooling; review remediation.

## 6. Gateway work remains a release dependency

Track evidence in `docs/gateway-v1/STATUS.md`; UI success does not close gateway
work. Implement required gaps alongside their dependent packet.

- W02/W03: stable typed IDs, nested pruning/inheritance, six masks and row filters
  preserve Arrow schemas/null/empty/map-key semantics. Independently expected
  cross-language fixtures assert allowed values and forbidden fields.
- W04/W05: issuer/subject/expiry binding, fetch reauthorization, ticket reservation,
  consistent publication generation and in-flight revocation work across races,
  processes and batch boundaries.
- W07: splittable native Iceberg plans, pinned snapshots, position/equality deletes,
  schema evolution and REST catalog have conformance evidence within the supported
  envelope. Unproven formats/features, including v3, fail closed.
- W08/W09: every ticket/partition is consumed; retry/speculation/cancellation,
  credential refresh, deadlines, slow consumers, memory/admission and cleanup
  work through actual Flight in Python/DuckDB, Spark and Arrow.
- W11/W12/W13: least privilege, packaging/PG/consumer/capacity evidence and
  independent evaluation cover shipped artifacts. Preserve W06 pickle restrictions;
  escalate unresolved trust-boundary risk rather than declaring completion.

## 7. Required release journeys

Use independent synthetic expectations shared by HTTP/browser/consumer lanes.
Fixtures are test data, not an alternative production implementation.

- J1: real login → register connection/asset → explicitly assign editor/publisher.
  Reader/outsider cannot retrieve administrative metadata or policy detail.
- J2: nested schema → parent/child/list/map grants and all masks. Parent reads
  return only permitted descendants; literal dotted names and null shapes survive.
- J3: save draft → test two-group AND and denied persona. Any input change makes
  evidence stale; failed or partial evaluation cannot publish.
- J4: exact review → concurrent edit/generation conflict → recover → publish once.
  Lost response reconciles; actual reads show committed policy.
- J5: compare → restore-as-draft → validate/review/publish with audit attribution.
  Empty-rule deny-all can revoke the last grant.
- J6: repair catalog/secret outage → dependency-aware disable/remove. No leaked
  credentials/source rows and no source-data deletion.
- J7: logout during delayed requests → revoked replay fails → another user sees
  only their authorized state. Failed logout is reported truthfully.
- J8: restart → recover authored state → upgrade → restore backup → rotate keys →
  repeat nested allowed/denied DuckDB/Spark/Arrow reads with verified TLS.

## 8. Evidence and stopping rules

Ledger states: `not-started`, `implementing`, `implemented-unverified`, `verified`,
`blocked`. Historical labels require interpretation, never automatic promotion.
Each slice records behavior/contract, authority, red failure, exact green command/
exit code, environment, commit, artifact paths, manual/independent evidence,
remaining risk/blocker and next action. Failed, skipped, interrupted or output-less
commands are not success. A blocked runtime check remains required.

Existing commands: `uv run pytest <focused paths>`, `uv run ruff check`,
`uv run ruff format --check`, `uv run ty check`, and
`mvn -f connectors/jvm/pom.xml verify`. Frontend currently has build/check scripts;
P04 adds actual behavioral/browser scripts before anyone cites their results.
Install appropriate server extras in CI. Package proof uses a fresh environment,
not an editable checkout with preinstalled dependencies.

Pause only dependent work for unresolved decisions requiring review, unavailable
infrastructure or required external evaluation. Continue independent authorized
work; do not repeatedly ask for permissions already granted. Do not contact users/
reviewers or deploy unasked. Production remains on hold until P16 evidence exists.

## 9. Implementer prompt

> Implement the remaining work in `docs/ui-v2/EXECUTION_HANDOFF.md`. Read the ledger
> and production review, then inspect real code. Preserve completed slices, data,
> Iceberg/nested/consumer scope and pickle logic. Start P00's concrete unresolved
> contracts and P01's restart preservation; advance P04 test tooling and P13 design
> where independent. Follow section 3 dependencies, delivering real backend services
> and UI behavior in atomic tested commits. Keep authorization/evaluation/publication
> on the backend. Execute actual API/browser/PG/consumer acceptance, not mocked
> success or static assertions. Maintain exact evidence; continue unblocked work
> until all authorized items are complete. Release stays on hold until P16 and
> applicable gateway gates have actual candidate-specific proof.
