# UI, backend, and paid-production readiness

Reviewed 2026-09-12 against `0e5a3a3`. **Release decision: HOLD.**
The product specification is broad, but the application is not feature complete
or ready to serve paying customers. This document adds required backend and
operational acceptance to P00–P12; it does not certify the implementation.

## Scope and release model

Keep the complete policy-authoring and management UI, nested Iceberg schemas,
all six masks, restricted DuckDB row expressions, and real Python/DuckDB,
Spark/JVM, and Arrow consumer support. Other Arrow frameworks use the supported
Arrow interface; advertise only individually tested integrations. No new backend.
Preserve the owner's existing pickle implementation.

Planning assumption, pending customer-hosting preference: one workspace and one
isolated deployment/database/key set per customer. Shared multi-customer SaaS
would require an additional tenant-isolation design and verification before
launch. Cell/tenant IDs in existing tables do not demonstrate that isolation.
Customer-owned deployments and a vendor-operated deployment are both possible,
but the operator responsible for certificates, backups, incidents, and upgrades
must be named. Manual invoicing can support first customers; billing automation
and a self-service tenancy platform are not implicit requirements.

## Findings verified against code

Abbreviated `interfaces/`, `application/`, and `infrastructure/` paths in this
section are under `src/dal_obscura/control_plane/`; `common/` is under
`src/dal_obscura/`.

1. **Critical release blocker: administrative data disclosure.**
   `interfaces/routes/deps.py` authenticates an actor; asset, policy, history,
   catalog, and settings read routes generally do not check scoped capabilities.
   A synthetic `TestClient` probe with `outsider-token` returned 200 for
   `/v1/assets`, an existing asset's `/policy-rules`,
   `/v1/settings/auth-providers`, and `/v1/policy-versions`.
   The resolver was a test fixture; this demonstrates route authorization after
   authentication, not verification of a real IdP. Fix P03, including disclosure
   through asset-detail responses that contain policy bodies.
2. **Critical release blocker: supported browser authentication is missing.**
   `interfaces/routes/session.py` uses demo password exchange and stores the
   provider token in a cookie. Logout deletes cookies without revoking that token.
   `common/config_store/orm.py` has no administrative session or scoped-grant
   records. P02/P03 need actual server implementation and two-process tests.
3. **High: UI and CLI publication are inconsistent.**
   `application/policy_version_service.py:create_asset_policy_version` permits an
   editor to publish, initially compiles the whole workspace, and calls ordinary
   activation. `infrastructure/repositories.py` also provides CAS activation,
   used by `interfaces/admin_cli.py`, but the HTTP path does not use it. This
   creates scope and concurrent-update risks. Shared P08 services must replace
   both supported publication paths; database-write CLI access remains a separate
   privileged administrative boundary until that replacement exists.
4. **High: durable workflow semantics are absent.**
   Policy rules persist, but personal draft revisions, evaluation bindings,
   reviewed digests, operation idempotency, and atomic audit records are absent.
   Persistent rows alone do not satisfy P06–P09. Empty-rule publication is rejected
   today; P06/P08 must explicitly support reviewed deny-all instead of making a
   last-grant removal impossible.
5. **High: UI can report the wrong state.**
   `apps/governance-ui/src/main.tsx` clears local state in logout's `finally`,
   does not fence responses by session/asset/revision, and describes saved-server
   preview as current for an edited draft. `src/api.ts` can overwrite its merged
   CSRF headers through the later `...init`. Four management destinations and
   history are placeholders. There is no frontend behavioral test script.
6. **High: startup can overwrite authoring state and source fixtures.**
   `examples/demo/keycloak/scripts/seed_table.py` drops an existing fixture table;
   `provision_demo.py` replaces rules/owners/settings and publishes on each setup
   invocation. `prepare_demo.py` removes the completion marker. P01 orchestration
   work is therefore not complete. Restart must preserve both authored policy
   and data; initialization/reset must be explicit and distinct.
7. **High: UI success does not establish gateway correctness.**
   `docs/gateway-v1/STATUS.md` still records open Iceberg deletion/evolution,
   nested-field, ticket/retry, streaming, and consumer gates. Unsupported Iceberg
   features must fail closed. Define and exercise the supported compatibility
   envelope before accepting real customer datasets.
8. **High: release pipeline does not validate the whole deliverable.**
   `.github/workflows/ci.yml` has Python/JVM lanes and scans/publishes the Python
   service image, but no frontend behavior, real browser, UI image publication,
   PostgreSQL race service, or recovery lane. Its package smoke installs only
   `[postgres]` before running migrations although Alembic/SQLAlchemy are in
   `[server]`. A UI/static-string test cannot stand in for these executions.

The package-extra defect was corrected during this review in `ffa69f4`; CI now
installs `[server,postgres]` and invokes `dal-obscura-control-plane --help` before
migration checks. Seven focused CLI/workflow tests, lint, format, and type checks
passed locally. The CI job itself has not run here; installed-wheel execution
and the remaining release lanes are still required.

Existing focused checks passed in this review: control-plane publication-flow,
actor-auth, and workspace tests. They do not assert the required complete
negative capability matrix and therefore do not invalidate the findings above.
No live customer environment, production credentials, or external system was
changed. Container, installed-wheel-server, and real-browser evidence remains
unverified. No independent security review is claimed.

## Every required UI workflow must have a backend implementation

Each item below is a vertical slice: route, typed response/error, application
service, persistence where required, authoritative permission check, UI behavior,
and executable acceptance. New paths remain governed by P00's final API contract;
do not implement parallel incompatible APIs just to make a screen work.

- **Login, session, expiry, logout (P02/P04).** Replace demo routes using a
  maintained OIDC library, shared revocable sessions, and safe session metadata.
  Test real IdP login plus expiry, replay, revocation, CSRF/origin, proxy headers,
  identity switch, and failed logout through HTTP and a browser.
- **Workspace identity and navigation (P03/P04).** Extend workspace/session
  services with authorized name, environment, capabilities, and scoped summary
  counts. Remove hardcoded `analytics` and inferred role labels. Test denied
  navigation, direct links, refresh, and count/cursor isolation.
- **Asset inventory and registration (P03/P05/P10).** Extend asset routes and
  `asset_service.py` with bounded search/pagination, binding validation, scoped
  metadata DTOs, and dependency-aware disable/remove. UI removal must never drop
  source data. Test direct IDs, name encoding, incompatible bindings, and races.
- **Connection management (P03/P10).** Extend catalog routes/service/discovery
  with allowed connection types, secret references, bounded diagnostics, explicit
  draft-vs-active configuration, and dependency checks. Test URL redirects, DNS
  rebinding, cloud metadata access, traversal, arbitrary module/config injection,
  timeout, credential redaction, and secret-provider least privilege. Do not
  blanket-block legitimate private Iceberg endpoints; require operator egress
  policy and explicitly authorized endpoints.
- **Canonical nested schema (P05).** Replace flat `schema_fields` strings with a
  typed schema API backed by authoritative Iceberg schema and shared path models.
  Preserve stable IDs, container/nullability, literal dots, fingerprint/version,
  and map-key dependencies. Test drift/rebinding and 5,000 fields at depth 12.
- **Rule and mask authoring (P05/P06).** Use shared policy/mask/compiler models,
  never frontend approximations. Support rule ordering, subjects/claims, AND row
  restrictions, six masks with typed/null values, parent/child inheritance, and
  preservation of advanced content. Every admitted policy must round-trip without
  losing unknown-but-supported fields or widening a grant.
- **Save, recovery, discard, conflicts (P06).** Implement a draft service and
  reviewed additive repository/migration with personal scope, immutable revision,
  base generation, and CAS updates. Test two editors, uncertain save, newer local
  edits during save, restart recovery, empty rules, and explicit discard.
- **Synthetic tests and explanations (P07).** Implement an evaluation service
  invoking the canonical resolver and DuckDB transform with bounded synthetic
  data. Bind result to draft, schema, input, and evaluator version. Return actual
  transformed schema/values, explanation and outcome; failure is never an allow
  result. Test resource exhaustion, stale evidence, cancellation, masks/filters,
  and source-credential isolation. Define bounded synchronous execution or a
  durable operation lifecycle; never fire-and-forget work in an API process.
- **Review and publication (P08).** Implement server-generated review and shared
  publication services with exact content/impact digest, expected generation,
  publisher permission, schema/evaluation freshness, and idempotency. Commit
  activation, operation outcome, and audit together. Test PostgreSQL races,
  initial publication scope, deny-all, API/CLI parity, and response loss after
  commit. The UI must reconcile via operation lookup.
- **History, compare, restore (P09).** Expose scoped immutable revisions and
  comparison detail through history services. Restore creates a new draft, then
  requires fresh validation and review. Test old-version compatibility, forbidden
  history/export, pagination and audit correlation.
- **Grants, ownership, operator access (P03/P10).** Implement a shared capability
  service and approved grant migration. Match identity by issuer and subject, not
  display name alone. Separate grant, edit, publish, connection, sensitive-audit,
  and Flight-read privileges. Test self-escalation, owner reassignment, final
  administrator removal, group changes, and grant revocation in an open session.
- **Activity, status, diagnostics (P09/P10).** Implement scoped audit queries and
  runtime observations carrying source, generation, timestamp, and freshness.
  HTTP database readiness and Flight liveness alone cannot prove a healthy read
  path. Test unreachable worker, old generation, partial rollout and bounded
  redacted support exports; do not display unknown as healthy.
- **Consumer setup (P10 plus gateway W02–W09).** Generate safe examples from real
  connector contracts. Read every returned ticket/partition, preserving nested
  values and masks. Verify Python/DuckDB, Spark executors, and Arrow independently,
  including authentication refresh, denied reads, retries, cancellation and worker
  restart. No raw credentials in snippets or Spark task serialization.

Contract rules across all slices: explicit request/response models; generated TS
types with CI drift checks; bounded body/list/depth/SQL sizes; scoped stable
cursors; safe structured errors and correlation IDs; no-store private responses;
authorization before sensitive reads; revision/generation preconditions on
mutations; audit coverage. A screen is complete only after its browser test runs
against those real backend services, not a mock route or fixture-only demo.

## Additional production packets

P00–P12 remain required. Add P13–P16 below; start independent operational work
early, but release waits for all dependencies. Each numbered slice is an atomic
behavior-and-tests commit. Expected output paths below are new deliverables, not
claims that these artifacts exist today.

### P13 — Supported deployment and startup contract

Depends on P01/P02/P03/P11 for acceptance.

1. Add a validated production profile and deployment reference under
   `deployment/production/`. Inputs: hostname, exact image digests, PostgreSQL,
   IdP/admin and reader audiences, TLS trust, secret-provider references, supported
   Iceberg catalog, capacity, and bootstrap operator. Fail startup for demo login,
   missing audience/key/TLS configuration, unsafe proxy trust, or stale DB schema.
   Require IdP MFA for privileged operators and document emergency access.
2. Package/version UI and backend together with API/schema compatibility and
   client versions. Provide TLS HTTP and HTTP/2 Flight ingress, internal-only DB
   and health ports, non-root/read-only containers, resource limits, restricted
   egress, and deployment-specific secret mounting. Separate migration, control-
   plane, and Flight database privileges. Do not claim Flight has read-only DB
   access: its current ticket store writes/reserves records and needs explicit
   narrow grants. No customer may write trusted ticket payloads directly.
3. Provide explicit install/migrate/bootstrap/start/verify commands. Routine
   start must not seed, reset, migrate implicitly, republish, or print credentials.
   Add readiness with timeout plus real authenticated synthetic Flight read checks,
   graceful drain/SIGTERM, connection pool sizing, and DB/IdP outage behavior.

Acceptance: operator with no repository knowledge installs the exact artifacts
in a clean environment, completes J1–J8 over valid TLS, restarts without data loss,
and cannot reach private services externally. Exercise at least two admin and
two Flight processes, including rolling restart and cross-process revocation.
Provide one concrete supported topology; do not claim every cloud/orchestrator.

### P14 — Recovery, upgrades, and credential lifecycle

Depends on approved P00 migrations and P06/P08/P09/P13.

1. Implement verified encrypted backups and point-in-time recovery for the
   control-plane PostgreSQL database. Cover separate restoration of IdP settings,
   encryption-key versions, deployment settings, and secret references; a DB
   backup alone is insufficient. Customer Iceberg data remains the customer's
   storage responsibility; document metadata/snapshot retention dependencies.
2. Write and execute a restore runbook into an isolated environment. Reconcile
   sessions, grants, policy generations, and outstanding operations before opening
   ingress. Revoke restored sessions and tickets so backups cannot resurrect access
   that was revoked after the recovery point. Design this around key/epoch rotation
   without altering pickle serialization. Verify restored drafts and allowed/denied
   consumer results, not merely database row counts.
3. Test additive upgrade from the previous supported release with authored drafts
   and publications, migration interruption, forward recovery, N/N-1 compatibility
   or an explicit maintenance window, and rollback without reopening an insecure
   authentication path. Expose schema/API/evaluator compatibility clearly.
4. Exercise IdP signing-key, TLS certificate, ticket-signing, session-hash/token
   encryption, and backend credential rotation. Test overlap, old-key retirement,
   compromised-key emergency revocation, and unavailable secrets without fallback.

Acceptance: measured recovery meets agreed RPO/RTO; failed restore/upgrade leaves
traffic closed, and no restored identity or ticket bypasses a revocation.
Initial planning targets: RPO <=15 minutes, RTO <=60 minutes. These are proposed
engineering targets, not contractual promises or achieved measurements.

### P15 — Capacity, observability, and customer operations

Depends on P07/P09/P13 and gateway W07–W09.

1. Bound login, metadata discovery, draft/evaluation/publication operations and
   Flight work by request size, concurrency, duration and memory. Bound aggregate
   per-customer work across processes, not just one process semaphore. Index and
   paginate inventory/history; configure DB statement/pool timeouts. Clean up
   expired sessions, tickets, evaluations, and operation data with documented
   retention; preserve audit requirements and active-reference integrity.
2. Emit redacted structured logs, request/operation correlation, metrics for
   latency/errors/capacity, DB pools, auth failures, active generation, memory,
   cancellation, and backup age. Bound metric label cardinality. Alert on customer
   failures, stale backups, certificate expiry, and exhaustion. Exercise the alert
   path and named responder; never send telemetry or support bundles externally
   without authorization.
3. Run UI corpus performance from P12 plus representative Iceberg bytes/files,
   rows/batch, fan-out, active streams, Spark partitions, and concurrency. Measure
   saturation/recovery, cancellation, slow consumers, OOM containment, retries,
   delete-file semantics, schema evolution, and 24-hour soak on documented hardware.
   Readiness must recover after overload without a restart or policy weakening.
4. Document first-customer onboarding/offboarding, configuration export, supported
   versions, compatibility limits, patch/upgrade procedure, incident triage,
   security contact, and support responsibility. Configuration removal never
   deletes customer source tables. Define support/SLA commercially after capacity
   and incident drills; do not invent compliance certification or uptime guarantees.

Acceptance: measured capacity envelope and operating cost are recorded for the
candidate; failures alert an operator with an executable recovery path. Define
separate availability SLIs for admin writes and authorized Flight reads. Proposed
initial monthly availability objective: 99.9%, subject to tested topology and
dependency budget. Expected authorization denials are reported separately.

### P16 — Whole-product release evidence and promotion

Depends on P00–P15 and applicable gateway W01–W13 security/correctness gates.

1. CI must build/install the default client and `[server,postgres]` wheel, start
   the installed executable, build/test both images, and run frontend behavior,
   generated-contract checks, real-browser J1–J8, PostgreSQL races, real IdP,
   supported consumers, upgrade/restore, accessibility, and capacity lanes.
   Separate fast tests from heavy lanes, retain exact failing commands/artifacts,
   and make every required lane a promotion dependency. Skips are not evidence.
2. Pin reviewed action/image/dependency versions; scan Python/JS/JVM dependencies
   and both images, generate SBOM/provenance, verify signatures/digests, document
   license inventory, and rehearse a security update. Scan the same architectures
   and immutable image digests that are promoted. No unreviewed critical/high
   risk, including findings hidden by scanner ignore settings, may pass silently.
3. Commission an independent security review of browser, admin/CLI, persistence,
   trusted serialized-task boundary, nested Iceberg enforcement, connectors and
   deployment. The owner preserves pickle; this is a fixed implementation
   constraint, not a waiver of its trust-boundary assessment. Findings requiring
   changes there must be escalated rather than edited or marked accepted silently.
4. Produce `evaluation/release/<candidate>/` with commit, image digests, migrations,
   config fingerprint excluding secrets, test versions/results, browser evidence,
   failed/skipped gates, capacity, restore/upgrade drills, review findings, and
   operator/owner sign-off. Promote the same tested artifacts only after acceptance.
   Publishing a development image is distinct from supporting it for customers.

Acceptance: every release gate has dated evidence for the same candidate. No
placeholder destination, mock-backed required workflow, unscoped backend route,
unresolved high-risk finding, or missing customer recovery path remains. Owner
acceptance and deployment authorization remain separate actions.

## Execution priority and evidence corrections

First finish P00 contracts and permissions, with the corrections below. Then
implement P02/P03 and the P04–P08 vertical journey. P09/P10 complete management;
P11/P12 verify product/local behavior. P13–P16 complete production operation and
release. Fix restart data loss, package CI, and missing backend contracts as
independent work; a stopped local VM does not block all implementation.

The existing ledger overstates some evidence: no successful isolated-wheel
command output/exit status was captured; a subsequent isolated-wheel server never
bound its port. Compose validation was also blocked by Podman. Treat both as
unverified. The route inventory test checks path presence and two method sets;
it does not mechanically synchronize the full Markdown authorization table.
P01.4 does not test the generated health environment values, and its HTTP readiness
check does not itself test Flight RPC admission. Correct these claims and replace
static checks with real execution in the respective packets.

## Primary reference baselines

- Map applicable controls to [OWASP ASVS 5.0.0](https://github.com/OWASP/ASVS/tree/v5.0.0),
  targeting Level 2 and explicitly documenting exclusions. This is a verification
  baseline, not a claim of certification or complete security.
- Use [OAuth security BCP, RFC 9700](https://www.rfc-editor.org/rfc/rfc9700.html)
  when selecting and testing P02's maintained library and code flow.
- Base PostgreSQL recovery on [PostgreSQL 17 PITR documentation](https://www.postgresql.org/docs/17/continuous-archiving.html)
  and validate the actual chosen deployment's backup/restore commands.
