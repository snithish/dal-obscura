# Governance application: implementation handoff

Baseline inspected: `612bb1c`. This document is a plan, not implementation evidence.
Audience: a less capable coding model executing small, explicit assignments.

## 1. Objective and precedence

Deliver a working governance application that can run locally with the same
application features, authentication flow, authorization checks, and publication
safety as the supported deployment. Complete this journey using real services:

Start → sign in → register an Iceberg asset → inspect nested schema → edit a
policy draft → evaluate synthetic personas → review → publish → verify governed
DuckDB, Spark, and Arrow reads → restore an earlier policy as a new draft → sign out.

Then complete management, diagnostics, activity, accessibility, and release gates.
A working first journey is a milestone, not feature completion.

Read these existing specifications before starting:

1. `docs/ui-v2/README.md`, `EXPERIENCE.md`, and `IMPLEMENTATION.md`.
2. `docs/gateway-v1/NESTED_AND_CONSUMER_CONTRACT.md`.
3. `docs/gateway-v1/STATUS.md`, `WORK_PACKAGES.md`, and `AGENT_HANDOFF.md`.
4. Applicable `AGENTS.md` files and installed skills.

This handoff orders concrete execution of U00–U10. It corrects earlier claims
about implementation readiness; it does not reduce their scope. Owner instructions
win: UI management is required; Iceberg is the backend; nested schemas, all six
masks, DuckDB/Spark/Arrow consumers remain required; do not touch pickle logic.
Do not declare W06 or the full gateway complete while its separate conflict remains.

This task authorizes planning only. A subsequent implementation instruction starts
execution. Prior atomic-commit authorization persists. Do not deploy, contact
participants, destroy durable records, or change serialization under this plan.

## 2. Verified starting problems

Recheck each against the current checkout before changing it. Do not blindly apply
patches to stale line numbers. Paths below are repository-relative.

- `scripts/docker-entrypoint.sh` invokes `dal-obscura-control-plane`, but
  `pyproject.toml` does not register it and the corresponding CLI module is absent.
  The documented Compose launch cannot be claimed working.
- `ui/Dockerfile` and `ui/nginx.conf` now build/serve the SPA and proxy `/v1`.
  Their container execution has not been verified. Exclude host `node_modules`,
  `dist`, and secrets from build contexts; pin the package manager as well as the
  lockfile. Inspect helper build contexts against `.dockerignore` too.
- `routes/session.py` implements password-based demo impersonation shortcuts,
  raw OIDC access tokens in HttpOnly cookies, and cookie deletion on logout.
  This is not the required authorization-code login or a revocable application
  session. A local URL and an HttpOnly flag do not establish production security.
- `routes/deps.py` validates actors and uses a basic double-submit CSRF check.
  It does not implement the complete scoped capability model. Its static admin
  token is a privileged bootstrap path that must not become browser login.
- Several metadata, policy, preview, history, and settings routes require only
  an authenticated actor. `policy_version_service.py` reuses editor authorization
  for publication. Reading and publishing require their own explicit checks.
- `apps/governance-ui/src/main.tsx` mixes shell, data fetching, edits, sessions,
  preview, and placeholder pages. It has no frontend behavioral test suite.
- Preview evaluates saved server rules but the UI can call it while local edits
  are unsaved and label its result current. Demo preview copies a selected rule
  rather than invoking the canonical evaluator. Neither proves draft correctness.
- Asset switches can discard unsaved edits. Concurrent loads/saves/previews can
  update newer state. A failed load may leave old data and claim a ready workspace.
- Logout uses `finally` to claim success even when the server failed. In-flight
  requests can repopulate cleared state. An expired token can prevent cleanup.
- API fetch options spread `init` after constructed headers, allowing caller
  headers to replace the CSRF headers. Error handling loses useful safe detail.
- Asset schema authoring exposes flat `name/type/nullable` strings. It does not
  supply the required typed nested field tree with stable Iceberg field IDs.
- Draft updates replace mutable rules without revision preconditions. Durable
  draft recovery, exact evaluation binding, review, idempotent publication,
  changes/history, management, and complete status screens remain unfinished.
- `ui_smoke.py` is an HTTP smoke, not a browser test. An owner reading inventory
  does not prove authorization isolation. Existing text-presence packaging tests
  cannot prove startup, browser behavior, or security.
- Existing README claims are stale, including automatic demo fallback and PKCE.
  Fix documentation against measured behavior, never against desired behavior.

## 3. Rules for the implementing model

Execute one numbered packet at a time. Split each packet into its listed atomic
slices. Commit a coherent behavior and its tests together after the red/green
cycle; do not accumulate a project-wide rewrite or leave unexplained failing tests.
Use Conventional Commits. Preserve user changes. Do not delegate unless authorized.

For each slice:

1. Read the named source and its callers/tests. Record current behavior.
2. Write independent expected outcomes and executable failing tests. The failure
   must be the missing behavior, not import/setup failure. Respect applicable
   review requirements; record any existing authorization that satisfies them.
3. Implement the smallest complete change. Backend permission checks remain
   authoritative even if the UI disables an action.
4. Run focused checks, then relevant integration checks. Record exact commands,
   exit codes, environment, commit, and unresolved failures in `EXECUTION_STATUS.md`.
5. Review the diff for accidental bypasses, dropped fields, secrets, and scope
   changes. Commit only selected files. Update the packet evidence entry.
6. Continue to the next unblocked slice. If runtime infrastructure is unavailable,
   work on independent slices and leave the runtime gate pending.

Never weaken an assertion to match actual output without explaining why the
original expectation was wrong and obtaining the required semantic review.
Never replace auth with a stub outside tests, suppress failing authorization,
return fake healthy status, silently omit unsupported nested fields, skip a
required consumer, or use local policy evaluation as authoritative evidence.
Never call a package complete because TypeScript, a build, or pre-commit passes.

A less capable model may implement mechanical work after contracts are fixed.
It must escalate unresolved security semantics to the owner/capable reviewer,
with a concrete proposed contract and failing examples. It must not choose a new
security model by trial and error. Independent security evaluation is a release
gate distinct from the implementer's own review.

## 4. Fixed architecture direction and review gates

Keep React/TypeScript/Vite, the Python administrative API, existing PostgreSQL
control-plane storage, and stateless Flight workers. No extra JavaScript API
server, new storage product, or framework rewrite is required. Browser and admin
API share one origin. Flight readers use separate credentials and authorization.

Use established OIDC authorization-code flow with PKCE, state, and nonce through
the administrative boundary. Choose a maintained implementation during P02;
verify current official documentation before adopting its API. Do not implement
OAuth cryptography or token validation in custom browser code.

Proposed session contract for review in P00: an opaque random HttpOnly browser
cookie points to a revocable administrative session. Store only the minimum
needed server-side, hash session identifiers, protect retained provider tokens,
and enforce absolute and idle expiry. Prefer reauthentication over implementing
refresh in the first release. Reuse the existing control-plane database; Flight
stays stateless. New session, capability, draft, evaluation, operation, and audit
records require an additive migration design and explicit approval under the
repository's persistence rule before those schema changes are implemented.
Do not infer that this plan itself grants that approval. Prepare the migration,
retention rules, and backup/restore test proposal as the review packet first.

Proposed capability contract for review in P00:

- Reader identity: governed Flight reads only; no administrative access by default.
- Metadata viewer: scoped asset/schema metadata, not policy bodies or source rows.
- Asset owner/editor: scoped policy read, draft edit, synthetic evaluation, and
  allowed history. Ownership does not grant publication or owner reassignment.
- Publisher: read/evaluate/review/publish only explicitly granted asset scope;
  editor permission is separate. No unrelated drafts bundled in initial publication.
- Connection operator: scoped connection registration/diagnostics, secret references,
  and asset binding; no implicit policy edits, publishing, or data reads.
- Grant administrator: assign/revoke capabilities and ownership; no implicit source
  reads. A deployment may explicitly assign additional publisher/editor roles.
- Auditor: scoped audit and policy-history access, with separate sensitive-detail
  permission; no mutation.

Deny unspecified actions. Scope every action by workspace and asset/connection.
Check list results, counts, cursors, direct IDs, history, export, preview, and
publication impact. Only an administrator may change grants. Do not add a
second-person approval workflow; explicit publishing capability plus review of
an exact revision is sufficient for this scope.

Local parity means the same login, session, permission, CSRF, validation,
publication, and read enforcement code. Use local TLS and a local Keycloak issuer,
separate admin and reader audiences/clients, generated secrets, and loopback-bound
host ports. Different local data, hostnames, certificates, and capacity are fine.
One-click privileged impersonation and disabled certificate verification are not
parity. Remove shortcuts from the default supported path; any retained disposable
prototype must be isolated, opt-in, and incapable of satisfying acceptance tests.

## 5. Ordered implementation packets

### P00 — Freeze contracts and build the execution baseline (U00/U03)

Read all routes under `control_plane/interfaces/routes`, their application services,
`common/config_store/orm.py`, migrations, and current local Compose/seed scripts.

Slices:

1. Inventory every UI action and API/CLI operation: existing, reusable, missing,
   or unsafe. Map each to permission, scope, request, response, and error behavior.
2. Write the final action matrix and session/migration proposal from section 4.
   Include revocation latency, expired-session logout, trusted proxy handling,
   cookie/CSRF/origin rules, login abuse limits, and bootstrap credential lifecycle.
3. Define typed API contracts and synthetic fixtures; name exact new routes once.
   Generate TS types from OpenAPI after implementation, not hand-maintained mirrors.

Tests/evidence: negative permission cases written before authorization changes;
one route inventory checked against OpenAPI and CLI entry points. A capable
reviewer resolves the session/migration/capability contract before dependent work.
Status: contract approval pending, not assumed. Commit `docs(ui): fix execution contracts`.

### P01 — Repair installed startup and container assembly (U02/U10)

Files: `pyproject.toml`, new `control_plane/interfaces/cli.py`, existing API factory,
health routes, root Dockerfile/entrypoint, `ui/`, Compose and `prepare_demo.py`.

Slices:

1. Restore the control-plane CLI and entry point using existing factory/services.
   Parse/validate explicit configuration; refuse missing auth configuration;
   never log credentials. Check migrations at startup; run upgrade separately.
2. Pin frontend package-manager version and build inputs. Exclude host artifacts
   and secrets from contexts. Serve real built assets and same-origin APIs.
3. Make Compose order migration, IdP readiness, control-plane readiness, fixture
   provisioning, and Flight readiness deterministically with bounded waits.
   Do not reset or reseed existing authoring state on routine startup.

Tests: build/install a wheel into a clean environment; run installed CLI `--help`;
launch actual process with fixture settings; absent configuration fails clearly;
readiness fails with unavailable DB. Build containers from a clean checkout.
Verify deep-link refresh, missing JS asset 404, API 404 as JSON, cache policy,
CSP enforcement, non-root runtime, and graceful termination.
Gate: no missing executable or missing build-context file. Container runtime
unavailable means blocked runtime evidence, not success. Commit each slice.

### P02 — Real login and revocable sessions (U03)

Depends on approved P00 session/persistence contract; P01 for real-stack tests.
Files: session routes/helpers, deps/API configuration, migrations/repository
adapters, realm template, local proxy/TLS config. Do not touch pickle.

Slices:

1. Add OIDC login/callback through the chosen maintained library. Validate issuer,
   signature, audience, state, nonce, PKCE, code replay, and bounded return URLs.
   Use the admin audience; a reader token is not an administrative session.
2. Implement approved opaque sessions with rotation, expiry, revocation and
   bounded storage. Use Secure, HttpOnly, appropriate SameSite cookies, narrow
   domain/path rules, and `Cache-Control: no-store` for private/session responses.
3. Bind CSRF protection to the session. Validate allowed Origin where applicable,
   enforce trusted proxy configuration, and prevent login CSRF. Preserve authorized
   CLI bearer use without treating a cookie request as a bearer bypass.
4. Implement logout that invalidates server state, expires cookies even when
   authentication has expired, and reports network failure truthfully. Do not
   claim provider-wide logout unless implemented and tested.
5. Make local Keycloak use the same browser flow over verified local TLS.
   Disable default password-impersonation routes. Keep secrets out of images,
   browser storage, JSON responses, diagnostics, and test artifacts.

Tests: real IdP happy path plus bad/replayed state, wrong nonce/audience/issuer,
expired session, revoked session replay, missing/wrong CSRF, foreign Origin,
forged forwarded headers, login downgrade, open redirect, and logout after expiry.
Restart/revoke across two API processes; reject revoked session on the next
administrative request. Local development must pass these without bypass flags.
Gate: independent session review before production credentials. No claim that
basic demo cookie tests satisfy this packet.

### P03 — Enforce capabilities on every administrative operation (U03/U04/U08)

Depends on approved P00 grants contract. Files: `application/access.py`, narrow
permission service/port, route dependencies, repositories, policy/version services,
settings/catalog/asset routes, and all CLI publication paths.

Slices:

1. Implement explicit grants and scoped capability queries; authorize before
   loading sensitive records. Return only safe session/capability metadata.
2. Apply filtering and permission checks to reads, counts, lists, pagination,
   schema, policy/preview, history, activity, diagnostics, and exports.
3. Separate edit, publish, owner/grant management, and connection operations.
   Check every affected resource, including initial workspace publication and CLI.
4. Close legacy endpoints that bypass new checks. Recheck capabilities on every
   mutation; UI cache does not grant permission. Audit permission changes safely.

Tests: full role/action matrix through real HTTP, direct-ID guessing, out-of-scope
cursors/counts, wrong-workspace IDs, malicious names, changed grants in an open
session, first publication with unrelated drafts, and denied CLI publication.
Expected: reader admin requests denied; owner edits own draft but cannot publish
without grant; publisher cannot alter rules without edit grant; revocation takes
effect next request. No protected detail appears in errors. Gate: matrix green.

### P04 — Reliable UI shell, session lifecycle, and tests (U01/U02/U03)

Files: split `main.tsx` into app shell, session, API, asset workspace and policy
features; add frontend unit/component and browser test tooling. No cosmetic
framework migration. Use maintained versions verified at implementation time.

Slices:

1. Establish navigation/deep links, loading/empty/denied/expired/error states,
   accessible forms and focus, and generated API types with safe error codes.
2. Add abortable requests and a session epoch. Discard old responses after logout,
   identity switch, asset switch, or a newer request. Clear all private caches and
   headers on invalidation. Preserve unsaved edits within the authorized session.
3. Fix header merge order, disable duplicate submissions, and show actual actor,
   workspace, capabilities and environment. Demo must be explicit and isolated.
4. Add route/asset-switch and tab-close unsaved-change protection. A failed logout
   must not say the session ended; hide private state and offer retry appropriately.

Tests: delayed login/load/save/preview after logout; logout failure; 401/403/409;
rapid asset switching; empty rules; last-rule removal; user changes while save
is in flight; malformed mask values. Tests must inspect visible behavior, not
source text. Establish browser login over the real local stack.
Gate: no stale identity data, false save status, or unhandled promise errors.

### P05 — Schema admission and nested tree (U04/U05; W02/W07)

Files: catalog discovery/asset services, canonical `common/query_planning` paths,
Iceberg schema adapter, API schemas, schema viewer and fixtures.

Slices:

1. Load authoritative Iceberg schema without reading table rows. Preserve field,
   element, key, and value IDs; types/nullability; version and fingerprint.
2. Return typed segments and hierarchy in a bounded versioned API. Reject unknown
   or rebound IDs; a manually supplied type string is not admission evidence.
3. Build keyboard-accessible nested expansion, search, partial selection, inherited
   grants/masks, and map-key dependencies. Do not derive paths by splitting dots.
4. Detect schema drift; block stale draft evidence/publication pending explicit
   revalidation. Added fields do not silently enter old parent/wildcard grants.

Tests: literal `a.b` vs nested `a.b`; struct/list/list-of-list/map combinations;
null/empty/null-element distinctions; stable ID rename; same-name new ID; map
value without key grant; 5,000 fields and depth 12. Verify Arrow output, not only
JSON labels. Depends on gateway path semantics; implement missing backend contracts
with reviewed tests instead of declaring nested support out of scope.

### P06 — Durable drafts with conflict protection (U05)

Depends on approved persistence design and P03/P05. Files: new draft service and
repository adapter/migration, draft routes/types, policy editor and recovery UI.

Slices:

1. Create personal drafts from active generation. Store author/resource scope,
   base generation, schema fingerprint, canonical content digest and revision.
   Existing authoring rows must migrate additively without loss.
2. Require expected revision for update/discard; compare-and-swap atomically.
   Return explicit 409 conflict with safe recovery information. Never last-write-win.
3. Implement save/retry/reopen, compare and recover conflicts, visible active vs
   draft state, rule add/remove/order, principals and attributes, restricted DuckDB
   expressions, and all six masks. Preserve unsupported advanced content intact.
4. Save remains inactive. Bind UI responses to submitted revision; newer local
   edits remain dirty. Prevent legacy rule-replacement routes bypassing revisions.

Tests: two sessions edit revision 4 → exactly one update creates revision 5;
loser retains edits; retry after uncertain save reconciles; restart reopens draft;
empty-rule deny-all behavior explicit; mask constants retain types/null semantics;
no SQL rewriting or dropped `when` conditions. Gate: exact canonical round-trip.

### P07 — Authoritative synthetic evaluation (U06; W03/W09)

Depends on P05/P06 and canonical gateway evaluation/transform semantics.
Files: bounded evaluation use case, API/records, shared compiler/transforms,
persona/fixture editor and result/explanation views.

Slices:

1. Evaluate immutable submitted draft revision using canonical policy/Arrow/DuckDB
   semantics and synthetic fixtures. No source credentials or source rows needed.
2. Bind evidence to draft revision/digest, schema fingerprint, persona/fixture
   revision, and evaluator version. Return allow/deny, output schema/values,
   applied masks, AND restrictions, and safe rule references.
3. Add input size, rows, depth, duration, concurrency and result-size limits;
   cancellation/error/timeout is explicit non-success.
4. Render results and freshness; a local edit or changed server revision invalidates
   evidence. Remove the browser rule-copy “evaluation” from acceptance workflows.

Tests: independent expected outputs for all six masks; null containers; denied
persona; two-group AND filters; masked/hidden dependencies; conflicting masks;
unsaved edit vs old saved preview; late response after edit; timeout/cancellation.
Intentionally removing a mask/filter must fail these fixtures. Do not manufacture
policy version 0/1 to suggest authoritative binding. Gate: no stale result is current.

### P08 — Exact review and atomic publication (U07; W05)

Depends on P03/P06/P07. Files: publication use cases/store, operation API, CLI,
changes/review/publish UI. Reuse gateway generation CAS guarantees.

Slices:

1. Produce server-side active-to-proposed diff bound to exact revisions, schema,
   evaluator evidence and expected generation. Show affected resources and
   generation-wide ticket invalidation. Unknown impact remains explicit.
2. Publish with reviewed digest, expected generation and idempotency key. Server
   rechecks permissions, schema/evidence freshness, and compatibility. Atomically
   activate and write audit/operation outcome; no client “validated=true” shortcut.
3. Reconcile timeout-after-commit through operation lookup. Same key/same body
   returns the original outcome; same key/different body conflicts. UI disables
   duplicate clicks without claiming this provides server idempotency.
4. Display committed publication separately from observed Flight readiness.
   Update/disable old direct-publish routes and enforce the same rules in the CLI.

Tests: real PostgreSQL concurrent publishers → one wins; edited-after-review;
revoked publisher; changed schema; wrong-scope draft; unrelated bootstrap drafts;
lost response after commit; stale generation; CLI bypass attempts. Integration:
newly planned governed reads reflect new publication; ticket invalidation matches
existing gateway contract. Gate: no unreviewed or concurrent overwrite.

### P09 — History, restore, audit and runtime verification (U07/U08)

Depends on P08. Add scoped revision/activity pagination, exact comparisons,
restore-as-new-draft, operation status, and runtime observations with timestamp,
source, and freshness. Audit actor/action/resource/outcome without secrets.

Tests: old revision restore requires new evaluation/review/publication; denied
history/export; inaccessible events and counts hidden; stale/unreachable Flight
reports unknown/unavailable, never healthy; restart-required config stays pending.
Transaction tests prove successful activation and its audit record cannot diverge.
Gate: rollback does not directly reactivate unvalidated historical content.

### P10 — Complete management and consumer handoff (U04/U08)

Depends on P03/P05/P08. Split into separate commits for assets, connections,
ownership/grants, diagnostics, and consumer guidance.

Implement authorized search/pagination/onboarding, connection secret references
and bounded diagnostics, schema drift repair, dependency-aware disable/removal,
owner/grant administration, safe activity, and Python/DuckDB/Spark/Arrow examples.
Use existing supported Iceberg boundaries; no retired backend resurrection.

Tests: malicious endpoint/path/labels, forbidden internal metadata endpoints,
unsupported backend, missing secret, unavailable catalog, referenced connection
removal, revoked grants, secret redaction, and no source-data deletion. Snippets
must execute against the supported clients without embedded bearer tokens.
Connection disable and permission changes must explain their actual activation
and ticket semantics. Gate: no placeholder required management destination.

### P11 — Local parity and full journey (U10; W11/W12)

Depends on P01–P10 for completion; run the growing first journey after P08.
Files: Compose, realm/certificate/secret preparation, CLI, provisioning and smoke
scripts, browser tests, Dockerfiles and operator documentation.

Provide one documented startup command and one verification command. The supported
local profile uses real OIDC with TLS, PostgreSQL, synthetic Iceberg, UI/admin,
Flight, and real required consumers. Verify expected TLS trust in host browser,
containers, Python and JVM; never disable verification. Local defaults bind only
to loopback and do not display credentials on ordinary startup.

Tests: clean checkout install/build/start; setup idempotence; down/up preserves
state; readiness timeouts actionable; forbidden and unauthenticated requests;
full journey from section 1 in browser; expected nested DuckDB/Spark/Arrow values;
restart/expiry/revocation/conflict/outage recovery; backups restored successfully.
HTTP smoke, browser test, and real consumer test are separate evidence lanes.
Do not use Python `assert` alone for an operator verification tool that may run
under optimization. Make non-success fail with safe diagnostics and nonzero exit.
Gate: actual local stack passes. A stopped VM is an environment blocker, not a
reason to claim “only startup remains” when feature tests are missing.

### P12 — Quality, usability, independent review and release (U01/U09/U10)

Depends on all earlier gates. Add CI lanes for fast frontend behavior, Python
contracts/security, browser real-stack journeys, PostgreSQL races, packaged CLI
and containers, and all required consumers. Use bounded waits and deterministic
fixtures; record supported Python/Node/browser/Java/Spark versions and lockfiles.

Validate keyboard-only navigation, screen-reader labels/errors/tree behavior,
focus after navigation/modals, 200% zoom, narrow layouts, contrast and reduced
motion. Automated accessibility is necessary but not sufficient. Obtain owner
visual review; run the existing five-participant task protocol when authorized.
Report participant availability separately from engineering completion.

Performance corpus: 10,000 assets, 5,000-field depth-12 schema, 100 rules. Proposed
budgets to approve in P00: initial compressed JS ≤250 KiB; local field interaction
p95 ≤100 ms; first inventory page p95 ≤1 s; bounded 100-row synthetic evaluation
p95 ≤2 s on a documented 4-vCPU/8-GiB reference environment after warmup. Measure
at least 30 samples; report p50/p95, memory and cold-start separately. These are
targets, not measured results. Use pagination/virtualization where measurements
justify it; never improve latency by skipping authorization/validation.

Independent reviewer examines auth/session/CSRF, scope and secret isolation,
publication races, nested disclosure, migration recovery, and local/deployed
parity. Resolve critical/high findings before release. Prepare evidence packet
with exact candidate commit, checks, environment, screenshots, limitations,
backup/restore proof, and open decisions. Owner accepts release; do not deploy
merely because tests are green.

## 6. Golden acceptance scenarios

Use small, independently specified synthetic fixtures shared by API/browser/consumer
tests. Each scenario must have expected decision, visible schema/values, operation
state, and mutation side effects. Do not derive expectations from implementation.

- J1: administrator registers Iceberg asset and explicitly grants owner/editor and
  publisher separately. An ordinary Flight reader cannot open its admin metadata.
- J2: editor grants `profile.name`; a parent read returns only name, never ssn.
  Literal dotted names stay distinct. Collection/map variants preserve structure
  and require map-key permission. Include all six masks and their null behavior.
- J3: editor tests two groups with `region='US'` and `active=true`; rows satisfy
  both. Nonmatching persona is denied. Editing the draft makes prior evidence stale.
- J4: publisher reviews an exact revision; concurrent draft/generation change
  blocks publishing. Lost response after successful commit reconciles to one result.
- J5: publisher restores an older revision as a draft, reevaluates and reviews;
  only then may it activate. Every successful publication is attributable.
- J6: operator diagnoses unavailable catalog/secret and repairs binding without
  receiving source rows, exposing secrets, or changing policies implicitly.
- J7: user signs out while asset fetch is delayed; no late private data reappears.
  Session replay is rejected. Another user sees only their authorized scope.
- J8: restart local stack and reopen persisted draft. Complete the same real login,
  policy, publication and nested consumer tests used for the release environment.

## 7. Progress and evidence protocol

Update `EXECUTION_STATUS.md` after each packet slice. Allowed states:
`not-started`, `implementing`, `implemented-unverified`, `verified`, `blocked`.
“Verified” requires the named acceptance evidence, not only code existence.
A blocked test remains required. Do not count skipped tests as passes.

For every slice record:

- Packet/slice and commit; exact behavior changed and relevant U/W dependencies.
- Red-phase expected failure; green-phase exact command and result.
- Integration environment and actual observed browser/API/DB/consumer results.
- Manual inspection/independent review, with reviewer and date when available.
- Remaining limitations, blocker owner, safe next task and required evidence.

Existing local tests use `uv run pytest`, `uv run ruff check`, `uv run ty check`;
frontend uses `pnpm` within `apps/governance-ui`. New test commands must be added
to package scripts/docs/CI and actually executed before citing them. Run tests at
the appropriate scope; do not repeatedly run the full suite without a reason.
Use `uv` for Python commands. Keep CocoIndex telemetry disabled if using `ccc`.

Milestones:

- M0: P00 decisions approved; P01 startup and package evidence exists.
- M1: P02/P03 secure login and negative permission matrix verified locally.
- M2: P04–P08 first complete nested edit/test/review/publish/read journey verified.
- M3: P09/P10 management, restore, activity and operational workflows verified.
- M4: P11/P12 local parity, accessibility, independent review and release accepted.

Do not present M0/M1 as a working feature-complete application. Do not present M4
as completion of unrelated unfinished gateway or market-validation packages.

## 8. Copy-paste prompt for the implementation model

> Implement `docs/ui-v2/EXECUTION_HANDOFF.md`, using
> `docs/ui-v2/EXECUTION_STATUS.md` as the durable progress record. Read the existing
> UI experience and gateway nested contracts. Recheck baseline gaps. Start with
> P00, prepare concrete decisions/tests for any required review, then perform the
> earliest authorized unblocked slice. Finish each behavior with meaningful tests
> and an atomic Conventional Commit. Never modify pickle logic. Never replace
> real authentication, scoped authorization, nested semantics, or publication
> evidence with frontend assumptions. Keep the same security flow locally and in
> deployment. Use existing authorization; request review only where required by
> unresolved contracts or persistence changes. Continue independent work while a
> gate is pending. Record failures honestly; do not call missing tests or stopped
> services successful. Do not claim feature completion until all packet gates and
> the real browser/consumer journey pass. Do not deploy without explicit authority.
