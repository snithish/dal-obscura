# Acceptance specification — remaining implementation

**Current ownership (2026-09-20):** [R01–R12 and E01–E18](../experience/ACCEPTANCE.md).
G/B cases below remain inherited guarantees, with the explicit amendments there.

Baseline `c464152`; 2026-09-13. These are **test specifications**, not new executable
tests or pass claims. Implement only when implementation is authorized.
Packet owners: [N01–N16](IMPLEMENTATION_PLAN.md). UX details: [UX](UX_REQUIREMENTS.md).

## Evidence and fixture rules

For every case record: candidate commit, test node or browser scenario, command,
exit status, run date, tool/dependency versions, environment/hardware, server/UI/
plugin/consumer hashes, golden dataset hash and evidence artifact path. PASS means
every stated subcase passed. Missing environment is VERIFY; skipped required tests,
mock substitutions or absent artifact identity cannot yield PASS.

Reuse one deterministic nested fixture: two assets A/B; A has at least two files
and multiple row groups, scalar/struct/list/map/null/empty values, decimal,
timestamp, literal dotted field names, explicit field IDs and sensitive email/region.
Persist exact expected Arrow schemas and row multisets independently of the
implementation under test. A 10,000-field schema, 10,000-table discovery fixture
and 10-million-row dataset serve capacity tests. Generate synthetic credentials
and unique leak markers; redact them before saving logs.

Actors: anonymous, unrelated user, read-only, editor, publisher, grant manager,
platform administrator. Federation fixture includes exact issuers with/without
trailing slash, same subject under distinct issuers, delimiter characters and a
subject resembling a group identifier. Use isolated databases/customer fixtures.

Put fast predicates at their owning module/API; component tests mount real UI;
browser tests exercise the actual built application and backend. HTTP mocks are
permitted only for deterministic UI interleaving tests, not real login, provider,
transaction or consumer acceptance. No sleep-only waits or blanket xfail.

## Existing regression guarantees — keep, do not reimplement

The original [A01–A23 scenarios](ACCEPTANCE_ARCHIVE_20260913.md) remain required
unless explicitly superseded below. Completed implementation stays out of the queue;
regression coverage stays in the smallest owning suite.

- **G01:** A01–A04 selected-only activation, saved immutable review, CAS, atomic
  publication, audited idempotency and lost-response recovery. Owners N03/N09/N12.
- **G02:** A05–A08 canonical mask/filter evaluation, typed nested fixtures, full
  schema digests/budgets and fail-closed field evolution. Owners N08/N12/N13.
- **G03:** A09–A11/A16–A18 bounded admission, secrets/IO, plugin provenance,
  exact Arrow output, parallel scans and cleanup. Owners N02/N03/N04/N13/N14.
- **G04:** A12–A15 real authority/session handling, stale UI isolation, complete
  authoring and configuration-to-runtime activation. Owners N05/N07–N11/N12.
- **G05:** A19–A23 consumers, secure local/production parity, capacity, recovery and
  exact-candidate release. Owners N13–N16.

Where a B case refines an archived numeric target or workflow, the B case governs;
all other A invariants remain required. Packet closure uses its owned subset:
B04 backend closes N03; B04 browser selection closes N10. B09 shell/foundation
closes N06, and each finished page must pass B09 in N08–N11. The full B09 gate
closes only after all those pages exist. Do not make later pages prerequisites
for starting the foundation or mistake foundation evidence for complete UX.

Breaking-change exception: old API/config/plugin success compatibility is no longer
required. Old inputs must fail explicitly and valid persisted records may have one
offline conversion. Mixed-version runtime shims are forbidden. Protected pickle
fixtures, import paths and semantics remain required. Do not broaden that exception
to retain unrelated legacy formats, module strings or unscoped identity/secret APIs.

## B01 — Reproducible baseline and work ownership (N01)

Given a clean checkout, collect the resolved versions, supported-runtime matrix,
test list/durations, package/module counts, normally formatted logical SLOC and UI
compressed assets. Every G/B invariant maps to an existing owner test or a named
planned test location; every old X packet has a disposition in the review.
Pass if the same locked inputs reproduce collection/build, no new empty tests are
added, and the map distinguishes fast/component/live/release evidence.
Primary evidence: baseline report and existing package smoke job.
The operational guide references existing source paths/commands and the current
queue. It must not instruct a new agent to recreate removed layers or preserve
superseded public APIs. Verify guide examples against the CLI/OpenAPI inventory.

## B02 — Current stable, minimal toolchain (N01)

Install the exact lock on documented minimum and release runtimes. Build server,
SDK/plugins and UI with matching local/CI/image toolchains. Type checks include
all package imports without checkout-only paths in the installed-wheel lane.
Pass if dependencies are supported stable releases, no unresolved high/critical
runtime advisory exists, every added dependency has a named replacement/benefit,
and runtime metadata matches executed version support. Nightlies and warning
suppression do not satisfy this case. Primary owner: package/build CI.
Container base images are included, not only language dependencies. Updating an
image must not require retaining an obsolete literal tag to satisfy a prose test.

## B03 — Deliberate break with no compatibility residue (N02)

Supply old module IDs, three-part locks, unscoped secret references and obsolete
config/API shapes: each is rejected before factory import/provider IO. Canonical
inputs succeed through both planes. A source caller/entry-point inventory finds
no callers of removed paths; direct imports of retired non-protected modules fail.
An offline conversion previews changes, converts known records once transactionally,
and refuses unknown mappings without modifying them. Rerunning reports no changes.
Protected pickle fixture hashes and referenced symbols remain unchanged.
Pass requires net production logical SLOC reduction in deletion slices and one
contract implementation. One-time offline migration is not a runtime shim.
Primary owner: existing plugin registry/package-boundary tests plus conversion test.
Retired /policy-rules and /policy-preview routes are absent from OpenAPI and return
404 with no write/provider side effect. Their former UI/CLI callers use revisioned
/draft or canonical /policy-evaluate. Existing evaluator internals and published
policy fixtures still pass; a route deletion cannot remove their shared helper.

## B04 — Real pair compatibility and typed forms (N03/N10)

Install C1 supporting F1 and C2 supporting F2; all share nested-schema capabilities.
C1/F2 must not be admitted. Tampered handle naming F2 for selected F1 fails before
ticket minting. A catalog supporting F1/F2 returns valid format choices and the UI
requires an explicit choice; reordering descriptor metadata cannot change selection.
String/boolean/integer/enum/secret-reference fields preserve types; unsupported
descriptor features fail closed. Pass if a newly approved pair appears without
provider-specific core/UI branches. Primary owner: plugin/catalog API tests.

## B05 — Preconditions and truthful error contracts (N03)

For each existing-resource mutation (catalog, binding, owner, grant, runtime, auth
config), omit revision, send stale revision, send current revision, and race two
writers. Expect 428, 409, one commit, one conflict respectively. Creation uses an
explicit create-only precondition; a duplicate name never silently updates it.
Activation includes expected generation, including explicit “none” for first
activation. Error contains safe code/message/request_id and field errors where
useful; unauthorized asset lookup is concealed 404, allowed resource/forbidden
action is 403. No write/audit-success/generation change after rejection.
Primary owner: parameterized API/repository tests; process race in B16.
Referenced-draft requests require draft ID and expected revision; the draft must
belong to the requested asset and an existing saved revision. A foreign/forbidden
ID is concealed 404; a stale revision is 409; missing preconditions are 428.
Evaluation/review/publication all address that exact content. Review evidence
binds draft author/ID/revision/hash separately from the authenticated reviewer.

## B06 — Secret scope and complete IO/cancel enforcement (N04)

Run real local hostile endpoints with request counters. Try redirect-to-denied,
DNS destination swap, metadata-service IP, non-allowlisted private address,
credential/query URI, path traversal, encoded path, symlink, alternate object
endpoint, returned metadata/data/delete-file URI and OAuth redirect.
Denied endpoint counters stay zero; explicitly permitted local/private paths work.
A user naming an unrelated secret with a forged matching scope is rejected before
resolution. Scan HTTP/Flight/audit/trace/log/UI output and new-plugin task metadata
for the unique synthetic secret: zero occurrences.
Stall provider IO; deadline expires and underlying connection/task closes by deadline
+2s. Valid next request succeeds with no leaked slot. Inspect packet/network counters
in the actual deployment, not only Python validator calls.
Primary owner: extend tests/integration/test_io_boundary.py with transport fixture.

## B07 — Exact identity and privilege freshness (N05)

Distinct exact issuers/subjects/kinds remain distinct through login, saved drafts,
ownership, grants, sessions, audit and API/Flight checks. Slash normalization and
delimiter collision cannot transfer access. Removed application grant fails next
request. Removed upstream admin role or disabled account loses administrative
authority within 5 minutes; after freshness expiry an IdP outage denies privilege.
Run across two API processes sharing the DB. Migration rejects ambiguous historical
identities for operator reapproval. Primary owner: actor/session tests plus live IdP.

## B08 — Normal login/logout and secure local parity (N05)

Start secure-local without mocks. Protected deep link redirects to login; OIDC
code/PKCE state/nonce succeeds and restores route. Bad state/nonce/replay/issuer/
audience/certificate/origin/CSRF fail. HTTPS cookies are Secure, HttpOnly for session,
host-bound and SameSite; session/CSRF/token bytes never enter URLs or localStorage.
Logout removes private content before delayed response and invalidates server session.
Failed logout shows pending/retry; expired-session logout still clears cookies.
No bootstrap-token or demo-password route enables browser privilege in supported
profiles. Operator initialization/recovery works and is auditable.
Configured login/request limits reject excess with bounded 429/413 responses
without creating sessions or leaking account existence. Security headers include
a tested CSP with no unsafe-eval or unrestricted script source; no inline script
exception is permitted without a nonce/hash policy.
Primary owner: one real Playwright/IdP journey plus existing negative session matrix.

## B09 — Visual system, navigation and accessibility (N06)

Render login, empty/populated inventory, editor, review, connection, audit and settings
at 390x844, 768x1024 and 1440x900, plus 200% zoom and light/dark modes.
Pass all UX_REQUIREMENTS.md requirements: icons with text/labels, visible focus,
account/sign-out at every width, route/back/refresh behavior, command palette,
one dominant action and consistent semantic states. Axe yields zero serious/critical
violations; manual keyboard and screen-reader checks cover focus and virtual content.
No page-level horizontal overflow; data tables/code may scroll inside labelled regions.
HTML/script payloads in asset names, plugin labels, audit details and policy text
render inert. Unsafe external return links are rejected. Run the built app under
its production CSP, including dialogs, generated forms and copy actions.
Primary owner: representative Playwright journeys, not snapshots of every component.

## B10 — Deferred-response isolation and ambiguous outcomes (N07)

Parameterize operations: load, inventory, save, evaluate, review, publish, operation
lookup, restore, history, discover, owners, grants and settings. Start in actor 1/
asset A/catalog C; change scope or edit relevant input before settling the promise.
Success, failure and finally must not update actor 2/B/D, erase edits, clear a newer
pending flag or mark stale review current. Include lookup-success after logout.
401 clears cache/content; 403/503 retain truthful error state instead of empty lists.
Double-click creates one logical mutation. Lost publish response keeps one operation
key, reconciles read-only status, and never automatically posts again.
Primary owner: rendered component tests with deferred transport, one browser binding test.
Do not test only isCurrentEpoch/nextEpoch arithmetic.

## B11 — Lossless authoring and deny-all (N08)

Create and duplicate rules, reorder, edit principals/typed conditions/fields/SQL and
all masks. Use numeric/boolean/string/null default values, keep_last length and
redact text. Save/reload must preserve exact semantic content and order.
Invalid condition/SQL cannot be silently dropped. Undo/redo and unsaved navigation
guards work. Rule selection reflects its own fields, not another rule's union.
Save/review/publish zero rules; subsequent governed reads deny. Unauthorized callers
cannot publish by invoking the API. Synthetic tests never expose source rows.
Primary owner: one parameterized component roundtrip plus existing evaluator/Flight goldens.

## B12 — Large nested tree and responsive editing (N08)

With 10,000 nodes, keyboard expand/collapse/search/Up/Down/Home/End/Enter/Space works,
including off-screen focus, clear-search restoration, literal dotted fields and
list/map segments. At most 200 tree rows are mounted. Every row has stable identity,
accessible name, level, position, expansion and selection semantics.
Over 100 post-load search/selection operations on the B19 runner, p95 <=200ms.
Editor remains usable at 390px and 200% zoom; no inaccessible action or focus loss.
Primary owner: existing tree algorithm tests plus one browser stress journey.

## B13 — Review, publication and history semantics (N09)

Diff the saved draft against active policy; show exact changes, persona, schema,
revision, target, active generation and invalidated-ticket impact. Change draft/
persona/schema/catalog/grants after review: review expires and Publish explains why.
Commit, drop response, reconcile status: one publication/audit outcome and truthful
active version. Compare history versions; restore produces a saved new draft
revision, never activates it. UI shows “Saved draft” and the returned revision,
invalidates previous review and requires fresh review before publication.
Deep-link/refresh reopens same asset/tab/version.
Editor A with read/edit saves a draft and copies its review link. Publisher B
with read/publish and no edit opens it, sees A as author, reviews and publishes
exactly A's saved content without creating a personal copy. B cannot edit it;
an unrelated principal cannot inspect it. Editing A's draft invalidates B's
review. Audit records both author and publishing actor. Reusing an operation key
with a different referenced draft is rejected; a retry of the same request
reconciles the existing operation.
Primary owner: one publication browser journey plus B10/B16.

## B14 — Complete catalog/configuration lifecycle (N10)

For each qualified pair create → validate → discover → select table/format →
govern → edit → review configuration impact → activate → disable → retire.
Existing edit forms prefill non-secret values and reference names only. Every
mutation carries revisions. Failed/stale activation leaves active config and history
unchanged. Next read observes new generation; disabled binding rejects next plan
and unused ticket; active streams terminate by their bounded execution deadline.
Missing/incompatible plugin displays repair steps; no auto-install fallback.
Retirement retains audit/history and does not delete tables/files. Search and
pagination are complete and bounded, including reversed discovery responses.
Primary owner: shared lifecycle journey parameterized by pair, backend activation tests.

## B15 — Administrative completeness and consumer guidance (N11)

Use these seven fixtures: anonymous; authenticated unrelated; read-only (read);
editor (read + edit); publisher (read + publish); grant manager (read + grant);
platform administrator. Capabilities are existing asset management capabilities.
Also test owner-derived read/edit without implicit publish/grant. Parameterize the
API operation/capability matrix once; browser tests cover one allowed/denied action
per capability and confirm displayed controls match it. Do not repeat every widget
under every actor in a separate browser test.

Publish-only actors cannot edit drafts; edit-only actors cannot publish. Grant
managers cannot delegate grant-management or escalate their own authority.
Only platform administrators manage owners, connections/runtime/auth settings.
Management read/admin access never grants Flight access: an administrator without
a matching data policy is denied a read. Conversely, a principal authorized by
data policy but without management grants may read governed data and cannot inspect
private policy-management APIs. Use the same live asset to test both directions.

Configure ownership/grants/runtime/auth using canonical identities,
revisions and validated fields. Refuse removal of the final usable administrator
until replacement login/recovery is verified. Test provider failure during change.
Audit filters/page/detail match permitted actions and redacted data; failures show
request ID and recovery action. Health uses measured observations or explicit unknown.
Execute copied consumer snippets with only endpoint/credential inputs replaced.
No secret/provider password, unsupported badge or fake health metric is displayed.
Primary owner: permission matrix plus management browser journey.

Audit query subcases: seed >=1,000 events with tied timestamps and permitted/
forbidden assets; combine actor/action/asset/time/outcome/request-ID filters.
Pages of 1/50/200 return all and only matching permitted events exactly once in
(created_at, id) descending order. Pin an initial upper bound so concurrent new
events do not duplicate/skip this traversal; Refresh starts a new traversal.
Revoke access between pages: subsequent pages reveal no newly forbidden records.
Malformed or filter-mismatched cursors fail 422; out-of-range limits fail 422.
Cursor length >512 or text filter length >200 also fails 422 before querying.
Filtering precedes LIMIT. Cursor contents never bypass current authorization.
Extend the existing audit API/repository suite; reuse this data for the UI journey.

## B16 — Multi-process publication and revocation correctness (N12)

Two API processes, real PostgreSQL and synchronized barriers race publication with
draft save/restore, grant/owner revoke, catalog edit, binding reassign and live schema
change. Include same/different-author drafts. Terminate worker before commit, after
commit and before response. Inspect publication, active pointer, operation/audit
and consumer output. Each outcome is atomic: either old valid state or new valid
state, never mixed authority/evidence or duplicate logical operation.
Explicitly race author A saving/restoring the selected draft against publisher B's
review/commit; B's personal draft must never be substituted. Revoke B's capability
before commit and verify that ordering determines a denied or prior valid commit.
No test accepts a 500 merely because a race happened. Selected-only initial
activation excludes unrelated incomplete B. Primary owner: extend existing
tests/integration/control_plane/test_publication_races.py with API processes.

## B17 — Three real pairs × three consumer families (N13)

Execute SQL-Iceberg/Iceberg, REST-Iceberg/Iceberg and manifest/Parquet against
Python/Arrow, DuckDB and Spark/JVM: nine required cells per advertised platform/
version set. Use exact installed wheels, real metadata/storage and TLS/OIDC Flight.
Compare complete row multisets and nested schema/mask/filter/null semantics,
snapshot consistency, delete-file effects and no duplicated/missing task rows.
Exercise delete-file semantics for formats declaring that feature (Iceberg);
formats without it explicitly reject unsupported inputs/capabilities. Verify
manifest membership/snapshot immutability without inventing Iceberg delete support.
Include expiry/revocation, early close, retry and mid-stream failure.
Conformance rejects wrong descriptor/handle/schema, oversized output, missing close,
expired work and unapproved factory before unintended execution. Third-party
package imports work without repository source roots. Primary owner: existing
consumer/conformance harnesses extended, not nine independent stacks.

## B18 — Less code and faster meaningful tests (N14)

Every new dependency/module/test has an owner requirement and unique purpose.
Replaced features have no old callers/aliases and deletion slices reduce normally
formatted handwritten production logical SLOC. No overall LOC ceiling forces
inlining, removed security or omitted features. Duplicate tests may be removed only
after the map proves preservation of each unique failure oracle.
Warm local fast lane <=60s; cold <=120s on the B19 reference runner. Documentation
checks <=10s; no full pytest/browser/containers on documentation-only hooks.
Target at least 30% warm-time reduction versus N01 if baseline exceeds 60s;
otherwise hold <=60s while retaining coverage. PR required non-capacity jobs <=15min
critical path on declared runner classes. Slow full release/capacity lane is separate.
Mandatory lanes have zero silent skips; injected representative faults must fail
their owning security/transaction/UI tests. Primary owner: collection/timing reports.

## B19 — Performance, cleanup and reliability (N14)

On a warmed 4-vCPU/8-GiB fixed environment with 10M rows, 10k schema nodes and 10k
discovery entries: schema/discovery deadline <=10s, synthetic evaluation <=5s.
Eight concurrent discovery operations/process, two/principal/process, bounded
configured worker count; excess returns 429 with Retry-After within 1s.
Five comparable runs: median throughput and p95 first-batch latency regress <=15%
versus qualified baseline; report absolute results. 60 minutes at 16 consumers plus
metadata/UI traffic: no OOM/unexplained admitted-read errors/secret leaks; <=2GiB RSS
per Flight worker; warm first/last 10-minute median RSS growth <=10%.
All canceled resources release by deadline+2s. Faults trigger tested metrics/alerts.
Audit load: 1M events and 10k assets, permitted-page size 50, fixed filter mix;
warmed page retrieval p95 <=500ms across 100 queries on this runner. Materialize
at most page_size+1 audit rows and no full asset-ID inventory per request.
Record PostgreSQL query plan and relevant indexes; no N+1 query loop.
Browser cold build: initial JS <=200KiB gzip, route increment <=150KiB gzip,
CSS <=50KiB gzip; p95 post-load interactions <=200ms, LCP <=2.5s/CLS <=0.1 across
five cold loads at 10Mbps/50ms RTT on declared browser/CPU. Measure, do not infer
from bundle size. Primary owner: existing capacity runner plus browser measurements.

## B20 — Real secure deployment and recovery (N15)

Clean build/install, explicit migrate with separate role, OIDC operator setup,
onboard/read/activate/revoke and restart in secure-local and production-reference
profiles. Runtime users cannot DDL; TLS/CA/issuer/audience/origin/plugin faults fail
closed. Private network policy actually restricts provider destinations.
Perform encrypted pg_dump/PITR backup and restore with real tools into isolation,
verify records/history/rows, invalidate old access before ingress, then newly
authenticate/read. Record measured RPO <=15min and RTO <=60min.
Rotate keys/secrets, drain old deployment and start one matching version set;
invalid tasks fail safely. Protected pickle fixtures stay unchanged.
Primary owner: extend existing production/recovery suites and operator runbook.

## B21 — Candidate release chain (N15)

Build once; pass exact wheels/images/lock digests between mandatory gates and promote
those artifacts. Manifest identifies commit, server/UI/SDK/plugin hashes, platform,
consumer/provider versions, test evidence, SBOM, provenance/signatures and audits.
Missing/failed/skipped browser, provider, PostgreSQL, consumer, recovery, capacity or
security evidence prevents release. A label saying passed is insufficient.
Inject tampered wheel/lock/image/signature and missing lane evidence; promotion fails.
No source-tree imports substitute for installed-wheel proof. Primary owner: CI.

## B22 — Human acceptance and product outcome (N16)

Five representative users perform: connect/govern an asset; author nested mask/filter;
explain review impact and publish; revoke access; locate a failed action in audit.
At least 4/5 complete each without facilitator intervention; median simple policy
change <=5 minutes; all five correctly identify draft versus active state and
deny-all impact; mean usability rating >=4/5. Record task times/errors and redacted
notes, not invented survey claims. Owner explicitly accepts light/dark visual design.
Independent security review has no unresolved high/critical findings. Runbook
install/read/revoke/restore succeeds on the same candidate. Any unmet gate = HOLD.
Contact participants only after authorization; pending human evidence is VERIFY.
