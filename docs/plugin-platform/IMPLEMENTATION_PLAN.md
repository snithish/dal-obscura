# Historical N implementation plan

**Do not restart this queue.** The [R01–R12 action plan](../experience/ACTION_PLAN.md)
supersedes it after review at eed27772 on 2026-09-20. See the
[reconciliation](../experience/REVIEW.md) for completed portions and remaining proof.

Baseline: `c464152`, reviewed 2026-09-13. **Planning only; release HOLD.**
This replaces the original X00–X23 execution queue. Completed implementation is
removed, with reconciliation in [review](IMPLEMENTATION_REVIEW.md). Historical
details remain in [the archived plan](IMPLEMENTATION_PLAN_ARCHIVE_20260913.md).

## Read before changing code

1. Read this file, [acceptance](ACCEPTANCE.md), [UX](UX_REQUIREMENTS.md),
   [technology](TECHNOLOGY.md), [cleanup](CLEANUP_PLAN.md), then [status](STATUS.md).
2. The owner's latest direction permits breaking API/config/plugin/UI changes.
   Do not add compatibility aliases, dual readers, silent default adapters or
   feature flags that retain the replaced production path.
3. The specific earlier instruction to preserve pickle-based logic still applies.
   Preserve serializer functions, serialized classes/import paths and payload
   semantics. If a proposed cleanup crosses that boundary, leave that portion
   unchanged and identify the conflict. Do not silently rename or shim it.
4. Stateless Flight workers, core authorization, DuckDB SQL masks/filters,
   streaming Arrow and parallel scan planning remain requirements. Durable
   control-plane state uses the existing database; no new state service.
5. Target one isolated deployment/database/key set per customer. Deliver three
   qualification pairs: SQL-Iceberg/Iceberg, REST-Iceberg/Iceberg,
   manifest/Parquet. Consumer targets: Python/Arrow, DuckDB and Spark/JVM.
   Other frameworks use the documented Arrow contract, with support claims only
   for executed version cells.
6. Do not run migrations, erase local/customer data or publish/deploy artifacts
   as part of this planning task. Later implementation must use disposable fixtures.
   Breaking code compatibility does not authorize deleting customer records.

## Execution protocol for a less experienced agent

- Pick the first ready packet below. Prerequisites are packet IDs, not suggestions.
  Work independent ready packets if external evidence blocks another.
- Read the listed files and their callers. Locate the existing owning test before
  adding one. For a bug, show one failing behavioral case, implement, then pass it.
  Existing user authorization permits routine implementation; honor any new review
  instruction when implementation actually resumes.
- For each slice, write the precise observable outcome, removal list and test owner
  in the ledger. Do not turn every helper or sentence into a test.
- Deliver one behavior per atomic Conventional Commit. Remove superseded callers,
  code, configuration and tests in the same slice. A temporary adapter may exist
  inside an uncommitted edit, never as the delivered architecture.
- Update obsolete route/version/prose assertions in that same owning slice.
  N14 consolidates remaining duplication; it is not permission to leave earlier
  commits failing tests or retaining obsolete runtime paths to satisfy snapshots.
- Run relevant tests, changed-language lint/type/build checks and update evidence.
  Never equate a source-string check, mock provider, skip, screenshot or successful
  build with the complete behavioral requirement.
- A packet has functional requirements (FR), non-functional requirements (NFR),
  an acceptance owner, and an explicit stop condition. “Done” requires all of them.
  If implementation exists but evidence is absent, record VERIFY; do not rewrite it.
- Each commit note records production/test logical SLOC delta, packages/modules
  added and removed, invariant IDs covered, commands/outcomes and remaining work.
  Logical code means normally formatted source, not minified line counts.
- Do not relax an acceptance threshold after failure. Record the cause and fix or
  propose an explicit contract revision. Do not contact reviewers without permission.

### Commands and verification ownership

Use existing commands: `uv run pytest <owning-file-or-node> -q`,
`uv run ruff check .`, `uv run ruff format --check .`, `uv run ty check`,
`pnpm --dir apps/governance-ui check`, `pnpm --dir apps/governance-ui build`,
and `mvn -f connectors/jvm/pom.xml verify` for changed JVM behavior.
N01 records exact environment/setup and selected test nodes in the ledger.
N06 replaces the UI `test` script with the selected component runner and adds
`test:e2e` for Playwright; N14 wires their path-aware CI ownership. Do not maintain
two runners for the same UI checks. Use the existing CI wheel/provider/production
jobs for release proof and record the exact dispatched job and artifact.
Documentation-only validation checks links/IDs/whitespace, not the runtime suite.

## N01 — Establish the lean baseline and supported toolchain

**Dependencies:** none. **Findings:** F07/F10/F11. **Acceptance:** B01/B02.
**Start:** pyproject.toml, uv.lock, apps/governance-ui/package.json and lock,
.pre-commit-config.yaml, .github/workflows/ci.yml, tests/architecture.

**FR:** inventory actual resolved versions, runtime support, test collection and
durations. Apply TECHNOLOGY.md selections in one toolchain slice. Record a baseline
of formatted production/test logical SLOC, duplicate contract owners, dependency
count, UI gzip size, cold/warm check duration, slowest 20 tests and mandatory skips.
Use the existing test system; create no custom test runner.
Update AGENTS.md, README entry points and docs/policy-authoring.md to the actual
control_plane/data_plane/common layout, current commands and UI-first workflow.
State the owner's breaking-change exception and protected pickle boundary.
Fix literal obsolete image/toolchain assertions alongside their upgrades.

**NFR:** one Python/Node/pnpm support policy across CI/images/local docs. No prerelease
dependency, unbounded latest tag or guessed compatibility. No broad test hook on
documentation-only commits.

**Smallest test:** one clean install/build/import check per published package type;
reuse package-smoke CI. Remove prose tests only with the coverage map in N14.

**Done:** B01/B02 pass; exact pins/support policy and baseline artifacts recorded.
Commit: `build: align supported runtime and toolchain versions`.

## N02 — Consolidate contracts and delete obsolete production paths

**Dependencies:** N01. **Findings:** F06/F07/F13. **Acceptance:** B03; G01–G04.
**Start:** CLEANUP_PLAN.md paths, common/plugin_api, packages/plugin-api,
public_plugin_adapter, published_config, catalog_service and their callers.

**FR:** one non-serialized SDK contract/validator definition; both planes and
external plugins import it directly. Make the SDK an explicit server dependency.
Remove three-part locks, module aliases, duplicate built-in descriptor definitions,
unscoped secret references, permissive manifest normalization and replaced CLI/API
paths. Implement explicit offline conversion of valid persisted records where
needed, fail unknown records without imports, and require maintenance-mode cutover.
Keep only necessary serialization-boundary adapters and active protocol generation.
Consolidate public policy editing on /draft and testing on /policy-evaluate;
remove /policy-rules and /policy-preview routes and old UI methods after migrating
callers. Preserve internal evaluator/authorization helpers actually in use.
Do not delete underlying published-policy records merely because an endpoint retires.

**NFR:** no re-export shim, dual-version runtime, catch-and-default parsing, or new
copy of a validator. Cleanup slice has a net reduction in handwritten production
logical SLOC; additions for a new feature are measured separately. Preserve pickle.

**Smallest test:** parameterized strict old-input rejection plus one canonical
built-wheel execution; retain protected task fixtures. Do not maintain old success
fixtures whose only purpose is backward compatibility.

**Done:** B03 passes, every replacement has zero production old-path callers, and
fresh install plus explicit migration work. Split contracts, locks and config into
separate coherent deletion commits.

## N03 — Fix pair admission and authoritative mutation contracts

**Dependencies:** N02. **Findings:** F02/F04/F14. **Acceptance:** B04/B05.
**Start:** SDK descriptor, routes/plugins.py, asset_service.py, schemas.py,
catalog/policy/settings routes and repositories, UI api.ts.

**FR:** descriptors explicitly name supported output format IDs and handle versions.
Pair admission requires that declaration, admitted versions and required capabilities;
generic overlap alone never admits a pair. Verify resolved handles match selection.
Return effective actor capabilities and resource revisions. Require expected revision
on existing catalog/binding/owner/grant/runtime/auth configuration writes and expected
active generation on activation; missing precondition is 428, stale is 409.
Use one safe error envelope: code, message, request_id, optional field_errors and
current_revision. Preserve concealed-resource 404 and authenticated forbidden 403.
Allow an authorized publisher to select an existing author's saved draft by
immutable draft ID plus expected revision. Reuse the draft endpoint with an
explicit reference, and carry that reference through evaluate/review/publish.
Verify asset membership and read/publish authority server-side; a reference is
not a bearer credential. Sign reviewer identity and draft author/ID/revision/hash
separately, including them in the idempotency request hash and audit. Do not
copy a draft into the publisher's ownership or add a second approval store.

**NFR:** server owns validation and scope for direct API/CLI callers. No reload on
ordinary requests to hide missing startup admission. Strict response models drive
generated TS DTOs; do not generate another handwritten client library.

**Smallest test:** extend existing API tests with shared-capability wrong-pair and
missing/stale/current revision cases, then generate/check DTOs.

**Done:** B04 backend and B05 pass; B04 browser selection belongs to N10.
Commit pair semantics separately from mutation preconditions.

## N04 — Finish secret, IO and resource enforcement

**Dependencies:** N02/N03. **Finding:** F08. **Acceptance:** B06.
**Start:** secret_providers.py, path_rules.py, catalog validators, REST/manifest
packages, deployment network configuration, tests/integration/test_io_boundary.py.

**FR:** scoped secret references are mandatory and validated against operator-owned
catalog/secret grants; caller-provided scope is not authority to name any environment
variable. Cover every initial/redirect/DNS/metadata/data/delete/manifest destination
with provider transport and deployment network rules. HTTPS/TLS verification and
bounded connect/read timeouts apply to catalog and OAuth endpoints. Cancellation
stops underlying work and closes resources. Keep configured private/local targets
working. Implement only the selected secret provider and required storage backends.

**NFR:** denied destination receives zero requests; credentials never reach tasks,
HTTP/Flight errors, UI, logs or audit. Inventory documented legacy pickle exceptions
without changing serialization. Multi-worker concurrency is bounded by configured
workers × per-worker limits; do not invent a global guarantee or add Redis.

**Smallest test:** one parameterized hostile local transport fixture with destination
counters plus success/cancel probes using actual adapters.

**Done:** B06 passes, network/control split is documented, no background work exceeds
its deadline plus cleanup allowance.

## N05 — Complete normal authentication and authoritative UI permissions

**Dependencies:** N03. **Findings:** F01/F04. **Acceptance:** B07/B08.
**Start:** application/access.py, routes/session.py/deps.py, session_store,
session_api, profile CLI, identity/grant records, secure-local deployment.

**FR:** canonical identity is exact issuer + principal kind + immutable subject/group
identifier using structured fields or unambiguous encoding. No trailing-slash or
delimiter normalization. Convert existing author/owner/grant/session keys explicitly.
Supported local and production use the same OIDC code/PKCE path, HttpOnly cookies,
CSRF, origin and session checks. Replace bootstrap browser-token/demo-password
login with explicit audited operator initialization/emergency procedures. Close
bootstrap before readiness. Session response includes safe effective capabilities.
Bound login/callback and request-body abuse with tested limits and safe errors.

**NFR:** absolute lifetime <=24h, idle <=2h; upstream disabled account/admin-role
removal loses administrative authority within 5 minutes (reauthenticate on privileged
action when freshness expires; provider outage fails closed). App grant revocations
apply on the next request. Logout hides private state immediately and revokes the
server session; failure remains visibly pending. Test two API processes.

**Smallest test:** extend actor/session tests for exact issuer and typed principal
collisions; one real local OIDC browser login/logout/expiry journey.

**Done:** B07/B08 pass; initialize/login/revoke/recover work without browser tokens.
No new password database or homemade identity protocol.

## N06 — Replace the visual foundation and application shell

**Dependencies:** N01. **Finding:** F05. **Acceptance:** B09.
**Start:** apps/governance-ui/src/main.tsx/styles.css, UX_REQUIREMENTS.md.

**FR:** implement the exact UX specification: semantic light/dark tokens, Lucide
icons, accessible primitives, responsive sidebar/header/account menu, typed deep links,
command palette and actionable empty/loading/error states. Separate shell, login,
assets, policy, connections, activity and settings by user-facing responsibility.
Route components own their local form state; remove the old global stylesheet/shell.
Treat provider/asset/policy/audit labels as untrusted text. Apply a restrictive
production CSP and validate same-origin return links; no raw HTML injection.

**NFR:** one component primitive set; CSS modules/tokens; no parallel design system,
remote fonts, decorative telemetry or bespoke focus trap. Sign-out is available at
every width. Never use minification to satisfy size targets.

**Smallest test:** reuse one browser shell journey for navigation, theme, account
menu, mobile and axe checks; visual snapshots only for representative stable states.

**Done:** B09 shell/foundation subset and its UX budgets pass. Finished pages
rerun B09 in N08–N11; N06 does not claim their workflow acceptance.

## N07 — Replace scattered async state and make recovery truthful

**Dependencies:** N03/N05/N06. **Finding:** F03. **Acceptance:** B10.
**Start:** UI App load/restore/publish/discovery flows, api.ts/lifecycle.ts/navigation.ts.

**FR:** one API transport and TanStack Query client scoped by session identity,
resource/revision and query parameters. Pass AbortSignal to fetch. Keep local drafts
separate from server cache. Mutation results use captured session/resource/edit
identity after every await, including reconciliation/finally. Captured identity
also includes the selected draft ID/author/revision when changing review links.
Cache cancellation alone does not guarantee mutation safety. Keep the same
publication operation key during uncertain-outcome reconciliation; never
automatically resubmit publication.
Remove old helper/epoch machinery once replacement owns all callers.

**NFR:** no private cache/localStorage persistence; logout/401 cancels and clears
private state before login renders. 403/404/409/422/429/503 are distinct recoverable
states. Never render success/empty/default values for failed reads.

**Smallest test:** parameterized rendered-component deferred-response scenarios in
B10. No tests solely for increment/equality helpers. One live transport wiring test.

**Done:** B10 passes across load/save/test/review/publish/lookup/restore/discovery/
grants/settings; duplicate clicks cannot create duplicate logical mutations.

## N08 — Complete asset discovery and policy authoring

**Dependencies:** N03/N06/N07. **Findings:** F04/F05. **Acceptance:** B11/B12.
**Start:** AssetWorkspace, VirtualSchemaTree, MaskEditor, AccessView and schema API.

**FR:** asset inventory/search/filters/deep links and first-asset onboarding; full
rule add/duplicate/reorder/delete, typed condition builder, exact field paths, six
typed masks and DuckDB row SQL. Preserve explicitly supported raw JSON editing as
an advanced view with bidirectional validation, not the primary workflow. Display
active-rule grants separately from effective-access test results. Empty policy is
a deliberate deny-all draft with review/publish action. Add local undo/redo without
extra server state. Unsaved guards cover all forms, not only policy.

**NFR:** lossless typed roundtrip; stable focus; 10,000-node virtual tree meets B12.
Unsupported expressions/types explain limitations without rewriting them. No new
evaluator or source-data preview in the browser.

**Smallest test:** one parameterized lossless editor test with all mask value types;
reuse G02 backend goldens. One browser authoring journey including deny-all.

**Done:** B11/B12 pass with usable desktop and narrow layouts.

## N09 — Deliver understandable review, publication and history

**Dependencies:** N07/N08. **Finding:** F14. **Acceptance:** B13.
**Start:** TestsView, HistoryView, ChangesView, review/publish/operation APIs.

**FR:** persona testing identifies synthetic input; before/after review shows exact
saved revision, changed fields/masks/conditions/filters, scope, active generation and
ticket impact. Invalidated review disables Publish with a reason. Publication has
explicit review/confirm/committed/failed/unknown states and safe reconciliation.
History supports version detail, semantic diff and restore-to-draft; restoring
never auto-publishes. Deep links retain asset/tab/version.
An editor can copy a same-origin review link for their exact saved draft reference;
a publisher opens it read-only, evaluates/reviews and publishes that revision.
Show author and reviewer separately. A changed/missing reference requires fresh
selection/review, never a fallback to the publisher's personal draft.

**NFR:** UI never claims active based on a local optimistic state. No unauthorized
history or raw source rows. Current backend review/idempotency services are reused.

**Smallest test:** one live publish/lost-response/reconcile/history/restore journey;
reuse component race matrix and backend transaction tests.

**Done:** B13 passes; novice can explain what changed and whether it is active.

## N10 — Complete generic connection and plugin lifecycle

**Dependencies:** N03/N04/N06/N07. **Acceptance:** B14.
**Start:** ConnectionsView, plugin/catalog APIs, configuration activation service.

**FR:** create/edit existing connection with safe prefilled values, typed descriptor
fields, immutable IDs, scoped secret references, diagnostic detail and bounded
searchable discovery. A table carries authoritative format candidates; user selects
when ambiguous. No Iceberg ID/module conditional in generic UI. Separate draft,
validated, active, disabled, retiring, missing and incompatible states. Activation
shows affected assets and expected generation. Disable blocks new plans and unused
tickets on the next request; already-running streams drain within their execution
deadline. Retirement preserves history and never deletes source data.

**NFR:** no secrets returned or persisted in browser state beyond reference names;
failed validation/activation keeps current generation. Operator installs/pins
plugins outside browser; UI cannot upload or execute arbitrary code.

**Smallest test:** parameterize one lifecycle browser/API journey by three pairs;
test ambiguous-format selection and stale catalog revision.

**Done:** B14 passes; new installed approved descriptor needs no UI source edit.

## N11 — Complete access, settings, audit and consumer handoff

**Dependencies:** N05/N07/N09/N10. **Finding:** F12. **Acceptance:** B15.
**Start:** AccessView, SettingsView, ActivityView, ConsumerView and existing services.

**FR:** explicit effective-capability/reason displays; edit owners/grants with exact
identities and revisions; edit/read/validate runtime and auth configuration; protect
last-admin continuity via candidate login validation and operator recovery.
Audit supports bounded filter/pagination and redacted event detail by actor/action/
asset/time/outcome/request ID. Show control-plane health and measured Flight health
separately. Provide accurate copyable Python/DuckDB/Spark/Arrow instructions and
qualified version badges. Refresh reflects authority changes without page reload.
Implement those audit filters and stable (created_at, id) keyset pagination in
the API/repository first; current limit-only queries are insufficient. Apply scope
through database joins/EXISTS, not materializing all visible asset IDs. Bound page
size to 200, cursors to 512 characters and text filters to 200 characters; never
filter only the returned page.
Use the precise B15 actor fixtures and keep management authority distinct from
governed data access. Do not introduce a second configurable role hierarchy.

**NFR:** all visible controls backed by authorized APIs; disabled controls explain
why. No invented metrics, default settings after failed GET, unbounded audit fetch,
or plaintext credentials in snippets.

**Smallest test:** parameterized permission matrix and one settings/access/audit
journey. Existing backend capability tests remain the security oracle.

**Done:** B15 passes, including forbidden users and safe last-admin recovery.

## N12 — Qualify publication and identity races at process boundaries

**Dependencies:** N03/N04/N05. **Acceptance:** B16; G01/G02/G04.
**Start:** existing PostgreSQL race/API/evolution tests; disposable provider fixture.

**FR:** extend current harness to two API processes and real PostgreSQL. Exercise
publication versus saved draft/restore/grant revoke/owner/catalog/binding/schema
change; initial activation with unrelated draft; duplicate idempotency request;
lost response; process termination before and after commit. Independent DB sessions
and barriers establish ordering. Inspect active generation, history, operation,
audit and actual Flight result together.
Include editor A's referenced draft being changed while publisher B reviews or
publishes it, and revocation of B's authority. This crosses actor identities,
not only two requests by the same publisher.

**NFR:** no sleep-only race oracle, SQLite replacement, mocks for transactions, or
retry that masks incorrect commits. One committed logical operation and no mixed
generation. Existing repository checks remain only if they diagnose a distinct fault.

**Done:** B16 passes with evidence for each interleaving; implementation already
passing the scenario is retained unchanged.

## N13 — Qualify built plugin pairs and real consumers

**Dependencies:** N02/N03/N04/N05/N12. **Acceptance:** B17.
**Start:** packages/*, conformance, tests/plugin_conformance/test_pairs.py,
tests/consumers/test_governed_reads.py, connectors/jvm, existing CI wheel lane.

**FR:** build once, install exact wheels in a clean environment without checkout
imports, create real SQL/REST Iceberg and manifest/Parquet datasets, then read through
TLS/OIDC Flight from Python/Arrow, DuckDB and Spark/JVM. Compare nested schema and
row multisets for all three pairs. Include deletes, snapshots, field drift,
endpoint coverage, expiry, revocation, retries, cancellation and partial failures.
Run deliberately invalid plugin variants through the same conformance runner.

**NFR:** no stub format, descriptor-only assertion or count-only golden establishes
support. Frozen dataset/version/artifact identities. External plugins depend on
the SDK and declared libraries, never service internals. Retain protected pickle.

**Done:** B17 passes for every advertised pair/version/architecture cell.

## N14 — Reduce test cost and measure capacity/UX performance

**Dependencies:** N01/N07/N08/N10/N12/N13. **Acceptance:** B18/B19.
**Start:** tests/architecture, UI tests, pre-commit, CI, evaluation/capacity,
existing benchmark runner and native-resource metrics.

**FR:** each invariant has one primary behavioral test and only boundary-specific
integration proof. Delete redundant prose/source-string/epoch arithmetic tests,
duplicate adapter matrices and obsolete fixtures after mapping replacements.
Fast local lane excludes live/socket/heavy tests; PR gates are path-aware with a
safe full fallback, and the release lane always executes the full matrix.
Run existing five-run benchmark and mixed-load harness, not a second harness.

**NFR:** B18 cost and B19 latency/memory thresholds are mandatory. No global coverage
percentage or test-count target. Collect stable RSS/handle/task metrics without
high-cardinality or credential labels. Fault-injected alerts must trigger.

**Done:** exact collection coverage is mapped; no missing mandatory tests/skips;
record timings, deleted tests/modules, resource results and justified dependencies.

## N15 — Prove secure deployment, recovery and candidate integrity

**Dependencies:** N05/N12/N13/N14. **Acceptance:** B20/B21.
**Start:** production and secure-local Compose, ui image, scripts/backup/restore,
maintenance commands, CI artifact manifest and recovery runbook.

**FR:** install exact server/UI/SDK/plugin artifacts, explicit role-separated
migration and OIDC initialization, first governed read, restart, config activation,
revoke, drain, upgrade and timed encrypted PostgreSQL restore. Invalidate old
sessions/login transactions/tickets before restored ingress. Rotate keys/secrets
and roll forward one supported version set; do not implement mixed-version shims.
Bind release evidence to exact tested artifact digests for every advertised arch.

**NFR:** unprivileged runtime, trusted TLS, network policy, least-privilege DB roles,
immutable images, SBOM/provenance and fail-closed required evidence. RPO <=15 minutes,
RTO <=60 minutes. Rollback uses isolated backup/previous complete artifact set,
not runtime dual parsing. Never automatically discard records to make install pass.

**Done:** B20/B21 pass, including missing-signature/skip/bad-CA failure gates.

## N16 — Accept the product with independent security and UX evidence

**Dependencies:** all previous packets. **Acceptance:** B22; all G/B invariants.
**FR:** prepare a redacted candidate dossier, obtain independent security review,
owner visual acceptance and five representative-user sessions. Run install →
onboard → author → review → consume → revoke → restore from the runbook.
Resolve reported high-severity defects and retest affected journeys.

**NFR:** no fabricated reviewer approval, results or usability scores. Do not
contact participants without permission. Feature richness means completed governed
workflows, not a marketplace/analytics/AI feature expansion.

**Done:** acceptance thresholds pass, evidence matches candidate, no open high
security defect or unsupported claim. Until then release remains HOLD.

## Resume prompt

Read the six authoritative documents linked at the top and STATUS.md. Start at
the first ready N packet. Reuse the existing governed backend and tests, remove
replaced production paths, preserve the specified pickle boundary, and implement
one observable behavior per atomic commit. Meet that packet's FR/NFR and B cases.
Record only executed evidence; blocked live proof remains VERIFY. Never restart
the archived X queue or add compatibility shims to avoid deliberate cutover.
