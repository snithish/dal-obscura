# Acceptance scenarios and release gates

> Historical snapshot archived 2026-09-13. Do not execute this queue or interpret
> its status as current. Use the [current handoff](README.md) and N01–N16 plan.
> Current acceptance explicitly supersedes old compatibility requirements.

These are required behavioral outcomes for [X00–X23](IMPLEMENTATION_PLAN.md).
They are not existing test results. Test names below are suggested stable names;
record their final node IDs and commands in [STATUS.md](STATUS.md).

## Shared fixtures and assertion rules

- Use a disposable workspace with assets A=`default.users`, B=`default.other`,
  separate owner/editor/publisher/reader/unrelated actors, and two exact OIDC issuers
  containing the same subject. Use only synthetic data and fixture credentials.
- Use real PostgreSQL for transaction/concurrency claims. SQLite may cover service
  logic but does not close database locking or multi-process revocation gates.
- Golden schema includes nullable struct children; lists of structs; maps with
  string and integer keys; literal `a.b` beside nested `a -> b`; literal reserved
  collection-segment names; boolean, binary, decimal, date, timestamp/timezone,
  signed numeric and string values. Add null parents/elements and empty collections.
- Golden data has unique row IDs across at least four files with multiple row
  groups, matching and nonmatching row-filter values, masked/unmasked fields, and
  deleted rows where a format claims delete support. Hand-author expected results;
  never generate expected results by calling the same function being tested.
- Assert row multisets and schemas, not order unless order is promised. Assert
  forbidden fields/values are absent, and denied requests return zero data batches.
  Streaming failure may follow already delivered authorized batches; it must never
  report successful complete output after partial failure.
- For rejected mutations assert active pointer, stored draft, operation record,
  and audit consistency. For denied metadata operations assert the provider/secret
  spy was not called. For budgets assert upstream work stopped and resources closed.
- Use stable codes: validation/review rejection is 400 or schema validation 422;
  generation conflicts are 409; unauthenticated is 401; forbidden is 403 or concealed
  404 according to a uniform documented visibility contract. X00 freezes exact
  endpoint outcomes before implementation; tests must assert one outcome per case,
  not accept all status codes. Flight equivalents are explicit typed errors.

## A01 — Selected-only publication

1. Use `workspace_helpers._provision_draft` to provision A. Add B under the same
   catalog with an allow rule for `unreviewed-reader`. Give both assets owners.
2. Publish A only. Inspect immutable published assets and the active pointer.
3. Assert A exists, B does not. Flight access to B is denied even to B's draft reader.
4. Repeat with incomplete B, an explicit empty personal draft for A, and an existing
   active generation. Incomplete unrelated B cannot block A's publication.
5. Publish B separately; A remains unchanged. Missing required A configuration causes
   no activation. Run through review-required API as well as service unit fixtures.

Suggested test: `test_initial_publication_excludes_unreviewed_assets`.

## A02 — No stale shared-rule review

1. Create an in-memory app with `require_review=True`, a separate review secret,
   and an explicitly authorized test actor. Bootstrap may be enabled for this
   unit fixture only; real production OIDC coverage is a separate gate.
2. Use `_EvaluationCatalog` as the authoritative synthetic schema provider. Set
   shared rules allowing user1, with no personal draft. Strict review must reject
   until an explicit personal draft is saved.
3. Save draft revision N, review it, then attempt a legacy shared-rule replacement
   allowing `different-reader` with masks removed. The legacy route must either be
   unavailable in strict mode or have no ability to alter the reviewed candidate.
4. Publish the token: it can publish only the immutable reviewed personal draft.
   If a relevant candidate changed, it must reject; it must never publish the new
   unreviewed shared content. Repeat with draft edits, asset rebinding, and config edits.

Suggested test: `test_review_cannot_publish_changed_shared_rules`.

## A03 — Review/publication race matrix

Use barriers, not sleeps, to pause (a) after policy resolution, (b) before signing,
(c) after token verification, and (d) immediately before activation. At each point
perform an independent draft edit, grant revocation, asset rebind, connection change,
or competing publication. Release the barrier and inspect the committed state.

Pass only when committed content matches evaluated evidence and all required
generations/permissions at the documented transaction point. Changes ordered before
commit must conflict or be included in a newly reviewed candidate. Also test two
independent first publications and same-key/different-body requests. Run each
ordering repeatedly on PostgreSQL; record all tested orderings, not just a count.

Suggested test module: `tests/integration/control_plane/test_publication_races.py`.

## A04 — Publication transaction and recovery

Inject failure after candidate insert, pointer update, audit insert, and idempotency
record insertion. Each failure rolls back the whole transaction. Retry a successful
identical operation after a simulated lost response: return the original result,
with one activation and one audit event. Reusing its key for different content
returns conflict. Operation lookup remains actor/asset authorized.

## A05 — Canonical mask and filter semantics

Place an unmatched principal's `redact` value first, followed by the tested reader's
different value: output must use the tested reader's value. For two matching
`keep_last` values 4 and 2, input `abcdef` must become `****ef`, regardless of rule
ordering. Repeat with conditions and groups, deny rules, and multiple row restrictions.

Test `null`, `redact`, `hash`, `email`, `keep_last`, and `default`, including nulls,
invalid parameters, nested children, typed defaults, and empty output. Compare
hand-authored goldens with synthetic evaluation and actual governed Flight results.
Unsupported mask/type pairs fail explicitly and produce no review token.

## A06 — Typed nested fixture and path correctness

Literal top-level `a.b` and nested `a -> b` must be two distinct selectable paths;
selecting one cannot authorize the other. Repeat with list/map segments and literal
reserved names. A top-level list/map must support the same valid operations as one
inside a struct. Fixture generation must produce values accepted by the declared
Arrow schema, including non-string map keys and non-nullable supported scalar types.

Omitting rows generates a documented synthetic fixture; supplying `[]` evaluates
zero rows. Both preserve the authorized output schema. Bad values return a redacted
validation error, not 500 with provider/row details. Changing fixture values changes
the evidence digest. Capture provider scan calls: synthetic evaluation makes none.

## A07 — Canonical schema digest and bounds

Construct two PyIceberg schemas with parent list ID 1 and element IDs 2 versus 99;
their digests must differ. Independently vary map key/value IDs, names, nested
nullability, decimal precision/scale, timezone, and field order. Each documented
semantic change produces a different digest. Reconstruct an equivalent schema:
its digest must match across API, evaluation, review, and planning.

Schemas at every limit are accepted if otherwise valid; one above node/depth/byte
limits is rejected. Exercise schema GET, evaluation, review, discovery diagnostics
when they materialize schema, and Flight planning. Check rejection before costly
unbounded conversion where possible. Error bodies/logs contain no fixture secrets.

## A08 — Schema evolution cannot expand approved access

Review a parent/wildcard policy. Add a sensitive child under that parent, add a
top-level field, drop/re-add the original name with a different ID, change collection
IDs/types, and rebind the asset to another table. For every mutation, old approval
must not expose the new/rebound field. Test stale existing tickets and new planning.
Rename behavior must follow the documented identity rule rather than guessed names.

For the Parquet dataset plugin, any schema change requires reapproval. Data-only
snapshot changes with unchanged approved schema must follow the documented consistent
snapshot contract without silently mixing old/new file membership.

## A09 — Admission, configuration, and secret boundaries

Supply `py-catalog-impl`, `py-io-impl`, arbitrary `module`, nested alternate loader
keys, and unknown provider options through API, legacy config, and fake provider
metadata. Reject before import/constructor invocation. The sentinel entry-point
module writes a test marker on import; unapproved discovery/config must not create it.

Valid scoped secret references resolve through both planes; wrong scope, missing
secret, and unauthorized actor fail before provider IO. Inject a unique synthetic
credential into provider exceptions and URIs. It must not appear in HTTP/Flight
messages, UI, audit, logs, traces, or new plugin descriptors/tasks. Inspect legacy
trusted pickle fixtures separately and record any pre-existing exception to this
new-plugin rule; do not alter serialization to make the test pass.

## A10 — Complete IO destination enforcement

Test initial URLs, redirects, DNS destination changes, metadata-returned file/object
locations, manifests, delete files, traversal, symlinks, alternate storage endpoints,
and percent-encoded variants. Denied destinations include unapproved loopback/private
addresses and metadata service endpoints. Prove allowed explicitly configured local
and private catalogs still work. Assert denied request counters remain zero at the
destination, including redirects. Enforce with provider adapters plus deployment
network restrictions; record what each layer proves.

## A11 — Resource admission and cleanup

Provider emits endless pages, repeated tokens, cyclic namespaces, oversized metadata,
and a slow response. Real discovery stops at limits before materializing the whole
result. Eight admitted per-process operations and two per-session is the initial
target; excess requests fail promptly with a documented retry outcome, not an
unbounded queue. Release/cancel every slot and connection; a subsequent valid request
succeeds. Deadline response must also stop work. Repeat across multiple worker
processes and report aggregate limits honestly.

## A12 — Authoritative authorization and session lifecycle

For every asset/config/draft/evaluate/review/publish/grant/history/audit/operation
route, exercise anonymous, unrelated, reader, editor, publisher, grant manager, and
administrator actors. Direct HTTP calls cannot bypass disabled UI controls. Same
subject from distinct exact issuers is a different identity. Issuer string separators
cannot collide. Account rename cannot transfer ownership to a new subject.

Test real login, CSRF, bad origin/proxy headers, expiry, logout, upstream role removal,
bootstrap closure, and revoked permissions across API processes. A revoked browser
session must not regain authority through cached state. Freeze and prove privilege
freshness/reauthentication bounds in X19; indefinite cached administrator access fails.

## A13 — UI asynchronous state correctness

Control deferred responses for load/save/test/review/publish/restore/history/grants.
For each, switch asset, edit draft/persona, logout/login as another actor, and fail
the request before delivering its response. Verify the response cannot mutate the
new scope, mark unsaved content saved, or mark outdated evidence current.

First successful workspace load reaches a visible ready editor. Double-clicking
save/publish produces one logical operation. Logout immediately removes private
content, including management pages; delayed pagination cannot repopulate it. A
failed server logout shows retry status and does not claim server revocation succeeded.

## A14 — Complete policy authoring and accessible nested navigation

Create, reorder, edit conditions/fields/filters/all supported masks, save, reload,
evaluate, review, publish, inspect history, and restore a policy. Assert lossless
content roundtrip. Remove every rule and successfully publish an explicitly reviewed
deny-all draft. Consumer read then fails. Unauthorized actors cannot publish via API.

Navigate a 10,000-node schema with keyboard, including expand/collapse, search,
selection, and focus restoration. Selection describes the active rule. Virtualized
content retains accessible names/roles and stable focus. Test loading, errors,
permission changes, conflicts, empty state, narrow viewport, and unsaved navigation.
Browser accessibility checks must have zero serious/critical violations on required
journeys; manual keyboard checks are also required.

## A15 — Configuration lifecycle reaches the serving runtime

Create and validate a draft connection; it is not active until explicit authorized
activation. Change an active endpoint/secret reference/runtime setting, display
impact, activate with expected revisions, and prove the next governed read observes
the new generation. Failure/conflict leaves current config active and auditable.

Disable/revoke follows the documented new-read/ticket behavior. Retirement preserves
history and never deletes source files/tables. Missing/incompatible plugin has a
repairable UI state. Auth configuration changes have tested recovery for the last
administrator. Restart does not implicitly activate drafts or reconcile grants.

## A16 — Installed plugin admission and declarative UI safety

Build two distributions claiming the same ID: startup fails deterministically.
Test missing package, bad API/config version, wrong descriptor/artifact provenance,
disabled plugin, editable production install, and unapproved installed package.
Unapproved factories never import; no startup/request downloads occur. Failed reload
retains a consistent prior generation except explicit security revocations.

Descriptor forms reject HTML/scripts/remote references and unsupported schema
features. Backend rejects fields the browser permits accidentally. Installed approved
plugin metadata appears without core/UI code edits; it conveys no installation or
authorization power to the browser.

## A17 — Streaming and task lifecycle

Assert core checks task/batch output schemas and removes or rejects undeclared
columns before any emission. Deny execution for expired/tampered/wrong-actor/revoked
tickets before plugin execution. Reapply full approved row restriction despite
pushdown claims. Mid-stream cancellation/error closes scanners, DuckDB resources,
and ticket reservations according to the documented retry policy.

Use multiple files/row groups. Plan splittable work in parallel; explain unavoidable
serial metadata steps. Assert exact coverage with no duplicate/missing rows. A
plugin cannot claim successful completion after a partial scan failure.

## A18 — Real catalog/format independence

Pass SQL Iceberg → Iceberg, REST Iceberg → Iceberg, and manifest → Parquet dataset
using built wheels and real provider fixtures. Explicitly reject unsupported pairs,
unknown formats, changed handle versions, unsupported required nested/delete/snapshot
capabilities, and corrupt metadata before tickets are minted when detectable there.

Review the core/UI diff used to add the external package: no provider-specific
branches are allowed outside plugin/compatibility adapters. The external package
depends only on the public SDK and declared libraries. Conformance must reject its
deliberately faulty variants; a fake package merely returning static metadata fails.

## A19 — Consumer qualification

For every required supported pair, execute pinned Python/Arrow, DuckDB, and Spark/JVM
clients against real TLS/OIDC Flight. Compare golden row multisets, nested schemas,
mask values, row restrictions, deletions, and empty results. Consume every endpoint
once. Test expiry/revocation, partial failure, retries, and early close.

Run documentation scripts without edits other than documented endpoint/credential
inputs. Record server/plugin/client versions, TLS mode, OS/architecture, commands,
result hashes, and failures. Unsupported framework/version cells stay unadvertised.

## A20 — Secure install and local/production parity

Build/install wheels and server/UI images from a clean checkout. Migrate explicitly
with the migration role; runtime roles cannot DDL or mutate unrelated protected
tables. Start services as their configured unprivileged users, verify TLS mounts,
bind and advertised Flight addresses, OIDC callback/cookies, readiness, and first
governed consumer read. Restart all services; configuration/history/drafts persist.

Run the same security matrix under the secure local profile and production reference
profile. Negative CA/issuer/audience/origin/role/secret/plugin cases fail closed. No
source patch, disabled verification, mock auth, hidden bootstrap, or shared migration
credential may be needed for the successful production path.

## A21 — Performance and reliability qualification

X00 records a fixed runner class with CPU/RAM/storage and pinned datasets. Initial
candidate targets below are acceptance targets, not benchmark claims. Changing a
target requires a recorded reason and explicit acceptance of the new contract; do
not relax thresholds after a regression merely to produce green CI.

- On a warmed 4-vCPU/8-GiB reference environment, test 10,000 schema nodes, a
  10,000-entry discovery traversal, and a 10-million-row multi-file dataset.
- UI field selection/search p95 is at most 200 ms after schema load, measured over
  100 operations with at most 200 mounted tree rows. Separate initial network time.
- Schema/discovery operations meet configured 10-second deadlines; evaluations
  meet 5 seconds. Saturation produces bounded admission failures, not runaway work.
- Five comparable benchmark runs: median throughput and p95 first-batch latency
  regress by at most 15% against the preserved qualified Iceberg baseline. Security
  changes that exceed this require documented profiling and a deliberate decision,
  never removal of checks. Report absolute numbers as well as the ratio.
- Run a 60-minute mixed workload at 16 concurrent consumers and metadata/UI traffic.
  Configure bounded worker memory (initial target 2 GiB per Flight worker), measure
  total RSS/native memory, and report rejections separately from admitted requests.
  No OOM, leaked reservations, secret leaks, or unexplained failed admitted reads.
- After warmup, compare first/last ten-minute median RSS, open handles, and pending
  operations. RSS growth is at most 10%; no monotonic unbounded resource accumulation.
  All canceled operations release resources by their deadline plus a 2-second cleanup
  allowance. Metrics/alerts must actually trigger under injected failure.

If the runner cannot execute this workload, leave the gate unverified and record
what hardware is required. A smaller smoke benchmark is not an equivalent pass.

## A22 — Migration, recovery, and upgrade

Exercise dry-run/apply on a baseline DB with drafts/history/grants/session/ticket
records, including unknown legacy module rows. Unknown mappings fail safely and
are reported without importing them. Restart never reruns data mutation implicitly.

Capture an encrypted backup, restore into isolation, verify data/history, invalidate
old sessions/login flows/tickets before ingress, and prove new authorized reads.
Initial recovery targets: RPO at most 15 minutes and RTO at most 60 minutes; measure
and record both. Backup existence alone is not a restore test.

Rotate keys/secrets; test old/new worker/plugin combinations and drain/invalidate
incompatible outstanding tasks. Execute immutable trusted old-ticket fixtures without
editing pickle logic or moving referenced classes. Unknown/incompatible environments
fail readiness/admission, not deserialize with a guessed compatibility fallback.

## A23 — Release evidence is complete and belongs to the candidate

All A cases link to commands, test node IDs, environment, artifact/commit hashes,
and observed outcomes. Mandatory skipped or unavailable tests fail this gate. Server,
UI, SDK, and enabled plugin wheel/image digests equal the tested/scanned/promoted
digests for every advertised architecture. Include dependency/SBOM provenance.

Independent security review covers authentication, review races, plugin/IO trust,
serialization constraints, and operations. Owner UI acceptance and required usability
evidence are recorded. No open high-severity defect or unsupported claim remains.
The operator runbook succeeds for install, onboard, authorize, consume, revoke,
upgrade, and restore. If any prerequisite is missing, release status remains HOLD.
