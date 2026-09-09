# Bounded implementation work packages

Prerequisite: read [README.md](README.md) and [EVALUATION.md](EVALUATION.md). Every package has a tests/expectations review gate before production changes. File paths below are current locations unless marked proposed. A smaller assignment may implement only one subpart; do not treat this list as permission for one giant rewrite.

Every completion packet must include: requirement IDs, changed paths, exact commands/results, independently understandable evidence, migration implications, unresolved issues and next eligible package. A green test count alone is insufficient.

## W00 — Freeze scope and establish reproducible baseline

Depends on: none. Production edits: none; documentation/tooling inventory only.

Read: AGENTS.md, pyproject.toml, uv.lock, CI/pre-commit config, existing docs, package entry points and this plan.

Produce:

- Commit/environment/dependency manifest and baseline test timings, with benchmark skips separately listed. Keep artifacts under proposed `evaluation/baseline/`; no tokens, dataset contents or machine secrets.
- A scope inventory: every retained/removed component, active imports/entry points, test coverage, external command, dependency and release job affected.
- Proposed updates to AGENTS.md: actual paths, narrowed scope, no pickle, security invariants, current commands, typed ports and test-review gate. Preserve caveman/ccc usage requirements unless owner changes them.
- An issue ledger mapping the prior findings to correction packages or explicit removal/rejection tests. No finding silently marked fixed because a file disappeared.

Checks: current full tests once, Ruff lint/format check, ty; record failures without editing expectations. If local socket binds are denied, rerun with suitable execution permissions and distinguish environment from defects. Retain and baseline the existing JVM/Spark build; UI is the retired surface. Record exact Java/Spark/Scala versions before changing compatibility.

Done: baseline and deletion inventory reviewed; every known finding has an owner package; no untracked implementation changes.

## W01 — Preserve evidence with regression tests

Depends on: W00. First run existing behavior tests; do not repair production code here.

Read: plan/fetch use cases, DuckDB transform, policy resolver/compiler, published registry, SDK and test fakes.

Write separate focused regressions for:

1. Wildcard grant + null parent mask + dotted child projection must never reveal the child. Reproduce current leak using real policy evaluation and plan/fetch with a stub storage backend. Expected behavior is correctly masked authorized nested data; reject only specifically invalid projections beneath scalar replacement masks. Blanket nested rejection is not a fix.
2. Same issuer/subject with removed group or changed relevant attribute cannot exchange an older ticket. Use real policy evaluation; a fake that ignores principal claims is unsuitable.
3. Empty/unknown/invalid masks cannot be published or silently omitted.
4. Published logical alias resolves to its configured physical identifier/options; conflicting catalog defaults cannot override it.
5. SDK first batch is available without calling `read_all()` or waiting for a gated second batch.
6. Rule-order changes do not weaken the supported mask result; all six retained mask types follow the reviewed conflict/applicability contract.

Use tiny synthetic data; no huge RSS benchmarks. Expected failure must be the semantic assertion, not import/setup failure. Keep observed output in the red-phase packet. It is acceptable for tests asserting future strict models to be introduced in W02 rather than faking nonexistent APIs here.

Done: owner has reviewed concrete failure cases. Current unsafe behavior is not converted into a permanent passing expectation. No production edits before that review.

## W02 — Define strict configuration and decision models

Depends on: W01 review. Main production scope: common policy/config/identity contracts; split into subpackages if touching more than three substantial modules.

Tests first:

- Strict JSON parsing rejects duplicate keys, unknown fields, missing required fields, booleans masquerading as integers, NaN/infinity, wrong-shaped collections and oversized inputs.
- Every valid/invalid case for all six retained masks, including nested leaves and compound-mask applicability, follows NESTED_AND_CONSUMER_CONTRACT.md.
- Published assets require logical ID, physical identifier, allowed roots, catalog reference and validated schema fingerprint; unknown backends reject.
- Struct/list/map combinations, literal dotted field names versus nested paths, null containers and leaf field IDs resolve unambiguously. Invalid paths/types reject specifically. New schema never inherits an existing wildcard grant silently.
- Column requests are explicit: wildcard alone supported; mixed wildcard/name requests, duplicates and empty lists reject. Explicit unauthorized leaves reject the request; parent/wildcard projection produces a pruned authorized field tree, or denies if none. Spark zero-column/count behavior has its own reviewed adapter tests.
- Deep immutability: mutation of caller-owned lists/dicts after validation cannot modify a stored publication or decision.

Implement:

- One strict model per policy/mask/config/decision/identity concept. Prefer existing Pydantic for external validation and immutable typed internal objects; avoid parallel hand-written coercion parsers.
- Versioned manifest model and explicit support-capability validator.
- Canonical identity context: issuer, subject, fixed namespace, sorted/deduplicated mapped groups, normalized explicitly mapped scalar attributes. Never include raw token or volatile unrelated claims in the fingerprint. Preserve absent versus empty where policy semantics differ.
- Typed errors distinguishing invalid request, denied request, unsupported capability, stale generation, capacity rejection and internal execution failure. Safe public messages; detailed internal event codes without secrets.
- Proposed `ReadDecision` contains authorized visible fields, hidden filter dependencies, masks, canonical policy restrictions and decision digest. Declared Arrow output schema must derive from the same decision.

Done: one authoritative model path; malformed security settings cannot be interpreted as permissive defaults; schema drift forces operator revalidation/republication. Update protocol/config migration examples. Do not implement a general migration that silently rewrites old mask semantics.

## W03 — Implement deterministic policy and transform enforcement

Depends on: W02.

Read: common access_control modules; duckdb_transform.py; get_schema/plan_access shared helpers.

Tests first: S02–S04; zero/one/multiple matching rules; AND row restrictions; all six masks and conflict composition; authorized nested projections, ancestor masks, lists/maps and invalid path requests; field grant failures; hidden policy dependencies; quoted SQL identifiers/literals; NULL comparisons; no source values in errors. Exhaustive small datasets should calculate expected rows in ordinary Python independent of the production compiler.

Implement:

- Resolve one deterministic ReadDecision, including restriction/mask canonicalization, before SQL generation.
- Apply final full policy and caller restriction inside the restricted DuckDB connection. Treat backend pushdown as optional optimization; do not remove final enforcement to improve benchmarks.
- SQL and output schema agree for all six masks and supported primitive/nested shapes. Preserve container/element nulls; validate map key/value authorization; mask resolution uses one field tree shared with schema/projection. Follow the explicit null/type/conflict rules in the nested contract.
- Centralize schema/column validation so `get_schema` and `plan` have the same authorization outcome and output schema. Get-schema does not mint tickets or enumerate scan files unnecessarily.
- Remove permissive skip/coerce behavior for invalid masks/paths. Implement valid nested projections; do not disable nested data to eliminate a failing security test.
- Keep SQL expression allowlisting and disabled external access/extensions. No arbitrary user SQL execution, UDFs or expressions outside the approved grammar.

Done: valid nested reads and ancestor-mask regression tests pass; mutation of grant/mask/filter application causes tests to fail. Reject if output schema differs from actual emitted Arrow schema; do not silently cast away values to make a test pass.

## W04 — Bind fetch to validated current identity

Depends on: W03.

Read: identity_oidc_jwks.py, identity_claims.py, identity port, plan/fetch use cases and HMAC codec clock usage.

Tests first:

- Wrong issuer/audience/algorithm, unsigned token, expired/not-yet-valid token, missing subject and malformed mapped claims reject.
- Same subject from another issuer cannot reuse a ticket.
- Removed group/changed mapped claim reject old ticket before reservation or scan; reordered equivalent groups do not.
- Identical relevant context in a refreshed token remains compatible, subject to the ticket's original expiry.
- Exact expiry boundary; ticket expiry <= original identity expiry and configured TTL.
- JWKS key rotation uses bounded fetching; unknown key IDs cannot induce unbounded refresh requests. Stale key cache behavior and network failure are documented/tested.

Implement: identity result carries validated issuer and expiry; authenticate each schema/plan/fetch request; re-evaluate authorization at fetch using stored original authorized request/decision inputs; compare identity context and decision digest. Do not trust client resubmission. Change to namespace mapping from deployment configuration only.

Use an injectable clock for deterministic unit tests. Include real signed-token integration tests against a local fixture issuer; no external IdP network required for ordinary tests. Keep online role revocation limitations explicit.

Done: S05–S07 pass through the Flight endpoint, not only direct helper tests. Unauthorized callers cannot burn another user's valid ticket or initiate storage access.

## W05 — Make publication and physical binding atomic

Depends on: W04.

Read: compiler.py, published_config.py, PublicationStore, CLI wiring, config-store ORM/migrations.

Tests first:

- Logical asset alias differs from physical Iceberg identifier; configured binding wins over catalog defaults.
- Deliberately activate between asset lookup and authorization: request uses one generation or rejects, never mixes them.
- Two publishers with the same expected generation: exactly one activation succeeds.
- All older tickets reject after any generation change.
- Schema fingerprint changes reject until revalidated publication; data append with unchanged schema can plan a new snapshot.
- Incompatible runtime fingerprint makes replica unready and rejects new requests; compatible asset/policy change is observed without stale auth fallback.
- Repeated generation changes do not produce unbounded cache growth. Simulated DB outage fails closed; no stale-config serving mode.

Implement: immutable request-scoped PublicationSnapshot, typed AssetBinding, one active generation read, compare-and-swap activation, content digest and bounded cache generation leases. PublicationReader performs IO; pure policy evaluator receives the snapshot. Remove unconditional global lock over unrelated DB operations. Retain useful existing repositories but separate reads, publication transactions and ticket storage.

Done: S08/S15 plus binding regression pass against PostgreSQL and deterministic unit barriers. No target fallback to a client path/URI. Old-generation cached data cannot make a denied/stale request succeed.

## W06 — Replace ticket serialization and preserve atomic replay limits

Depends on: W05; W07 finalizes the payload scan-data variant. Implement strict envelope and generic typed storage first; legacy pickle path must be gone before G2.

Tests first:

- Strict schema/version/size checks, malformed encodings, duplicate JSON keys, altered reference/digest/payload/MAC, unsupported key ID and expiry equality reject.
- Identical concurrent fetch attempts across two gateway processes: exactly one reservation wins on real PostgreSQL.
- N-ticket insertion failure rolls back all N; no partial usable plan returned.
- Failure after reservation remains consumed; failure before authorization does not reserve.
- Ticket produced by one replica can be validated/exchanged by another with the same configured keyring/namespace.
- Secret sentinel never appears in DB serialized payload, ticket, logs or exceptions.
- Every legacy pickle ticket rejects before any pickle load/import path can execute.
- Authenticated retry/speculation issuance binds new attempts to the original plan/snapshot. Issuance idempotency, per-partition quotas, concurrent attempt limits, token refresh, plan expiry and current-generation checks pass independently of ticket reservation tests. Cross-language attempt references use golden fixtures.

Implement: TicketEnvelopeV2 and typed stored record from README, canonical MAC binding to payload digest, shared injected clock, purpose-separated signing/integrity keys and bounded key rotation. Stored payload includes a strict ScanPlanV2 data shape, never executable objects. New protocol/schema version is mandatory; no unsafe backward-compatibility decoder.

Use bulk creation in one transaction. Keep atomic conditional SQL reservation and verify returned identity/generation/integrity. Cleanup is bounded, rate-limited maintenance outside every plan request; ensure it cannot delete another namespace's rows. Test fairness/backoff if multiple replicas perform maintenance.

Split W06 into envelope/reservation and logical-plan/attempt-issuance subpackages. Follow NESTED_AND_CONSUMER_CONTRACT.md section 7: no ticket refunds, no fresh-snapshot retry and no raw-token task descriptors. Single-use is per attempt ticket, not a prohibition on authorized distributed task retries.

Migrate forward without rewriting old records into apparently valid V2 tickets. Invalidate old tickets explicitly. Do not remove durable authoring records. DB-admin compromise remains outside the security claim.

Done: S06/S10/S11; PostgreSQL concurrency evidence; serialized size limit tested. If cross-replica correctness or key handling is unclear, stop for review rather than improvising a new cryptographic scheme.

## W07 — Build a safe, pinned Iceberg scan specification

Depends on: W06 envelopes/models and the README investment checkpoint; can include a tests-only feasibility investigation before W06 implementation finishes. Do not start substantial remodeling without a concrete prospective workflow or explicit owner research-continuation approval.

Read: iceberg.py, table_format contracts, exact locked PyIceberg APIs, native ArrowScan semantics and real snapshot fixtures. Record dependency versions and source/documentation links for non-obvious API claims.

Tests first:

- Multiple data files split into tasks; union of tasks equals a native scan of the same snapshot with no duplication/missing rows.
- Append after plan leaves old ticket pinned; new plan sees new snapshot.
- Schema evolution after plan never rebinds a field by name to a different field ID. New incompatible schema blocks new plans until publication.
- Positional and equality deletes apply correctly across files, projection, nested field IDs, null keys and task grouping. Inventory v2/v3 and physical-encoding capabilities; mandatory gaps block completion, and unknown features fail before tickets until implemented. Do not advertise a version on the strength of accepting its version number.
- Empty snapshot and filter-pruned empty scan emit no data without reopening the latest table or rescanning all paths.
- Hidden filter columns are scanned but never emitted. Selected column/type values match native Iceberg and SQL-reference output.
- All metadata/manifest/data/delete-file paths pass storage-root/endpoint constraints; credential material never enters ScanPlanV2.
- Storage object missing/expired snapshot produces safe failure, never fallback to latest snapshot.

Implement in two reviewed subparts:

1. Codec feasibility report and a round-trip test for a tiny native task with positional deletes. Choose explicit data serialization/native reconstruction or deterministic pinned-snapshot replanning/exact matching. Prove correctness before optimizing.
2. Production planner/executor with byte-aware file-task grouping, limits on task/file/serialized-plan counts, immutable snapshot/field-ID context, bounded metadata caching and strict capability checks.

Parallelism requirement: expose multiple file tasks when the snapshot has multiple files. Investigate native row-group/range support for large single files. Use it only if it preserves Iceberg field IDs, partition evolution and absolute delete-row positions. If the locked native backend cannot safely execute sub-file tasks, document API limitation and single-file straggler cost in implementation + user docs, retain native file tasks, and report it at G2. Do not bypass Iceberg semantics with a raw Parquet reader to meet fan-out counts.

Tune grouping by estimated bytes; unknown sizes use a documented conservative fallback. Task order need not imply global row order. Do not promise sorted output.

Done: S01/S11/S12; native scan differential tests pass; no pickle remains on retained read path. One real REST-catalog/object-store integration fixture passes in the dedicated suite. Unsupported table detection must fail closed even when a feature marker/API is unfamiliar.

## W08 — Implement framework-neutral streaming and DuckDB/Spark consumers

Depends on: W07 for end-to-end validation; SDK unit tests from W01 already exist.

Read: python_sdk.py, Flight service/streaming, Java client, Spark datasource, connector fixtures/tests and NESTED_AND_CONSUMER_CONTRACT.md. Split this package into W08a protocol/Python, W08b DuckDB/Arrow interoperability, W08c Java/Spark reads and W08d executor credentials/retry/speculation; each needs a separate test review.

Tests first:

- Gated producer provides first batch while later data is unavailable. `read_batches` must yield without `read_all()`.
- Early iterator close, exception, context exit and deadline release upstream Flight reader and owned client resources.
- Empty result returns exact authorized schema, including `read_table()`.
- Python iterator mid-stream failure surfaces without silently retrying previously yielded rows. Spark task retry is an explicit new attempt against the same pinned partition, with no duplicate committed framework output.
- DuckDB relation receives an Arrow reader rather than a complete table. Relation consumption while client/reader context is alive works; use after closure fails clearly.
- Client import/install works without Iceberg, FastAPI, SQLAlchemy, Delta or JVM runtime.
- Same principal/snapshot yields equivalent nested values/schema through DuckDB, Spark and PyArrow/Polars. Spark tests exercise multiple executors, nested pruning, masked-field residual filters, zero-column/count requests, task failure, speculation and cancellation with fresh same-plan attempt tickets.

Implement: a context-managed stream API with explicit `schema`, iteration, `close/cancel` and a compatible documented batch-iterator convenience method. A generator break alone does not guarantee deterministic closure; documentation must show the context-managed form. Keep `read_table()` as explicit materialization. Default endpoint consumption sequential; any later concurrent prefetch must have bounded queues and justify complexity with W13 results.

Java/Spark implementation remains columnar, uses authoritative authorized schema, and separates logical partitions from execution attempts. Implement safe predicate capability metadata without exposing policy literals. Spark evaluates unsupported/masked-output predicates as residuals over governed data. Provide a minimal adapter guide and non-DuckDB/non-Spark Arrow example; do not promise native ADBC/JDBC or arbitrary-framework pushdown without a tested adapter.

Support explicit token refresh providers for Python and Spark executors, with no raw token in serialized plans/logs. Never implement an IdP/browser login framework. Set explicit connection/RPC deadline options. Propagate typed terminal errors.

Done: S14, first-batch gate passes end-to-end; DuckDB, local-cluster Spark with multiple executors, and a PyArrow/Polars interoperability example pass the same nested/policy fixtures. Real DuckDB integration proves no whole-result materialization in the adapter. State that DuckDB user's own joins/sorts can legitimately materialize data beyond gateway control.

## W09 — Enforce resource budgets and stream lifecycle

Depends on: W08.

Tests first: EVALUATION performance/failure cases; admission saturation, client cancellation, token expiry, generation activation during read, DB outage, deadline expiration, storage exception, output queue backpressure and resource closure. Use barriers/controllable clocks, not fixed sleeps.

Implement:

- Configured global active-stream limit, bounded wait/admission behavior and one controlled DuckDB connection per admitted stream. Start with `threads=1` per connection; measure before increasing.
- Explicit DuckDB memory limit plus process/container budget; bound Arrow batch/queue input as well. DuckDB's configured limit does not cover all Arrow/native/Python memory.
- Limits for request/ticket bytes, columns, filter size/depth, plan files/tasks, output batch target, row/value size handling, runtime deadline and serialized payload. Reject oversize inputs; do not silently truncate rows.
- StreamGuard checks generation freshness and identity expiry at each output boundary using the <=1-second freshness policy. Publication query timeout is bounded. Policy DB loss stops further output when freshness expires.
- Cancellation signal reaches DuckDB interrupt/native reader close and underlying iterators. Clean up on success, exception, consumer disconnect and explicit close.
- Audit start/completion/cancel/deny/failure events with request ID, hashed/pseudonymous principal identifier as configured, asset ID, generation, snapshot, counts/bytes/durations and safe reason codes. No sensitive values/SQL/tickets/tokens.

Guard against internal queues outrunning policy checks. Do not claim bytes already handed to Flight are retractable. Do not equate authentication-token expiry with maximum time for a table scan unless the stream enforces it.

Done: cancellation and revocation timing meet EVALUATION thresholds; resource measurements plateau under bounded concurrency. Fast path without DuckDB is deferred unless profiling shows material value and a separate equivalence test gate proves projection/schema/security preservation.

## W10 — Remove expansion scope and provide minimal operator workflow

Depends on: W09 and renewed owner review of demand/investment evidence. Destructive DB data operations: none.

Tests first: removed backend/auth/module strings return actionable unsupported errors; minimal manifest validate/preview/publish/status workflow; CLI help; no reader-facing authoring endpoint; unsupported old configs cannot start permissively.

Implement in small subpackages:

1. Retain compiler/publication persistence behind minimal operator CLI. Proposed commands: `dal-obscura-admin validate <manifest>`, `preview <manifest> --personas <file>`, `publish <manifest> --expected-generation <id>`, `status`. Publish produces an explicit diff/summary and generation; execution requires operator credentials. Preview uses the same evaluator, with clearly labeled caller-supplied personas, and is never accepted as authentication.
2. Remove UI/web authoring runtime, owner workflows and retired storage/auth adapters/examples after replacement CLI demo works. Retain and harden Java/Spark modules, contract fixtures, tests and release jobs; retain all six masks and nested-schema functionality.
3. Remove unused aliases/helpers/dependencies and split oversized repository responsibilities. Preserve transactions and migration history. No bulk unrelated renaming.
4. Update docs/examples/entrypoints/AGENTS.md so only supported behavior is advertised. Include a machine-readable rejection/migration report for legacy configuration.

Deletion inventory maps each removed path to retained behavior, rejection tests or Git history. Search entrypoints, imports, packaging data, CI, container files and docs. Do not repair Delta/Avro slated for removal. Partial masks and nested projection remain required repair work. Do not delete old database tables just to reduce line count.

Done: clean install exposes exactly the supported commands/features; no advertised dead links or orphaned job dependencies; minimal operator path provisions synthetic data and authorizes equivalent nested reads through DuckDB, Spark and the Arrow interoperability example.

## W11 — Separate client packaging and deployment privileges

Depends on: W10.

Tests first: build/install wheels in clean environments; client without server dependencies; gateway/admin with required extras; migrations packaged; `--help` works without runtime secrets; TLS failure cases; reader has no direct storage permission; gateway cannot publish config; publisher cannot exchange tickets unless explicitly granted.

Also build/install the Java client and Spark connector artifacts against an explicit Spark/Scala/Java version matrix. Validate a clean multi-executor Spark job from packaged artifacts, with no checkout or storage credential access. Keep the Python client lightweight without removing Java consumers from the product.

Implement: lightweight client package or lightweight core distribution plus explicit `server`/`admin`/`postgres` extras. Pick one in the tests review; keep existing namespace imports where feasible. Ensure import side effects do not require optional heavy dependencies. Pin and test actual supported dependency bounds; a lockfile alone does not prove advertised lower bounds. Support one documented Python version initially (3.12, matching existing CI) unless real users require more; package metadata must agree.

Provide a single-process synthetic local demo and a PostgreSQL + TLS + OIDC + REST Iceberg/object-store deployment example. Use read-only storage permissions, explicit approved roots/endpoints, non-root container, mounted runtime secrets, readiness indicating supported active generation, liveness independent of DB availability and graceful shutdown. Publish neither real credentials nor automatic storage grants to clients.

Migration/runbook covers export/backup, protocol V2 cutover, in-flight invalidation, auth/runtime fingerprint rollout, key rotation, DB restore and rollback. Never roll back to an unsafe pickle decoder or known masking bypass. Retain migration history even after product scope removal.

Done: clean-wheel end-to-end smoke passes; least-privilege checks and supported configuration work on fresh deployment. No hidden dependence on source-tree imports.

## W12 — Reorganize tests and CI around risk

Depends on: W11. Tests/CI behavior changes still reviewed before implementation.

Implement:

- Fast deterministic unit/contract lane; integration lane with real PostgreSQL/Flight; native Iceberg conformance lane; separate scheduled/manual capacity lane; wheel/container packaging lane.
- Replace 20-second startup sleep with readiness polling and early process-failure reporting.
- Share immutable connector-derived retained fixtures where useful; retain and share immutable JVM/Spark fixture construction where safe; keep mutation tests isolated. Do not share mutable DB state between unrelated tests.
- Combine repeated large streaming subprocess scenarios to capture compatible measurements once. Keep smaller functional streaming gates on each PR.
- Remove literal job-name assertions; assert parsed relevant configuration or exercise jobs. Enforce real import boundaries with AST/import checks including runtime imports where relevant.
- Gate CI on security regressions, schema/type contracts, PostgreSQL atomic reservation and package smoke. Remove frontend jobs only when that surface is removed. Retain Java/Spark Maven verification, actual distributed read/retry tests and cross-language protocol fixtures; no compatibility claim without its CI coverage.
- Detect unexpected skips/xfails in supported security/conformance cases. Benchmarks are not a substitute for correctness tests.

Done: fast and integration lanes meet EVALUATION budgets on named runners; all supported critical cases run at least once per PR; retired code removal reduces scope without deleting retained security gates.

## W13 — Independent evaluation and security sign-off

Depends on: W12. Evaluator must not approve its own implementation solely on implementation-authored tests.

Run EVALUATION.md on a frozen candidate commit. Produce `evaluation/<run-id>/` manifest, commands, machine limits, raw metrics, summarized failures, reproduction fixtures and artifact digests. No sensitive partner data in committed artifacts.

Required reviewer actions: trace each emitted field back to authorization; attempt supported/unsupported projection and policy attacks; inspect trust boundaries and failure paths; verify no pickle/dynamic module loading on request paths; audit counter races, key handling, expiry/revocation and cancellation; independently recompute small fixture expected results.

If a cheaper LLM implemented the code, use the owner or a capable independent reviewer for G2/G3. Do not use another copy of the same assertions as independent evidence. Manual/expert review does not replace automated tests; both required.

Done: all release blockers pass, no unexplained skipped tests, performance failures understood and resolved or support scope reduced explicitly. Findings must be fixed with reviewed tests before a sensitive-data pilot.

## W14 — Validate demand while engineering proceeds

Depends on: W00 scope only. Human-facing discovery; do not send messages to third parties without owner authorization.

Owner tasks: recruit 8–10 platform/data engineers with relevant workflows; conduct interviews; find two design partners; approve sharing synthetic demo and research artifacts. Implementer tasks: prepare interview script, evidence log, comparison measurements and minimal demo from EVALUATION.md; do not fabricate demand or contact people autonomously.

Record exact unmet workflow, current alternative, operational buyer, data/schema/delete features, IdP constraints, performance needs, storage credential boundary, willingness to deploy and next concrete commitment. Record concrete nested schemas, Spark deployment/version constraints and online revocation requirements. Record true disqualifiers separately; nested support and Spark are required scope, not interview disqualifiers.

Done: two partners commit evaluation resources and a concrete workflow, or owner receives evidence supporting pause/pivot. A star, compliment, survey response or synthetic benchmark is not a design-partner commitment.

## W15 — Make an explicit release / hold / pivot decision

For release approval, depends on W13 and W14. A hold/pivot/pause decision may occur at any earlier checkpoint based on available evidence; do not complete unnecessary engineering first.

Produce one decision record with engineering gates, partner evidence, supported contract, operational cost, open risks and recommendation:

- Release restricted pilot only when security/conformance gates pass and partner demand is concrete.
- Hold if demand exists but security/operability gates fail; fund fixes, not features.
- Pivot/pause if interviews reveal that existing engines solve the problem sufficiently or no partner will run it.

Preferred pivot experiment: policy tests and change-impact reports for one existing engine, using personas and policy diffs; no new data plane. Alternative: extract a small enforcement/conformance library or contribute upstream. Neither pivot is automatically implemented by this plan.

Done: owner records the decision. Do not infer authorization to publish packages, deploy to partner environments or contact third parties from passing tests alone.
