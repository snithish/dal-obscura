# Evaluation specification

All thresholds below are proposed acceptance targets, not measured capability. Approve them with the test packet before optimization. Freeze hardware, dataset and commands per comparison. Never loosen an assertion simply because current code fails. A changed target requires a written rationale and owner review.

Read [README.md](README.md) for normative scope/security semantics and [WORK_PACKAGES.md](WORK_PACKAGES.md) for dependencies. Case IDs here belong in test names or test metadata and in the completion ledger.

## 1. Evidence and test independence

For every evaluation run record: commit and dirty state; Python/Arrow/DuckDB/PyIceberg/SQLGlot versions; OS/architecture; CPU/memory/cgroup limits; dependency-lock digest; catalog/store topology; TLS on/off; row counts and estimated/actual bytes; file/row-group layout; policy/identity fixture identifiers; randomness seed; concurrency; cache state; exact commands; exit codes; skips; raw results.

Keep small synthetic fixtures reproducible. Expected authorized rows for security tests come from manually reviewed constants or simple independent Python predicates, not the same SQL compiler under test. For Iceberg semantics, compare with a native scan at the exact same snapshot plus independent expected rows for delete fixtures. Never compare only row counts when duplicates/missing rows could cancel out.

Compare schema and row multisets: preserve duplicate multiplicity; compare floats with explicitly documented NaN/signed-zero handling; compare decimal/timestamp values exactly under the declared contract. For large tests, use counts plus per-column null counts and a streaming order-independent multiset digest with independently defined canonical row encoding. Do not use sums alone. Keep small exact comparisons even when large digests pass.

A test mock may replace external IO, not the policy evaluator/transform whose security is being asserted. Fault-injection fakes must assert the call never occurs when authorization should block it. At least one test for each externally visible invariant goes through a real Flight client/server.

## 2. Security and correctness cases

### A — Policy, schema and masking (S01–S04)

- A01: no matching rule -> denied, zero storage scans, zero emitted batches. Asset existence/details not exposed in public denial.
- A02: explicit allowed fields -> exact fields/values; explicitly requesting any forbidden leaf -> whole request denied. Parent and wildcard requests return a pruned authorized nested tree only. Unknown/duplicate/mixed-wildcard/empty projection -> invalid request.
- A03: policy filter references a hidden field; that field is internally read but absent from declared schema and every emitted batch.
- A04: caller filter on unauthorized or masked field rejects, including quoted/case variants; caller filter on an allowed unmasked field composes with policy restriction using AND.
- A05: parent/child masks and nested projections reproduce the old defect, then return correctly protected authorized data. Null parent remains null under child selection; scalar-masked parent child selection rejects specifically. Test structs/lists/maps, mixed parent-child selection, literal dotted names and schema evolution; blanket nested rejection fails this case.
- A06: all six masks over applicable primitive/nested leaf types -> exact values/schema per nested contract. Include null container versus null element, empty collections, malformed email, keep_last(0), conflicting keep_last lengths, typed default literals, decimal/temporal boundaries and quoted/Unicode replacements.
- A07: matching rule order, duplicate identical rules and group ordering do not change decisions. Null dominates; smaller keep_last length wins; identical masks merge; incomparable or conflicting constant masks reject publication. All six documented masks are supported; invalid type/mask combinations reject before execution.
- A08: multiple matching row restrictions intersect, including a contradiction -> empty result. Multiple column grants union. No matching restriction is silently omitted because its rule grants a different requested column; this restrictive behavior is documented.
- A09: invalid masks `{}`, unknown kind, wrong value type, unexpected properties and nonexistent types reject; missing required values reject. Invalid rule/config activation leaves prior active publication untouched.
- A10: SQL grammar attacks reject: statements, multiple expressions, subqueries, external scans, extension loading, arbitrary functions, oversized/deep expressions and unsafe identifier forms. Include malformed Unicode/encoding payloads and escaped literals. Do not rely solely on string blacklist tests.
- A11: a deliberately non-filtering backend returns forbidden rows; final transform still removes them. A deliberately unmasked backend returns source values; final masks still apply.
- A12: schema response, FlightInfo schema and every DoGet batch schema agree exactly. Empty result preserves that schema. Schema fingerprints reject renamed/retyped/added/dropped fields pending republish; a wildcard grant never automatically exposes a newly added field.
- A13: baseline input types include bool, int32/int64, float32/float64, string, binary, date32, timestamp-us without timezone and UTC, and decimal128 cases up to supported precision. For every additional advertised type, add values/nulls/boundaries to this matrix. Required nested cases include structs, lists, maps, nested collections and nulls across Python/DuckDB/Spark. Publish an explicit mapping for Iceberg time/UUID/fixed/decimal/temporal and v3-specific types; required gaps block completion. Unknown extension encodings reject explicitly without silently corrupting data.
- A14: supported SQL three-valued semantics: NULL equality/inequality, IS NULL, AND/OR, IN and supported scalar operations agree with independently expected rows. Test numeric/string type mismatches as safe errors, not permissive casting assumptions.

### B — Identity, tickets and revocation (S05–S11)

- B01: wrong issuer, audience, algorithm, signature, expiry, not-before or missing subject rejects before scan. Same `sub` from another issuer/namespace cannot exchange the ticket.
- B02: group removal, group addition or any mapped authorization-context change invalidates old tickets conservatively. Equivalent group reordering/deduplication does not. Unmapped volatile JWT claims do not change fingerprint.
- B03: fetch reauthorization denies a previously permitted request even if principal ID and policy generation remain unchanged. Compare decision/context; do not merely call authorize and ignore its result.
- B04: ticket issued at time T has expiry min(T + configured TTL, validated token expiry). It fails at exact expiry, one tick afterward, and malformed/coerced expiry inputs. Inject one clock; no real sleeps for boundary tests.
- B05: refreshed JWT with identical relevant context can fetch within original ticket lifetime. A longer-lived new JWT cannot extend an old ticket. Existing still-valid JWT claims cannot be inferred to have changed at IdP; document/test the actual offline-verification contract.
- B06: forged/altered ticket, nonce, expiry, payload digest/MAC, key ID, unsupported version and oversize reference reject. Duplicate JSON fields, NaN and invalid base64 reject deterministically. Public error does not echo the payload.
- B07: database payload mutation rejects before native scan construction. A plain recomputed SHA hash alone cannot authenticate a changed payload. Tests use a secret sentinel and verify no credential leaks into payload/reference/logs.
- B08: 32 concurrent exchanges of the same attempt ticket distributed over two processes sharing PostgreSQL -> exactly one successful reservation. Repeat with synchronized starts and both success/scan-failure paths. SQLite-only testing does not satisfy this case.
- B09: unauthorized caller using another user's authentic ticket does not consume it. Authorized owner can still reserve it afterward. Malformed ticket cannot trigger cleanup/large DB scans.
- B10: partial batch emission followed by backend error -> surfaced terminal error, attempt ticket remains consumed, no automatic replay. A Spark retry obtains a new attempt ticket for the same immutable plan/partition; a separately requested new logical plan may select a newer snapshot. Never conflate these operations.
- B11: insertion failure partway through a multi-ticket plan leaves zero new usable tickets and returns no partial plan. Empty task result behavior is documented and tested.
- B12: generation activation invalidates all older unexchanged tickets, including unrelated assets. Concurrent activation/planning either yields one internally consistent generation or rejects; never returns mixed binding/policy.
- B13: activate restrictive policy during an active stream. After freshness expires, no additional batch is handed to Flight without authoritative revision confirmation. At most one second of verification age; test handoff events separately from later client receipt/network buffering.
- B14: expire JWT during stream, disconnect policy DB, stall DB query or change runtime fingerprint. Stop future handoffs according to the guard contract and release resources. A process must not indefinitely emit using a once-validated identity.
- B15: legacy ticket/protocol versions reject without `pickle.loads`. Instrument or deny that symbol/import during retained-path tests. Malformed records never invoke a class/module named by input.
- B16: approved signing-key rotation handles configured overlap and rejects retired keys. Unknown JWKS kids produce bounded refresh traffic. Startup with insufficient signing key strength/missing TLS in production mode rejects.

### C — Asset, publication and Iceberg semantics (S01/S12/S15)

- C01: logical alias bound to a different physical table resolves only the published table. Changing catalog defaults cannot redirect an existing bound decision within a generation.
- C02: request path/URI/module injection cannot resolve a new source. Test traversal, encoded separators and alternative URI authorities at configured boundary validation. Include metadata and delete-file references outside roots, not merely the top-level table path.
- C03: operator publication uses expected-current generation. Two racing activations cannot both succeed. Restore/rollback requires explicit new activation; no stale cache fallback.
- C04: plan -> append -> fetch reads original snapshot; new plan may see append. Plan -> incompatible schema change -> old fetch still pins old snapshot; new plan rejects until republished. Missing pinned metadata/data fails without reading latest.
- C05: projected Iceberg fields preserve field IDs and values across rename/reorder/evolution fixtures. Unsupported evolved schema fails explicitly. Partition evolution is required: missing native support blocks the capability until a correct reviewed implementation exists. Unknown features still reject safely.
- C06: positional delete fixtures remove expected absolute row positions from correct data files under task grouping and projected reads. Equality deletes also have required independent fixtures, including hidden/nested delete keys and sequence applicability. Inventory v3 feature support separately; unknown features reject before tickets, and required capability gaps remain blockers.
- C07: one file, many files, heavily skewed sizes, empty table, all-pruned filter, null-heavy columns and duplicate rows. Task union equals reference scan exactly, including duplicates.
- C08: large single-file row groups use only proven native split support. If limited to file tasks, required documentation and explicit capability indicator exist; no false “fully parallel” claim.
- C09: shared PublicationSnapshot cannot be mutated through caller-owned configuration objects; generation caches are evicted under repeated activation and completed requests.
- C10: production REST catalog plus object store works with scoped credentials. A separate client identity cannot read objects directly. Do not include any test credentials in artifacts.

### D — Transport, client and lifecycle (S13/S14)

- D01: producer exposes batch one then blocks on an event. Python streaming API yields batch one before the event releases batch two. Assertion independent of `read_all` implementation details.
- D02: same gated scenario through Flight and DuckDB relation; consumer owns explicit context lifetime. Document single-pass relation semantics if input reader cannot be replayed.
- D03: consumer closes after one batch, raises locally, times out, or disconnects abruptly. Backend cancel/close fires; DB sessions, threads, file handles and DuckDB connections return toward baseline.
- D04: slow consumer induces bounded buffering. Increasing total rows by 10x does not grow per-stream memory proportionally with fixed file/row-group layout and concurrency.
- D05: exception before first batch and after several batches produces safe typed errors; no raw SQL, credentials, values, filesystem paths or bearer headers in public errors/ordinary logs.
- D06: admission saturation rejects or queues within the documented bounded policy. Rejected work does not open a scan/DuckDB connection. Already admitted work remains responsive.
- D07: giant string/binary values, wide schemas, pathological filters and oversized plans respect configured bounds. Do not claim row count alone bounds memory.
- D08: successful non-retrying consumption of multiple endpoints delivers every partition once; no global row-order promise. SDK does not silently ignore endpoints or replay consumed tickets. Spark retry/speculation uses authorized attempt issuance and framework attempt isolation.
- D09: wheel-only client/server deployment passes TLS/auth/schema/plan/fetch; source checkout unavailable. Client install/import lacks heavyweight server dependencies.

### E — Full nested capabilities and distributed consumers (S01–S15)

- E01: canonical field-ID/path fixtures decode identically in Java and Python. Literal dotted names cannot alias nested fields; list-element/map-key/map-value segments are unambiguous.
- E02: parent grant, leaf grant, parent request, wildcard, explicit forbidden sibling and overlapping projection order produce the exact authorized pruned field tree. Added schema descendants do not inherit an unreviewed grant.
- E03: each mask applies to eligible leaves at multiple depths. Test null parent, null list, empty list, null element, struct with null field, nested list, map with null value and nested map/list combinations without changing source row count or collection shape.
- E04: parent null dominates descendants; parent scalar replacement plus child projection rejects specifically. Masking an ordinary authorized nested leaf must work; rejecting all nested schemas fails acceptance.
- E05: map keys require permission. Hidden keys are not leaked; key-changing masks reject. Authorized key/value projection preserves key/value pairing and nulls. Whole-map null masking remains supported.
- E06: all-six-mask golden values/types, canonical hash version, malformed-email protection, Unicode keep_last semantics, keep_last conflict minimum, default versus redact null behavior and incompatible-mask conflicts pass independent of rule order.
- E07: nested scalar and typed collection predicates follow documented three-valued EXISTS/ALL semantics. Masked ancestor/descendant, collection leaf and map-key inference attempts reject; hidden policy dependencies never appear in output.
- E08: DuckDB, Spark and PyArrow/Polars return equivalent authorized row multisets and nested schema meaning for the same immutable plan, including duplicate rows, decimals/timezones, masks and row filters. Record framework-specific physical mappings explicitly.
- E09: Spark nested column pruning, parent/leaf grants and residual predicates agree with standalone evaluation. Filters on masked columns operate on masked output as residuals, never on forbidden source values. Required residual fields remain authorized.
- E10: Spark count/zero-column projection uses an authorized sentinel and correct post-policy row count, including null sentinel, empty results and no-grant denial.
- E11: kill a Spark task after partial output; retry obtains a new bounded attempt ticket for the same snapshot/partition. Final Spark result contains no duplicate or missing rows. Appending source data during retry cannot change that result.
- E12: force two speculative attempts; both must authenticate and fit limits. Framework accepts one logical partition result, loser closes. Gateway does not claim it emitted only one copy globally.
- E13: executor token refresh works without serializing bearer tokens/storage credentials. Changed groups/issuer/generation or expired logical plan reject future attempts. New valid token cannot extend an already-issued ticket.
- E14: duplicate issuance for one attempt is idempotent until consumed; after reservation no duplicate usable ticket is created. Different forged attempt IDs cannot evade quotas. Race this on PostgreSQL across two gateway processes.
- E15: Java/Spark package-only multi-executor integration and Python clean-wheel integration pass. Spark/Scala/Java and Arrow/framework versions are recorded; untested major versions are not advertised.
- E16: generic Arrow adapter example labels materialization versus streaming honestly and closes owned resources. No automatic Flight SQL/ADBC/JDBC compatibility claim without an implementation test.

## 3. Mutation and adversarial review gate

At least these deliberate local mutations must make a relevant test fail: skip final filter; skip mask; omit hidden filter dependency handling; omit group/context check; accept old generation; bypass payload MAC; replace atomic reservation with load-then-update; ignore positional deletes; call read_all in batch API; suppress cancel; silently accept unsupported schema.

Do not commit mutations. Record test IDs and failure evidence. Mutation testing can be targeted/manual; a new mutation-testing framework is not required. Independent reviewer traces at least one allowed and denied flow from wire input through emitted Arrow batch.

Any confirmed supported-path unauthorized row/value, ignored delete, wrong table binding or fail-open parse is a release blocker regardless of test totals or severity score.

## 4. Performance protocol

### Reference environment

Dedicated or otherwise controlled Linux runner, 4 vCPU and 8 GiB RAM, fixed dependency/container image. Gateway process/container limit 4 GiB for initial measurements; Postgres/catalog processes accounted separately. Record actual CPU model, disk, network and neighbor load. Do not compare laptop results directly with CI targets.

Use 4 admitted streams initially, DuckDB threads=1 per stream, configured DuckDB memory budget initially 256 MiB per stream, finite deadlines and bounded queues. These are starting settings to evaluate, not a proof of total memory safety. Arrow/native buffers and plan metadata also count toward measured RSS.

### Workloads

- P01 correctness/latency: 10,000 rows, approximately 8 columns, one file and 16 small files; no-op grant, filter-only, null mask and redact scenarios. Repeat with 90% and 1% selectivity.
- P02 throughput: 1M and 10M rows with fixed-width fields plus controlled approximately 128-byte string values. Record real encoded/uncompressed sizes. Keep row-group sizing fixed between row-count variants.
- P03 layout: same rows/values arranged as one large file with multiple row groups, 64 balanced files and skewed files (one holds approximately 80% of data). Quantify allowed splitting limitations.
- P04 concurrency: 1, 2, 4 and 8 requested streams; with admission limit 4, excess work follows bounded policy. Two gateway replicas share PostgreSQL for exchange tests.
- P05 consumers: full-speed, rate-limited and disconnect-after-first-batch. Mask/filter mix identical across comparisons.
- P06 control path: plans with 1, 8 and 32 tickets, warm/cold publication/metadata caches, concurrent plans, repeated generation activation. Use real SQLAlchemy/PostgreSQL ticket store, never FakeTicketStore for production claims.
- P07 metadata scale: increasing file counts up to configured plan limit; measure planning RSS, serialization bytes, DB writes and latency. The planner must reject above bounds rather than construct an unbounded list first.
- P08 resource growth: repeated successful reads, cancelled reads, denied reads and generation swaps; compare retained memory/FD/thread counts after warm-up and cleanup.
- P09 nested cost: repeat throughput/memory tests with equivalent struct/list/map data, controlled collection cardinality, null containers and all mask types. Record leaf-pruning benefits and large nested-value limits; flat-only results cannot establish nested capacity.
- P10 distributed consumers: DuckDB full-speed/slow consumers and Spark multi-executor scans, nested pruning, residual predicates, retry/speculation. Measure executor/client memory separately, task skew, retry issuance latency and duplicate attempted work. Admission settings account for Spark's simultaneous partition attempts.

### Baselines and fair comparisons

Baseline N: direct native Iceberg scan from the same machine at the same snapshot and projection. This quantifies added gateway work, not equivalent governance functionality.

Baseline G: direct native scan plus the same approved policy transform locally, with output fully consumed and equivalent schema/values. This isolates transport/control overhead for equivalent output.

Candidate: full deployed gateway + real ticket DB + each required consumer (Python/DuckDB and Spark separately); same data, projection, policy, TLS configuration, output consumption and machine allocation. No `.read_all()` on streaming measurements. Collect cold and warm results separately. Dataset creation is outside measured scan interval; include metadata/planning latency separately and in an end-to-end metric. Spark comparisons use the same cluster/executor layout and an equivalent native-Iceberg policy-enforced reference; do not compare cluster throughput directly with a single-process Python baseline.

Partner baseline: their actual current workflow, measured only with permission. Do not substitute an invented competitor benchmark or compare different hardware/cost boundaries.

Measure at least: plan p50/p95, time to first nonempty batch, total elapsed, rows/sec and output bytes/sec, source bytes if observable, gateway/client peak RSS separately, CPU, DB query/commit count, task skew, cancellation completion time, revocation-check/handoff timing, open FD/thread counts and error rate. Label unavailable metrics rather than guessing. For empty results, record time to completion separately from first batch.

Use high-water RSS plus periodic sampling independent of batch callbacks; callback-only sampling can miss peaks while input is materialized. Record all child processes or avoid attributing child memory to the wrong process. Repeat small latency runs at least 30 times after warm-up; large/capacity runs at least 3 times. Report all observations and median/tail, not just best result. Report noise; do not set tight regression gates from unstable measurements.

### Initial acceptance targets

- Zero incorrect/unauthorized results in every workload; optimization never weakens this.
- Fast unit/contract lane <=15 seconds on reference runner; ordinary integration lane <=90 seconds excluding image pulls/dependency installation. Large conformance/capacity runs have their own published duration and resource budget. Compare to refreshed W00 baseline, not only old laptop timings.
- P01 warm plan p95 <=250 ms for <=32 tickets on local test topology. Cold remote metadata latency reported separately, not hidden.
- P02 warm unmasked stream throughput >=70% of Baseline G under identical conditions. Report absolute throughput and CPU cost; no claim that 70% is universally sufficient for users. Filtered/masked cases judged against equivalent-output G as well.
- P02 first nonempty batch <=2 seconds on the defined local/object-store test topology and does not wait for full source completion. The gated-producer test is mandatory even if timing passes.
- Increasing rows 1M ->10M with same batch/row-group shape and one active stream adds <=256 MiB retained/peak gateway RSS beyond the smaller run, and total gateway RSS stays below configured 4 GiB limit. Report file/task-count effects separately; plan size has independent bounds.
- Under P04/P05, gateway stays within its 4 GiB limit, uses bounded queues and does not OOM. Admission at 8 requested streams obeys limit 4; throughput should not collapse solely because unused connections each allocate independent full-machine thread pools.
- Explicit cancel/disconnect -> scan/query resources close within 2 seconds in the controlled fixture. If a dependency cannot interrupt blocking IO within this budget, set bounded IO timeouts and document measured worst case; sensitive pilot remains blocked until owner accepts an explicit limit.
- Generation freshness <=1 second at output handoff; after required refresh fails or sees change, no further handoff. Separate already buffered bytes. Do not evaluate solely at client receipt time.
- After 100 completed/cancelled reads and 100 generation changes, retained caches are within configured bounds; post-warm-up memory has no approximately linear growth with completed requests/generations. Investigate noise with repeated cycles before declaring a leak.
- Ticket creation uses one transaction for the whole plan; transaction/commit count does not grow one-per-ticket. Reservation remains atomic. Cleanup never performs an unbounded delete on every plan.
- Required nested workloads satisfy the same bounded-resource/security gates. Publish separate throughput targets for Spark and each mask workload in W13's reviewed benchmark packet after collecting reproducible baselines; do not hide a failing nested/Spark case behind passing flat Python averages.

If a target fails: reproduce, profile, identify bottleneck, propose the smallest correction and add a focused regression. No profiler-driven removal of policy checks. A target can be renegotiated with evidence of user needs and a written decision; a security invariant cannot be weakened by calling it a performance target.

## 5. CI and deployment acceptance

Per PR: strict model/policy tests; identity/ticket security suite; native supported-type/snapshot/delete cases; Flight/Python/DuckDB/Spark streaming and nested-schema contracts; real PostgreSQL atomic reservations/publication activation; Ruff/format/ty/import boundaries; wheel install smoke. Cache dependencies, not mutable test databases.

Scheduled/manual: large memory/throughput/concurrency runs, real selected REST catalog/object store, key rotation/DB outage/kill-process scenarios and full image smoke. Before every pilot/release, run them on the candidate commit. Scheduled means a CI job configured by implementation, not permission for the planning agent to create a task automation now.

Required deploy checklist is evidence-oriented: no raw storage credentials for consumers; gateway cannot publish config; publisher cannot migrate schema; migration credential absent from running gateway; production TLS/auth enabled; unsupported config rejected; readiness tracks runtime fingerprint/generation; backup/restore tested; old tickets explicitly invalidated during V2 cutover; safe rollback documented.

Policy authoring and management UI is required. Add the [U09/U10 frontend, admin API, usability, accessibility, and packaging gates](../ui-v2/IMPLEMENTATION.md). Java/Spark remains required: retain Maven and distributed integration/retry gates plus cross-language protocol conformance. Removing unsupported backends must not reduce retained invariant coverage.

## 6. Product evaluation: four-week discovery window

Owner conducts/authorizes outreach. LLM prepares materials and summarizes supplied evidence; it must never invent interview quotes, adoption, willingness to pay or competitor limitations.

Week 1: shortlist 8–10 engineers with recent governed-data access problems. Interview at least four. Ask about the last concrete incident/workaround before demonstrating the project.

Interview prompts:

1. Who needed which data, from which storage/catalog, and what prevented access?
2. How is it solved today? Which engine, views, copies, exports, permissions or approvals are involved?
3. Why is an existing query engine/sharing tool insufficient? What does the workaround cost in time, infrastructure or operational risk?
4. Must consumers avoid raw storage credentials? Can that boundary actually be enforced in their environment?
5. What table features/types are used: nested schemas, deletes, evolution, sizes, files and concurrency?
6. Which identity/revocation requirements apply? Can short-lived offline-verified JWTs meet them?
7. Who would install/operate another service? Who approves it? What would block a trial?
8. Will they provide a synthetic/approved representative workflow and an engineer for a trial? What concrete next step and date?

Week 2: complete interviews; classify evidence as concrete problem, stated preference, deployment commitment or rejection. Prepare one short demo of the exact supported workflow. Collect representative nested data and DuckDB/Spark/other-framework workflows; validate actual schema/type/delete interoperability and installation friction instead of assuming Arrow alone makes every framework compatible.

Week 3: aim for two design partners agreeing to a workflow, environment, success criteria and evaluation time. Synthetic data only until G2 independent security gate. Compare setup and performance with their actual alternative when authorized.

Week 4: summarize evidence and decide whether further engineering is justified. If engineering has not reached G2, partners can evaluate synthetic behavior; no need to rush sensitive-data deployment to meet calendar.

Proposed success signals:

- Two teams commit meaningful evaluation resources, not merely express interest.
- At least one team repeats its workflow on separate days and explains what becomes easier/safer than its current approved approach.
- A platform engineer unfamiliar with the code can run synthetic demo in <=30 minutes excluding image downloads, then explain the credential/security boundary.
- Partner can name an operational owner and a path toward an approved deployment; measured costs/limits fit the workflow.
- Supported scope fits an actual problem. If every interested user immediately needs a server-side broad SQL engine or another storage backend, the proposed niche has not yet been validated.

Business-model questions are discovery, not release blockers for an open-source learning project: who benefits, who operates, whether support/hosting would have a buyer, and what ongoing maintenance budget exists. Record the owner's goal explicitly (career/learning, open-source adoption, or business) before judging success solely by revenue.

Stop/pivot signals: no two concrete partner commitments after the discovery window and reasonable outreach; existing approved tools solve interviewed problems sufficiently; deployment friction outweighs benefit; security/revocation requirements exceed what a small gateway can responsibly support; all demand is for features outside scope.

A failed four-week recruitment target is a signal for owner review, not proof that the market does not exist. Extend only with a named hypothesis and bounded next experiment; do not respond by adding features indefinitely.

## 7. Final decision record

Record: candidate commit; S01–S15 case results; supported/unsupported feature list; independent reviewer findings/status; exact benchmark environment/results; install/operation effort; partner commitments/use; unresolved limits; security revocation statement; recommendation (restricted pilot / hold / pivot).

Known alternatives from the prior review, useful for interview comparisons rather than unsupported market claims:

- [Trino OPA access control](https://trino.io/docs/current/security/opa-access-control.html)
- [Dremio row/column policies](https://docs.dremio.com/dremio-cloud/manage-govern/row-column-policies/)
- [GizmoSQL](https://github.com/gizmodata/gizmosql)
- [Delta Sharing](https://docs.delta.io/delta-sharing/)

Before relying on an alternative's specific current capability in a partner recommendation, verify its current primary documentation. Prior review sources are context, not a substitute for checking a concrete comparison.

If pivoting, propose one bounded experiment: policy-change/persona regression reports for one existing engine, or a small Arrow enforcement/conformance library. Do not start another platform rewrite without validating that problem.
