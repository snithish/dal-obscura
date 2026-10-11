# Project shortcomings and improvement plan

Audit date: 11 October 2026. Source baseline: `0c1d89f0bdfa4fecee4b1aa94319531e1da618b3`.

This is a proposed implementation and qualification plan, not a claim that the work is complete. The audit examined source, repository qualification reports, and archived UI captures. It did not rerun the full runtime suite, conduct a penetration test, or qualify live cloud accounts. Existing local changes to VS Code and the nested-MoR demo were excluded. The [visual companion](project-improvement-plan.html) summarizes priorities and dependencies.

## 1. Assessment and scope

The project already has a substantial correctness and security foundation: immutable configuration snapshots, signed single-use tickets, exact plugin admission, policy revision fences, explicit secret references, shared SQL/Arrow projection logic, and independently executable Iceberg/Delta tasks. The improvement program should preserve these contracts.

The main shortcomings are scale ceilings across several boundaries, incomplete end-to-end provider qualification, operational recovery gaps, and interfaces that become expensive or awkward as catalogs and schemas grow. There is no evidence from this audit of a demonstrated policy bypass. Security validation gaps below are investigations and release gates, not asserted vulnerabilities.

The requested outcome is a governed **read** platform. CoW/MoR support means correctly reading snapshots produced by upstream writers, including updates and deletes. Implementing a write/compaction service is a separate architectural scope and is not required by this plan.

Three requirements need precise contracts:

- **Every supported transactional format provides CoW and MoR reads on local and cloud storage.** A format does not graduate to supported status if either mode fails. Plain Parquet is a file format and has no transaction log or table-level merge semantics. Retain it as an explicitly labeled file source, or govern it through a transactional table format; do not advertise plain Parquet as MoR capable.
- **All catalog types are addressable through the catalog contract.** Build and qualify a named provider/version inventory across catalog families. An open adapter contract makes additional catalogs possible; it does not prove support for every present or future proprietary API. New catalog types remain backlog items until their adapters and qualification exist.
- **Field names, values, and nesting are not constrained by incidental application constants.** Preserve data exactly and make resource admission configurable and visible. Literally unlimited input cannot be promised with finite memory or dependencies that have their own limits. A dependency ceiling must be removed, worked around without semantic loss, or disclosed as an unmet support requirement—never silently truncated.

## 2. Evidence-backed findings

**Confirmed** means directly observed in source or explicitly documented. **Qualification gap** means the repository does not establish the required deployment guarantee. **Design proposal** means an improvement that requires implementation and validation. P0 blocks a broader support claim; it does not imply a critical vulnerability. P1 is the next delivery priority; P2 extends breadth after the foundation.

### E01 — Schema and path ceilings: confirmed, P0

[Typed paths](../src/dal_obscura/policy/paths.py) cap field names at 256 characters and paths at 64 segments. [Schema bounds](../src/dal_obscura/policy/schema_bounds.py) cap schemas at 10,000 nodes, depth 64, and 2 MiB of encoded schema. [HTTP models](../src/dal_obscura/interfaces/http/routes/schemas.py) introduce additional path, identifier, and field-list bounds. These directly conflict with the requested unrestricted field/depth contract. The schema bounds walker is already iterative; do not replace working iterative code simply because other traversal code is recursive.

### E02 — Independent SDK/wire limits: confirmed, P0

[SDK contracts](../packages/plugin-api/src/dal_obscura_plugin_api/contracts.py), [passive tasks](../packages/plugin-api/src/dal_obscura_plugin_api/tasks.py), and [storage identities](../packages/plugin-api/src/dal_obscura_plugin_api/storage.py) impose separate limits on configuration, namespace segments, metadata, JSON depth/string size, and URI length. These represent different domains: a catalog namespace segment is not an Arrow field, and a task-string ceiling is not a scalar-value ceiling. Raising only the policy constants would leave other failures and increase resource risk. Database field-ID widths also need review without confusing field identity with the field's displayed name.

### E03 — Schema rendering scales only at the DOM boundary: confirmed, P1

[SchemaSidebar](../apps/governance-ui/src/components/SchemaSidebar.tsx) virtualizes rows but first flattens the complete schema and searches it on each query. Indentation grows by 12 pixels per level. [schema_tree.ts](../apps/governance-ui/src/schema_tree.ts) recursively traverses children. [ColumnMultiSelect](../apps/governance-ui/src/components/ColumnMultiSelect.tsx) renders selected chips separately from the virtualized list and resolves their options repeatedly. These paths require changes before much larger/deeper schemas are supported.

### E04 — Discovery remains a complete-inventory operation: confirmed, P1

[Discovery](../src/dal_obscura/sources/discovery.py) collects all plugin pages into one list and stops beyond 10,000 tables; namespace and active-discovery budgets also apply. [Iceberg discovery](../src/dal_obscura/sources/iceberg_catalog.py) enumerates and sorts the provider inventory before slicing pages, caching that listing for the same context. It does not refetch the entire inventory on every page of that context, but first-page cost remains proportional to the full inventory. [ConnectionsView](../apps/governance-ui/src/components/ConnectionsView.tsx) renders all discovered table rows. UI pagination alone would not fix backend cost.

### E05 — Existing UI is useful; large-workspace usability remains unproven: qualification gap, P1

The archived [desktop workspace](ui-review/images/after-workspace.png) shows a clear schema reference, rule actions, and saved/live status. The archived [mobile workspace](ui-review/images/after-mobile-workspace.png) shows a tall fixed action area consuming a material part of the viewport. These are historical captures, not a fresh reproduction of current behavior. Validate mobile keyboard occlusion, zoom, long identifiers, and very large selection sets before making a current defect claim. Existing draft preservation, stale-response protection, and navigation guards must survive the redesign.

### E06 — Browser performance gate is incomplete: confirmed, P1

[Web-vitals measurement](../apps/governance-ui/scripts/measure-web-vitals.mjs) records LCP, CLS, and navigation duration; it enforces CLS but only checks that LCP exists. It has no user-interaction latency gate. There is no performance conclusion about a large schema from this script alone.

### E07 — Delta has material scale and feature limits: confirmed, P0

[Delta snapshot handling](../packages/delta-plugin/src/dal_obscura_delta/snapshot.py) limits files to 10,000 and log input to 64 MiB, and rejects column mapping. [Delta format execution](../packages/delta-plugin/src/dal_obscura_delta/format.py) materializes deletion-vector masks through the native API with a 16-million physical-row budget. These are explicit safe failures, but prevent a broad large-table support claim.

### E08 — Iceberg v3 and additional transactional formats are gaps: confirmed scope, P2

The [cloud-format contract](cloud-formats.md) qualifies Iceberg v1/v2 and documents v3 as outside that contract. The existing supported transactional paths are Iceberg and Delta. Hudi requires a new qualified adapter if included in the product's target format set. Do not treat the absence of every conceivable table format as a bug; publish a closed release inventory and its backlog.

### E09 — Transport support exceeds deployment evidence: qualification gap, P0

[Cloud-format documentation](cloud-formats.md) delegates local/S3/Azure/GCS and other transport behavior to official clients. Existing separate-process S3-emulator evidence is valuable, but does not establish live AWS/GCP/Azure IAM renewal, TLS, quotas, or multi-host performance. Workers require their own storage access; catalog-vended credentials are not supported as a delegated runtime flow.

### E10 — Memory limits are not a worker or cluster budget: confirmed, P0

[Runtime settings](../src/dal_obscura/interfaces/cli/settings.py) default to 16 active streams and a 512 MB DuckDB limit per connection, plus batch/ticket/deadline budgets. [Execution invariants](read-execution-invariants.md) acknowledge upstream native allocations, post-allocation retained-buffer checks, blocking provider calls, and metadata/delete buffering. Multiplying the configured DuckDB cap by admitted streams alone yields roughly 8 GB of potential DuckDB allowances before other allocations; this is arithmetic exposure, not measured RSS. Per-process semaphores do not establish deployment-wide fairness.

### E11 — Operational workflow is narrower than routine production deployment: confirmed, P1

The new [operator workflow](plugin-deployment.md) already installs packages, prepares admission, collects options/references, packages image and lock together, and checks replicas plus an authenticated read. It still requires an initialized Compose target and manual target JSON. Multi-catalog publication is not atomic; automatic rollback, safe ticket draining, and other orchestrators are not established by that workflow. Live image build/push/Compose qualification was unavailable in the preceding implementation session. Preserve the working commands; complete the surrounding lifecycle.

### E12 — Security boundary expansion needs proof: confirmed limitation and qualification gap, P0

[Secret loading](../src/dal_obscura/sources/secrets.py) currently admits the environment provider only. [Catalog option validation](../src/dal_obscura/control/catalog_service.py) validates URI hosts and unsafe IP literals; this function is not proof of DNS/redirect enforcement inside all provider clients. [Security documentation](security.md) describes trusted executable plugins and a single configured OIDC/JWKS provider. Provider-level egress, key rotation, token renewal, log redaction, and supply-chain provenance need explicit qualification as provider breadth increases. Plugin locks establish identity; they do not sandbox malicious admitted code.

### E13 — Distributed consumer recovery is incomplete: confirmed, P1

[Compatibility](compatibility.md) documents custom Flight rather than generic Flight SQL/JDBC/ADBC compatibility, single-use tickets, and unqualified Spark retry/speculation and executor credential renewal. Timezone semantics and several Arrow types are outside tested Spark support. Python materializing helpers and Polars conversion are intentionally not streaming. These limits must be visible during onboarding, not discovered during a failed production job.

## 3. UX and UI workstream

**Owner:** frontend/product, with control API support. **Priority:** P1. **Depends on:** the discovery and schema API contracts in sections 5 and 6.

### 3.1 Connection-to-first-read journey

Create a guided flow: choose catalog family → configure endpoint and secret references → check authentication/storage access → browse a namespace → select a table → inspect capabilities → govern → run a bounded verification read. Keep credentials in the secret provider. Display the difference between an admitted package, an enabled plugin, a configured catalog, and a healthy table through user-facing status labels and contextual help.

Use typed descriptor fields, defaults, descriptions, and validation consistently in CLI and UI. Report errors at the field or failing stage, with a safe correlation ID and next action. Preserve configuration after transient failure. Do not display internal entry-point groups or require hand-authored lock files. When discovery is unavailable, offer explicit table registration with a clear capability explanation.

### 3.2 Large asset inventories

Move namespace browsing, search, sorting, and pagination to the owning API. Preserve URL filters and cursor history on return from a table. Distinguish empty inventory, no search matches, permission denial, offline provider, partial discovery, and stale results. Provide a retry for the failed page without losing earlier selections. Bulk governance must offer a concrete selected-item review and per-item outcomes rather than making a series of hidden mutations.

### 3.3 Policy authoring and effect explanation

Keep the existing default-deny model and shared compiler. Present a concise effect summary: who is affected, fields revealed/masked, applicable row constraints, and overlap behavior. Keep preview inputs explicitly synthetic. Show which policy revision the preview evaluates and distinguish draft from saved policy. Review a readable change summary before publication; expose ticket revocation as a deliberate action without implying old tickets silently adopt new policy.

For wide selections, render a short summary plus a searchable selected-fields panel instead of thousands of chips. Select-all must state whether it applies to the current page, current search, current schema, or future matching fields; persist explicit semantics rather than inferring them from visible rows. Preserve ancestor-mask precedence and exclusion rules.

Use Immuta's separation of policy kinds as prior art for organizing policy tasks, not as a specification to copy. The proposed effect summary and progressive disclosure are project-specific design recommendations. [Immuta data-policy documentation](https://documentation.immuta.com/latest/governance/author-policies-for-data-access-control/authoring-policies-in-secure/data-policies).

### 3.4 Long names and deep structures

Show a readable leaf label with a bounded breadcrumb and an inspect/copy action for the exact full typed path. Never derive field identity from a truncated display label. Avoid indentation that grows indefinitely; let users focus a subtree and navigate ancestors. Fetch children/search results incrementally and preserve focused field identity when rows are virtualized or schema revisions change. Keep the schema reference collapsible and separate from editable selection state.

For a tree interaction, implement the complete keyboard and focus model, including visible focus, expansion, parent navigation, selection state, and virtualized level/position metadata. A disclosure list can remain a disclosure list if its interactions are simpler; adding ARIA roles without implementing their behavior is not an improvement. [W3C tree-view pattern](https://www.w3.org/WAI/ARIA/apg/patterns/treeview/).

### 3.5 Acceptance and design review

- Run five realistic journeys: first connection, first policy, deep-field masking, conflict recovery, and failed-deployment diagnosis. Proposed usability gate: at least 4 of 5 representative operators finish each core journey without developer help; zero secret-value entry into application configuration. Small-study results inform design, not a statistical population claim.
- Verify keyboard-only operation and screen-reader announcements, 200% zoom, 320/390/768/1440-pixel layouts, long Unicode labels, and light/dark themes. No primary action or active field may be hidden by fixed bars or the mobile keyboard.
- Preserve unsaved edits across view changes, retries, and revision conflicts. Sign-out must clear identity-scoped caches; test two different users in succession.
- Capture current baselines and before/after images in `docs/ui-review/`. Existing screenshots are reference material only. Reuse the current Mantine components and design tokens.
- On a documented browser/device fixture, target p95 local field-selection/search interactions below 200 ms, LCP below 2.5 seconds, and CLS at or below 0.1. These are proposed gates, not measured results. Report network search latency separately.

## 4. Security and operational ease workstream

**Owner:** security/platform plus identity/control maintainers. **Priority:** P0 for expanded trust boundaries; P1 for onboarding and recovery.

1. **Threat model by boundary.** Cover browser session, control API, catalog metadata, storage locations, admitted executable plugins, tickets, workers, build pipeline, and logs. Separate trusted operators/plugins from untrusted table metadata and consumers. Inventory existing controls before adding duplicates.
2. **Provider egress enforcement.** Qualify DNS rebinding, redirects, IPv4/IPv6 variants, link-local metadata endpoints, credential-bearing redirects, and catalog-returned URLs. Use provider-supported transport controls and deployment network policy for enforcement that option validation cannot supply. Preserve explicit internal-catalog and local-storage access. Tests must prove actual socket destination handling, not only string rejection.
3. **Secrets and identity lifecycle.** Add a reviewed provider interface for cloud secret managers/Vault or mounted secret stores where required. Scope resolution by catalog/identity owner, bound caching, and qualify rotation during reads. Keep values out of drafts, lock files, images, tickets, logs, browser state, and diagnostic exports. Preserve existing environment-provider support. Multiple identity providers or mTLS mapping should be separately scoped only when needed; do not add a user directory or SCIM collection.
4. **Supply-chain admission.** Extend immutable artifact locks with build provenance, SBOM, approved signing identities and verification policy. Build with restricted credentials/network scope. Display added/removed plugin IDs and exact package versions before admission. Explicitly admit transitive plugin providers and test tampering, downgrade, and mismatched SDK/descriptor/artifact cases. A lock is necessary but cannot establish that admitted code is safe.
5. **Low-friction CLI.** Add a target-initialization command or guided first-run configuration and a diagnostic command, with the exact command names chosen during implementation. Reuse `plugin add`, `plugin configure`, and `deploy`. Support human output plus stable machine-readable results, deterministic exit codes, offline validation where possible, and actionable missing-prerequisite messages.
6. **Release transaction and recovery.** Prepare the exact image/lock/configuration change set before approval. Introduce atomic publication of related catalog revisions using existing PostgreSQL transactions where possible; otherwise show explicit partial outcomes and resumable recovery. Separate schema migrations from routine deploy. Drain in-flight reads according to ticket lifetime, compatibility, and explicit revocation; a rollback must never revive revoked admission or invalid policy state.
7. **Health and recovery UX.** Show preparing, admitted, rolling out, validating, healthy, degraded, and recovery-needed stages. Verify all intended replicas, image/lock identity, control readiness, and a governed authenticated read. Add failure injection after push, during replica replacement, during catalog publication, and during smoke verification. Document recovery from each state and retain the last verified release.

**Acceptance:** negative tests at each trust boundary; live SSO logout/key rotation with a real identity provider; no secret leakage in retained artifacts; signed release verification; reproducible fresh-wheel qualification; an actual disposable registry and Compose deployment with injected failures. Other orchestrators require their own deployment adapters and gates. Provider-unavailable tests must fail visibly in the mandatory lane rather than silently skip.

## 5. Catalog coverage and discovery workstream

**Owner:** sources/plugin SDK and control API. **Priority:** P1, with additional provider adapters P2.

Preserve catalog → immutable handle → format → storage separation. Define a versioned capability inventory: hierarchical/flat namespaces, paged discovery, search, exact resolution, snapshots/references, credential delegation, supported output formats, permission behavior, and cleanup/cancellation. Capabilities must reflect provider behavior; a product-name badge is insufficient.

Catalog families and planned treatment:

- **Local/directory and in-memory registries:** deterministic exact registration, contained paths, symlink/path replacement tests, portable examples. Existing paths form the initial baseline.
- **SQL-backed catalogs:** qualify PostgreSQL first, then each explicitly supported backend/version. Verify concurrent metadata changes, transaction isolation, and credential renewal. A SQL catalog is metadata storage; it does not automatically mean querying arbitrary relational databases as tables.
- **Hive Metastore:** use the maintained client path; qualify namespace behavior, service authentication/TLS, and catalog-to-storage identity separation.
- **Cloud-native catalogs:** qualify Glue, DynamoDB, and BigQuery paths already delegated through PyIceberg, plus explicitly requested providers through adapters. Provider support does not imply that every native table format from that provider is readable.
- **Iceberg REST implementations:** qualify the standard contract plus vendor extensions separately, including authentication, references, and any vended credentials. Candidate vendor inventory includes Polaris, Nessie-compatible interfaces, and Gravitino where their chosen versions expose the required protocol; compatibility is a test outcome, not assumed from branding.
- **Unity Catalog/Delta-oriented catalogs:** retain existing registered `uc://` lookup; add real namespace/table discovery through a maintained public API where available. If provider listing is unavailable, explicit registration remains visibly limited support.
- **Custom/enterprise catalogs:** admit external SDK adapters with the same conformance tests. Hudi metadata services and other format-specific catalogs enter here or a dedicated reviewed adapter; no arbitrary Python loader paths.

Implement an end-to-end cursor API through plugin, service, HTTP, and UI. Bind opaque cursors to principal, catalog revision, namespace/filter, and expiry; reauthorize every page. Use stable ordering and define behavior during concurrent catalog mutation. For providers without pagination, expose bounded enumeration/cancellation and exact lookup; do not promise constant first-page cost. Avoid a new persistent metadata index until measured needs justify it and architectural approval is obtained.

**Acceptance:** each advertised provider/version passes real resolution/discovery/authentication tests; namespaces containing dots, slashes, Unicode, long names, and empty segments according to provider rules round-trip without collisions. Browse a synthetic 100,000-table inventory without fetching/rendering everything for the first page on paged providers. Test stale/tampered cursors, cross-user reuse, deletion between pages, access revocation, provider timeouts, and cleanup. Maintain a visible list of providers still unqualified.

## 6. Field length, nesting, and schema correctness workstream

**Owner:** policy/compiler, SDK, HTTP, storage, and frontend together. **Priority:** P0. This is a contract change, not a constant-only patch.

1. **Inventory separate dimensions:** field-name bytes/code points, catalog identifiers, field IDs, nested depth, schema width/encoded bytes, scalar string/binary lengths, list offsets, metadata size, task size, URL size, and database/index limits. Establish which boundary owns each limit and its error code.
2. **Introduce coherent resource admission:** publish configured byte/work budgets and negotiated capabilities through the API. Keep defensive defaults but remove unrelated tiny shape ceilings where safe. Budget before costly parsing/materialization where possible; separately account for allocations that upstream libraries cannot preflight.
3. **Use explicit stacks where application recursion is avoidable:** schema traversal, path conversion, HTTP/schema serialization, UI flattening, mask/projection construction, and validation. Keep the existing iterative walkers. Verify dependency recursion independently in isolated processes; a Python recursion-limit increase is not a fix for native stack risk.
4. **Preserve exact identity:** retain typed path segments and field IDs, especially literal dotted names, list/map tokens, rename/reuse cases, hidden equality keys, Unicode, and map keys. Display truncation must never mutate stored identity or policy behavior. Migrate database/API widths only where they actually constrain the affected identity.
5. **Handle large data values explicitly:** qualify Arrow large-string/binary/list offsets through native readers, DuckDB, Flight, Python, Java, and Spark. Keep full values when admitted; never silently truncate or stringify. A single value exceeding a batch/message budget needs an explicit unsupported/capacity error or a separately designed chunked-value protocol—not accidental row fragmentation that changes Arrow semantics.
6. **Make schema access incremental:** children-by-identity and search endpoints return versioned slices; UI caches are session/revision scoped. Full schema remains authoritative for compiler correctness, but authoring should not download and render every node merely to inspect one subtree.
7. **Change contracts together:** SDK, conformance kit, native SQL/Iceberg, manifest/Parquet, Delta, REST adapters, OpenAPI generation, clients, docs, and installed wheels. Make a deliberate API-version decision; do not add an internal second contract or compatibility shims by default.

**Acceptance:** property-based generation and differential checks cover exact names/values and metadata across schema/plan/fetch. Exercise names of 257, 1,024, and 16,384 characters; nesting of 65, 128, and 256 levels where dependencies permit; and widths of 10,001 and 100,000 nodes. Vary Unicode byte width, list/map structure, NULL/empty values, sliced arrays, and ancestor masks. These fixtures probe beyond today's caps; they do not prove infinite capacity. Record every dependency failure and fix it or mark the requirement unsatisfied for that consumer. Over-budget cases fail predictably without partial ticket issuance, truncation, unbounded allocation, or process crash.

## 7. Transactional formats across local and cloud storage

**Owner:** format/native-reader maintainers and provider qualification. **Priority:** P0 for current formats; P2 for new formats.

### 7.1 Iceberg

Preserve native v1/v2 snapshot and delete interpretation. Expand qualification for compaction/rewrite, position and equality deletes, NULL keys, multiple delete files, partition evolution, schema rename/reorder/drop/reuse, historical hidden equality fields, nested lists/maps, and concurrent writes after planning. Keep exact assigned-file/row-group execution on separate workers.

Add Iceberg v3 as a distinct feature milestone using maintained upstream support. The v3 specification adds capabilities such as deletion vectors; v2 correctness is not evidence for v3. Do not write a homegrown delete interpreter to fill an upstream gap. [Apache Iceberg specification](https://iceberg.apache.org/spec/).

### 7.2 Delta

Prioritize column-mapping support through stable field identity, checkpoint/log scale, and deletion-vector memory. Investigate a maintained per-file/range native API that avoids snapshot-wide mask materialization. If no suitable API exists, contribute upstream or keep the limitation explicit rather than removing the row guard. Qualify protocol reader features individually and reject unknown features before issuing any tickets.

Test log checkpoints, replacement rows, deletion-vector encodings, row-group boundaries, partition values, rename/reuse, compaction, vacuum/expired snapshots, and writes after planning. Keep deletes before policy filters/masks and compare exact projected values and metadata.

### 7.3 Hudi and file sources

For broader lakehouse coverage, add Hudi only through a maintained reader that can provide independent file-group/file-slice tasks and correct log merging. Qualify record keys, precombine/ordering semantics, updates/deletes, completed timeline instants, compaction, rollback, and schema evolution. If the available reader requires a new JVM/native boundary, present its deployment cost before approval; do not silently introduce a service. Hudi distinguishes COPY_ON_WRITE and MERGE_ON_READ table types. [Apache Hudi technical specification](https://hudi.apache.org/learn/tech-specs/).

Keep manifest/Parquet as a clearly identified immutable file-source capability unless it is wrapped by real transactional metadata. For a strict product requirement that every exposed source supports CoW/MoR, remove plain-file sources from that product surface or require conversion; there is no honest flag that adds merge semantics to raw files.

### 7.4 Required qualification matrix

For each supported transactional format and reader-feature version, run **CoW and MoR × local filesystem, AWS S3, Azure Blob/ADLS, and GCS**. Add HDFS/S3-compatible implementations and other transports only with separate evidence. Local multi-worker tests must use an explicitly shared read-only mount or assigned immutable artifacts with verified identity; equal path strings on different machines do not imply the same data.

For every cell require: real upstream-writer fixtures; pinned snapshots; exact independent expected values/schema/metadata; NULL/empty/deep collections; inserts/updates/deletes/compaction; at least two worker processes and a multi-host cloud lane; task-assignment access guards; no duplicate/missing rows; cancellation/early-close cleanup; expired credentials and renewal; provider errors and throttling; bounded allocation measurements. Shared metadata/delete reads are legitimate. Per-worker full-table rescans filtered afterward fail the gate.

Cloud emulators remain a fast lane. Live cloud tests establish provider identity, TLS, network, and throttling behavior. Report a cell as implemented, emulator-qualified, live-qualified, or unsupported; never collapse those states into one supported badge. No format graduates with only its CoW cells passing.

## 8. Scalability, performance, and consumer reliability

**Owner:** read/storage/platform and connector maintainers. **Priority:** P0 for resource safety; P1 for throughput/recovery.

- **Resource accounting:** measure planner/native/delete/Arrow/DuckDB allocations separately and together. Derive stream/task/provider admission from a worker budget with reserved headroom. Account for a single oversized batch or value. Use container/process memory ceilings as a final containment layer, not as graceful admission. Add spill only through explicit ownership, bounded disk space, cleanup, and confidentiality rules.
- **Distributed fairness:** benchmark concurrent principals and catalogs across replicas. Decide whether existing PostgreSQL reservation records can coordinate deployment-wide quotas without making the database a bottleneck. Keep caches bounded and workers stateless apart from current shared records. Any new queue, distributed cache, or coordinator requires a separate approved design.
- **Planning scale:** stream/group native planning output where supported; avoid materializing every file or deletion mask before useful work. Measure Iceberg Python/Rust stream-open and copy costs before optimizing. Add safe worker-local bounded caching/pooling only with immutable snapshot keys and deterministic cleanup.
- **IO reduction:** qualify partition/row-group statistics and safe pushdown through declared capabilities. Always retain the complete governance filter on original values; masks and hidden predicate/delete keys cannot leak. Compare bytes read and object requests as well as elapsed time. Metadata pruning must not cause missing rows under NULL or schema evolution.
- **Revocation and database cost:** measure per-batch revocation queries and ticket cleanup under concurrency. Any optimization must preserve explicit revocation and the promised check boundary; do not replace them with an unannounced stale cache or global policy-generation counter.
- **Cancellation and backpressure:** set native/client timeouts where supported, stop requesting new batches on cancellation, and bound work after the client stops. Document uninterruptible in-flight provider calls separately from the guarantee that no late batch is emitted. Test success, failure, cancellation, and consumer early close.
- **Consumer retries:** design authenticated attempt issuance for eligible single-use ticket retries, with clear replay, expiry, revocation, and snapshot rules. Test worker crash, lost response, executor credential expiry, and Spark speculation. Do not promise exactly-once delivery over an interrupted client connection without a defined acknowledgement protocol.
- **Usable client contracts:** publish streaming versus materializing APIs and supported Arrow types. Qualify distributed Spark, timezone semantics, large offsets, zero-column reads, and consumer-specific nesting limits. Generic Flight SQL/JDBC/ADBC support is an explicit additional protocol project if product priorities require it, not implied by the existing Flight transport.

**Proposed performance gates:** establish same-host baselines first. Cover 1/10/100 concurrent reads, 1/4/16 workers where infrastructure allows, 10³/10⁴/10⁵ files, multiple delete densities, narrow/wide/deep schemas, and warm/cold caches. Report planning p50/p95/p99, time-to-first-batch, throughput, peak RSS, CPU, object requests/bytes, open handles, and database load. Default regression gate: investigate >10% p95 latency or >15% peak-RSS deterioration on stable fixtures; tune thresholds from observed noise. Scaling success requires better aggregate throughput without duplicated IO or semantic changes; no fixed linear-speedup claim is made. Saturation must yield bounded queues or clear retryable admission errors.

## 9. Delivery sequence, dependencies, and estimates

Effort ranges below are planning estimates in engineer-weeks, not delivery promises. Provider provisioning and upstream library work can dominate elapsed time. Workstreams overlap, so adding all estimates does not produce a calendar schedule. Start with the current Iceberg/Delta contract; do not delay its usability improvements until every future catalog exists.

1. **Phase A — Baseline and contracts, 1–2 engineer-weeks.** Produce the capability inventory, limit inventory, current UI captures, threat model, benchmark fixtures, and live-cloud test account plan. Define exact support claims and the dependency ceiling policy. Exit: each claim has an owner, evidence lane, and reproducible command.
2. **Phase B — Usability and routine operation, 4–6 engineer-weeks.** Deliver target initialization/diagnostics, consistent descriptor forms, guided connection/read, clearer policy effects, mobile fixes proven by current captures, deployment recovery states, and cursor-based discovery for the first qualified provider. Depends on Phase A contracts. Exit: usability journeys and a real disposable Compose deployment pass.
3. **Phase C — Schema and resource foundations, 5–8 engineer-weeks.** Update field/path/schema limits coherently, remove unsafe application recursion, provide incremental schema APIs, adopt bounded selection UI, and establish worker-budget admission. Coordinate SDK/OpenAPI/consumer changes. Can overlap B after API shapes are settled. Exit: beyond-current-limit fixtures and cancellation/resource gates pass without breaking policy invariants.
4. **Phase D — Current-format cloud coverage, 5–9 engineer-weeks.** Remove or address Delta's column-mapping/DV bottlenecks, expand Iceberg/Delta semantic fixtures, and qualify all required local/AWS/Azure/GCS CoW/MoR cells. Depends on C for larger-schema claims and Phase A cloud access. Exit: published per-cell evidence, independent task coverage, and measured resource envelopes.
5. **Phase E — Catalog and consumer breadth, 4–8 engineer-weeks for an initial provider tranche.** Deliver remaining catalog-family adapters, credential delegation if required, provider-specific discovery, and distributed consumer retry/renewal. Per-provider estimates follow the inventory; “all catalogs” cannot be priced as a finite fixed list without that inventory. Exit: every advertised provider/version and consumer cell has a real qualification result.
6. **Phase F — New transactional formats/features, 6–12+ engineer-weeks provisional.** Spike Hudi task execution and Iceberg v3 upstream readiness, then estimate implementation separately. Depends on the SDK and qualification foundations; upstream contributions may extend this substantially. Exit: new formats satisfy the same CoW/MoR × storage matrix, with no degraded governance path.
7. **Phase G — Release qualification, 2–3 engineer-weeks per major release.** Run mandatory no-skips Python/consumer/PostgreSQL lanes, affected UI/live-SSO/JVM lanes, fresh installed-wheel admission with source paths disabled, migrations, signed artifact checks, live provider matrix, and rollback/failure drills. Rebuild wheels and lock from the exact final artifacts. Exit: published support matrix, operator runbook, before/after UI evidence, and benchmark report.

The critical dependency chain is **capability/limit contracts → coordinated schema/resource changes → broad format/provider qualification → support claims**. UX and deployment ergonomics can proceed earlier against stable APIs. Hudi and Iceberg v3 are separate milestones; they must not destabilize current supported reads.

## 10. Definition of done and unresolved decisions

The release is complete only when the requested experience and support contract are evidenced together:

- Operators can install, configure, review admission, deploy, verify, and recover through the supported CLI/UI journey without hand-building locks or exposing secrets.
- Large inventories and schemas remain usable with keyboard, screen reader, small screens, long names, and deep navigation; drafts and exact identities survive.
- Every advertised transactional format has passing local and live-cloud CoW/MoR cells, correct independent distributed work, and an explicit reader-feature version.
- Every advertised catalog/version has real discovery or an explicitly labeled exact-registration limitation, correct resolution, and tested authentication/storage access.
- Longer/deeper fields succeed end to end within the declared resource envelope. Any upstream hard ceiling remains an explicit unmet requirement, not a hidden truncation.
- Security, correctness, memory, cancellation, performance, and recovery gates pass on the exact release artifacts. Documentation distinguishes measured limits from configured budgets and implemented features from qualified support.

Resolve during Phase A: the named catalog/provider inventory; whether raw Parquet stays on the product surface; whether Hudi belongs in the first release; target cloud accounts/regions and storage identity model; acceptable dependency ceilings; supported orchestrators; and the workload envelope used for performance budgets. These decisions refine implementation sequencing. They do not prevent completing this plan, and no implementation approval is implied by this document.

Recommended first delivery: the capability/limit inventory, paged discovery, guided operator setup, and current UI baseline. Then change schema/resource contracts together before expanding support claims.

## 11. First implementation delivery — paged discovery

The initial atomic implementation addresses the shared-service and UI portions of E04, plus discovery-specific recovery and mobile usability from section 3. It deliberately changes the HTTP contract: discovery returns typed table rows, catalog revision, and an authenticated `next_cursor`. There is no complete-inventory response or legacy identifier fallback.

Implemented behavior:

- One plugin page per HTTP request, with a 1–500 entry limit. A virtual 100,000-table catalog test proves that the shared service constructs only the requested page and does not enumerate namespaces first.
- Encrypted, authenticated cursors bind caller, catalog identity/revision, and page size. They expire after ten minutes, reject tampering/scope changes, and work across replicas sharing the configured key. Every page reauthenticates the administrator.
- Immutable catalog capture and final revision/status lookup use separate short database transactions. Provider IO holds no database transaction. Governed-state lookup queries only returned identifiers, including friendly asset aliases.
- Diagnostics sample at most ten entries and report an unknown total when more pages exist. The browser replaces 50-row pages, retains the last page after failure, clears results when catalog configuration changes, exposes retry/refresh/empty states, and keeps actions visible at 320/390 pixels with long names.
- Generated OpenAPI and UI types, production key provisioning, deployment configuration, and [before/after review](ui-review/catalog-discovery/README.md) accompany the implementation.

Qualification includes the fast Python lane, focused rendered-browser paging/race tests, keyboard/mobile and automated accessibility checks, UI unit/types/build budgets, and fresh installed-wheel HTTP/plugin admission with repository production source paths disabled. Exact wheels and regenerated locks remain outside Git.

E04 is **partially resolved**: the built-in Iceberg client still enumerates its bounded inventory for each independent page. Browsing every page can repeat provider IO. This delivery does not claim native Iceberg pagination, catalog snapshot isolation across external mutations, or live-cloud qualification. Provider-native discovery/search, larger-schema contracts, worker budgets, operator initialization/recovery, additional formats/catalogs, and the remaining plan are still outstanding.


## 12. Second implementation delivery — exact long field identities

This atomic delivery addresses the field-name and path-shape portions of E01 and the text-index issue identified while implementing them. The schema compiler's depth/work envelope and the catalog/SDK dimensions of E02 remain separate work.

- Policy paths no longer impose a 256-character field-name or 64-segment shape ceiling. They validate non-empty printable names without trimming and account for canonical UTF-8 text, including JSON quoting, plus segment work against a 2 MiB budget. Only small paths enter the existing bounded parse cache. Quoted-path parsing reads offsets from the original string instead of copying every remaining suffix.
- Flight retains its 64 KiB descriptor budget and 1,024-column work limit while removing the independent 256-character column restriction. Typed paths must still match their human columns exactly.
- Schema admission preserves exact names and path segments, including whitespace, and charges each normalized field against an aggregate 2 MiB JSON encoding budget before replacing stored rows. HTTP and direct storage callers share the 5,000-field admission limit. Over-budget attempts preserve previous rows and revision.
- Stored fields are unique by asset and bounded field identity, rather than indexing displayed names. This avoids PostgreSQL's index-entry size restriction on long incompressible text and allows identical leaf names at different paths. Unique path and field-ID validation remains in the owning storage boundary.
- The sole database baseline changes to `20261011_0001`. Earlier databases are intentionally unsupported; initialize a fresh database and republish configuration. No destructive automatic reset or compatibility migration is provided.

Qualification covers 257-character ASCII, 1,024-character Unicode, and 16,384-character names through typed paths, HTTP admission, signed plan/fetch, exact output schemas, NULL-preserving masks, and hidden row-filter dependencies. Real local Parquet, native Iceberg, and Delta deletion-vector fixtures pass authenticated Flight/Python consumer reads. A disposable real PostgreSQL instance qualifies the packaged baseline and a 16,384-character incompressible name; SQLite alone is not the index evidence.

Isolated-process probes on PyArrow 23.0.1 round-trip Arrow IPC schemas at depths 64 and 65. Depths 128 and 256 fail with `OSError: Invalid flatbuffers message.` These are dependency probe results, not governed-read support claims. Default schema admission still enforces 64 levels, 10,000 nodes, and a 2 MiB Arrow encoding; the conformance kit, recursive projection/type construction, HTTP tree serialization, and consumer nesting limits remain to be coordinated. Changing the path representation does not make deep schemas readable.

E01 is **partially resolved**. Field names beyond the old cap now preserve identity; arbitrary nesting, larger schemas and values, configurable/visible resource budgets, incremental schema authoring, and live-cloud format qualification remain outstanding. This delivery does not expand the advertised catalog, format, storage, or consumer inventory.

## 13. Third implementation delivery — lossless schema metadata identity

Schema foundation review exposed a correctness and security defect: the canonical
shape decoded arbitrary Arrow metadata bytes using `backslashreplace`. A binary
byte such as `0xff` and the literal four-byte text `\xff` consequently produced
the same schema fingerprint and synthetic identity scope. Runtime admission could
accept that metadata substitution for schemas without provider field IDs.

The shared canonical shape now uses encoding version 2 and base64 for every
metadata key and value, including schema, field, and collection-child metadata.
Sorting the encoded pairs keeps insertion order irrelevant. Both authoring
fingerprints and runtime synthetic identity scopes consume this one shape; there
is no compatibility decoder or alternate identity implementation.

This is an intentional breaking identity change. Republish and explicitly
readmit schemas before using the new artifacts. Existing synthetic identities and
stored fingerprints cannot be reused. Provider IDs retain their existing meaning;
this change does not add metadata-based revocation for previously issued tickets
or replace the separate stable-provider admission contract.

Regression coverage distinguishes binary metadata from escape text at schema,
field, list-element, and map-value locations for both keys and values. It also
checks insertion-order stability and rejection through the runtime admission
boundary. Qualification uses focused source tests and rebuilt installed-wheel
schema/admission tests, followed by the required fast commit checks.

E01 remains partially resolved: this delivery fixes exact metadata identity,
not arbitrary nesting, native IPC ceilings, iterative projection, or UI schema
paging. CoW/MoR cloud coverage, broader catalogs, and the remaining plan continue
to require their own implementation and qualification.

## 14. Fourth implementation delivery — bounded canonical identity work

The canonical identity owner now uses one iterative version-2 JSON encoder and
incremental hash calculation for authoring fingerprints and runtime synthetic
identity scopes. It no longer constructs the complete recursive schema shape or
retains a complete canonical JSON string and UTF-8 byte copy. Long strings are
escaped in 4,096-character pieces; small scalar records are encoded together.

Both consumers enforce the existing schema node/depth/IPC limits before encoding,
and a separate two-MiB canonical encoding budget during hashing. Previously,
runtime scopes had no canonical JSON limit. Base64 metadata and ASCII-escaped
Unicode names can exceed that limit while fitting Arrow IPC, and now fail closed
with an explicit budget error. The control boundary translates the runtime scope
error into its existing validation response. No partial identity is returned.

Valid version-2 fingerprint and synthetic scope bytes are unchanged. Independent
expected JSON covers Unicode and escaping across chunk boundaries; behavioral
checks cover collection sizes, key ordering, child order/nullability, timestamps,
decimal scale, metadata identity, and runtime/control rejection. Earlier encoding
versions remain unsupported under the third delivery's readmission requirement.

Qualification: 262 installed-wheel schema, admission, read-service, and plugin
tests passed without skips in the selected local lane, plus the added control
budget case and three benchmark cases against each of the old and new artifacts.
Fresh environments and regenerated exact artifact locks remain outside Git.
The repository fast checks run at commit. This does not replace PostgreSQL,
real-provider, consumer, live-SSO, or live-cloud qualification for later changes.

[Same-host measurements](schema-identity-performance.md) show lower Python peak
allocation and higher latency. For 4,000 fields, allocation fell from about 2.6 MB
to 0.9 MB while median fingerprint latency rose from 7.680 to 10.692 ms. This is a
boundedness tradeoff, not a throughput claim or hard worker RSS guarantee.

E01 remains partially resolved. Canonical identity traversal is now iterative;
projection compilation, HTTP schema trees, UI schema browsing, provider/native
depth ceilings, wider schemas, and configurable coordinated work budgets still
need implementation. The advertised CoW/MoR storage and catalog matrix remains
subject to the plan's separate qualification gates.

## 15. Fifth implementation delivery — usable schema reference inspection

This UI delivery addresses the long/deep field browsing portion of E01. Schema
flattening uses an explicit stack, preserving node order, visibility, sibling
positions, and existing row virtualization. A synthetic 20,000-level chain no
longer exhausts JavaScript's call stack; this does not change source/native depth
admission or establish end-to-end support for that nesting depth.

Indentation is capped at 48 pixels so deeper fields keep readable labels on
mobile. Deep rows show leaf names while inspection retains the exact full human
path and name, including Unicode and literal dots. Native disclosure controls
expand/collapse containers. Keyboard-operable inspection buttons expose type,
nullability, full path, and name in a bounded, scrollable reference panel. The
panel is not policy selection; inspection leaves rule state unchanged and makes
no policy writes. Schema identity changes reset expansion/search/inspection state;
matching identities preserve the reference context.

The interaction remains a disclosure list rather than claiming the complete
[W3C tree-view model](https://www.w3.org/WAI/ARIA/apg/patterns/treeview/). A full
arrow-key navigator, subtree focus, provider-backed schema paging, and bounded
HTTP schema response construction remain open. The browser still receives and
indexes the entire admitted schema; virtualization only bounds rendered rows.

[Before/after review](ui-review/schema-reference/README.md) includes desktop and
mobile captures. Qualification covers 58 unit tests, rendered keyboard/exact
identity/layout/accessibility cases at 1440/390/320 pixels, existing expansion
focus and scroll checks, 10,000-field virtualization/search, component stories,
TypeScript, and production bundle budgets. A pre-existing component-story focus
assertion now waits for the modal's exit-transition focus restoration rather than
asserting immediately after Escape; no production picker behavior changed.

Core/SDK/API contracts are unchanged. No new PostgreSQL, installed service wheel,
live SSO, consumer, or CoW/MoR cloud support claims are made. E01 is still partially
resolved, and the remaining storage/catalog/resource/operator work retains its
separate acceptance gates.

## 16. Sixth implementation delivery — bounded schema response construction

The authoring response repeats ancestor names in typed paths, human paths, and
container type labels. Arrow IPC and canonical identity budgets alone do not
bound that expansion. An admitted schema with a 100,000-character ancestor and
100 children previously produced a response far larger than its source schema.

Schema discovery now accounts for compact UTF-8 JSON while constructing the
response, with an 8 MiB budget shared by the envelope and every nested node.
Array wrappers and separators count toward the same budget. Rejection happens
before retaining the rest of the tree and returns a validation error naming the
limit. No partial response or truncated field identity is returned. Exact long
Unicode, quoted names, and collection paths remain supported within the budget.

Behavioral tests reproduce the expansion and prove traversal stops before all
101 nodes are visited. Independent serialization through the HTTP response model
checks exact byte accounting, success at the boundary, and rejection one byte
below it. Existing schema identity, authorization, and provider error tests remain
applicable. This is a construction/output bound, not a hard RSS guarantee: Arrow
type rendering, individual JSON chunks, provider schema loading, and HTTP model
validation still allocate memory.

Qualification: 47 schema service and HTTP API tests passed without skips against
the rebuilt service wheel in a fresh 102-package environment, with repository
source paths disabled. The six-plugin lock was regenerated from the exact
installed artifacts; plugin artifacts and contracts are unchanged. Ruff and type
checks passed. Repository fast checks run at commit. No new provider, consumer,
PostgreSQL, native reader, or live-cloud qualification is claimed.

The current tree contract remains recursive and the 64-level native admission
ceiling remains. This interim fixed output budget intentionally rejects some
previously accepted authoring requests; governed reads and admitted schema
identities are unchanged. Provider-backed schema paging, configurable coordinated
budgets, deeper native support, and complete catalog/CoW/MoR cloud qualification
remain open. E01 is still only partially resolved.

## 17. Seventh implementation delivery — bounded selection review

Wide selections no longer render every field as a chip. The rule shows five
chips and a count-bearing review action; the review searches the selected set and
renders a bounded row window. The available-field search index, expanded leaf
sets, result eligibility, and selection lookups are memoized at their owning
component. Chip validity uses the existing path map instead of repeated scans.

Selection review is available on mobile through an explicit browse/review toggle.
Keyboard removal returns focus to review search; closing the dialog restores the
picker trigger. Unavailable selections remain removable. Removing a field changes
the draft without publishing it. Select-all copy now states that it applies to the
current schema regardless of search, and future/unselected fields remain NULL.
A modal-header landmark defect found by the full accessibility check is fixed.

[Matched visual evidence](ui-review/wide-selection/README.md) covers 1,000 selected
fields on desktop/mobile. Browser qualification exercises 10,000 selections at
1440/390/320 pixels with zero automated accessibility violations and p95 local
selected-field search below 200 ms. Existing bulk exclusions, prefixes, draft
behavior, and 10,000-field available search remain qualified. UI unit, story,
type, and production bundle checks accompany this delivery.

E03 is partially resolved: DOM selection work and repeated lookup/derived work
are bounded or indexed, while source schema transfer, full option indexes,
provider-backed schema search/paging, and other policy summaries remain open.
These synthetic UI fixtures do not change backend schema/policy/Flight limits or
prove provider qualification. E05 still requires representative-operator studies
and live device/keyboard evidence. Remaining release gates are unchanged.

## 18. Eighth implementation delivery — atomic catalog publication

Related catalog changes now publish through one authenticated, bounded
`POST /v1/catalogs/publish` request. Every changed instance supplies an explicit
revision fence; the server reuses admitted-plugin, egress, option, and audit
validation inside one transaction. Sorted catalog names establish deterministic
row-lock order. Success commits all changes and audit records; a late validation,
admission, or revision failure rolls them all back.

The deployment CLI retains its conflict preflight and unchanged-configuration
retry behavior, then sends one change set instead of individual PUTs. A release
may contain at most 100 catalog drafts, checked before container preparation; the
API also enforces batch shape and the existing HTTP byte budget. There is no
fallback to older servers or partial-batch splitting. Generated OpenAPI and
browser contract types include the new endpoint. Existing individual catalog
editing remains available.

Qualification: 113 installed-wheel catalog, operator, API-contract, and schema
tests passed with no skips in a fresh environment. Nine PostgreSQL cases use
disposable schemas, including proof that another reader sees the old complete
inventory while publication pauses after its first write, then sees both changed
revisions after commit. SQLite and PostgreSQL independently qualify rollback of
catalogs and audits on late failures. Exact plugin locks were regenerated from
the installed artifacts; plugin contracts are unchanged. UI types, generated
contract checks, and unit tests pass; repository fast checks run at commit.

The catalog-publication portion of E11 is resolved. Image replacement, database
schema installation, catalog publication, and smoke verification remain separate
stages. This delivery does not implement automatic rollback, ticket draining,
target initialization, provenance signing, another orchestrator, or a live image
push/rollout. Live infrastructure and provider qualification remain open.

## 19. Ninth implementation delivery — guided deployment target and diagnostics

`dal-obscura target init` collects first-run target settings, validates immutable
image references, repository/endpoint shape, referenced files, and independent
synthetic smoke values, then writes a mode-0600 target without replacing existing
state. Required fields can be supplied with `--non-interactive`; shared labels
serve both prompts and flag help. Invalid or cancelled input leaves no target.
Columns use a JSON array, retaining exact path strings. Credential values are
never requested or copied: target settings retain environment names and TLS file
references. Credential-bearing Flight URIs are now rejected before connecting.

`dal-obscura doctor` provides human output or a versioned JSON report. Offline
checks validate files/references and credential availability without external IO.
Online checks use read-only container/Compose operations and authenticated control
inventory. A failed check returns nonzero with an actionable, sanitized message;
subprocess stderr, service bodies, and credential values stay out of the report.
Offline readiness is not authentication evidence. Replica image/lock/readiness and
exact Flight values remain owned by deploy verification.

Qualification includes 35 installed-wheel target/operator tests, console help,
and three real local authenticated Flight smoke cases (success, wrong values,
invalid token). The service was rebuilt in a fresh environment, selected plugin
locks regenerated from installed artifacts, and Ruff/type/fast checks applied.
Existing target configurations remain readable; earlier malformed or
credential-bearing image/endpoint references fail closed. Plugin APIs and schema
identities are unchanged.

The target-initialization/diagnostic portion of E11 is resolved. This release still
uses an existing initialized Compose topology. Provisioning, deployment-stage
recovery/rollback, ticket draining, additional orchestrators, live registry rollout,
live IAM/SSO, and broader provider/catalog/format qualification remain outstanding.

## 20. Tenth implementation delivery — enforce navigation performance budgets

The standalone web-vitals command now rejects LCP above 2,500 ms and CLS above
0.1, as well as missing, non-finite, negative, or nonnumeric measurements. It
prints the measured URL, viewport, limits, and metrics before failing so a failed
run retains reviewable evidence. Navigation duration remains informational.
The LCP threshold follows the [documented good-LCP boundary](https://web.dev/articles/lcp);
this local gate does not establish the field 75th percentile.

Twelve owning command-gate cases first exposed eight failures, then passed after
implementation. All 70 UI unit cases and TypeScript checks pass. A real Chromium
navigation against an isolated production preview reported LCP 120 ms and CLS
0.000953 on this host. That fresh browser rendered the signed-out shell; the
existing authenticated E2E fixtures and selected-column search p95 gate remain
separate evidence. No live workload, field INP, or aggregate capacity claim is
made. E06 remains partially resolved pending representative workload and field
qualification.

## 21. Eleventh implementation delivery — indexed mask edits

Mask application and grant removal now use one typed-path coverage index per edit
instead of rebuilding the entire schema lookup for every comparison. Strict typed
ancestors retain literal dotted names, collection tokens, and historical field
identity. Another surviving grant still preserves a covered mask; unrelated masks
and rule metadata remain unchanged. Schema option traversal now uses an explicit
stack, and unrelated editor changes retain the option array.

A failing synthetic deep traversal test first exposed JavaScript stack overflow.
All 72 UI unit tests, 58 rendered stories, 101 E2E cases, TypeScript, and unchanged
bundle gates pass after implementation. A reproducible same-host 300-grant probe
measured mask apply 560.082 → 0.278 ms and removal 1,145.828 → 0.497 ms; exact
values and selected columns are checked independently of the timed interval.
See [measurement and limits](mask-edit-performance.md). Synthetic deep traversal
does not remove the native Arrow depth limit. Full-schema transfer, prefix work,
mask-summary rendering, provider coverage, and the remaining plan stay open.

## 22. Twelfth implementation delivery — bounded operator control responses

Deployment control requests now read at most 8 MiB plus a rejection sentinel,
without trusting Content-Length, and close responses on success or failure. The
maintained parser rejects malformed UTF-8/JSON and excessive nesting; an iterative
check also rejects numeric exponent overflow. Errors omit response bodies. The
parser's nesting bound remains an upstream constraint, and decoded-object memory
can exceed transport byte size; this is not an RSS or unlimited-input claim.

Nine owning transport cases first exposed eight failures. After implementation,
47 installed-wheel operator/target/transport cases pass without skips, including
three real loopback HTTP cases: authenticated inventory, an oversized body without
Content-Length, and rejection before a redirect credential destination is reached.
The service wheel was rebuilt in a fresh source-disabled environment, six unchanged
plugin artifacts were locked and admitted, and Ruff/type/fast checks apply. Live
TLS/proxy/identity renewal and cloud-provider qualification remain separate gates.

## 23. Thirteenth implementation delivery — responsive wide mask forms

Mask forms retain derived groups, allowed-field maps, and membership sets across
local typing, avoiding repeated cross-field scans. Exemptions, map-key restrictions,
mask changes, pending-edit guards, and exact target ownership stay at the same
boundaries. A 5,000-field browser fixture checks local redaction and no premature
HTTP writes. Three comparable single-worker runs measured p95 replacement typing
137.900–147.415 → 28.699–29.085 ms, within the unchanged 200 ms gate. See
[measurement scope](mask-edit-performance.md#replacement-typing).

All 72 unit tests, 58 browser stories, TypeScript, relevant policy-authoring E2E
cases, and unchanged bundle gates pass. Mask-summary DOM, full-schema transfer,
provider-native paging, and representative live operator qualification remain open.

## 24. Fourteenth implementation delivery — scoped mounted secret references

Startup configuration selects a fixed `environment` or `mounted_file` provider
with `DAL_OBSCURA_SECRET_PROVIDER_KIND`, removing module-path mechanics. The
mounted provider resolves existing scoped references from an operator-owned fixed
POSIX directory. It reads nonempty UTF-8 regular files within a 64 KiB byte budget,
preserves exact whitespace, and observes atomic file replacements without caching
values. Absolute directory identity is pinned; path traversal, symbolic/hard links,
nonregular files, oversized data, invalid text, and replaced roots fail closed.
Both descriptors close on success, rejection, and IO failure; short reads retain
complete values. No service-managed persistent state is added.

This intentionally breaks startup configuration: remove
`DAL_OBSCURA_SECRET_PROVIDER_MODULE`, including an empty value, and select the
provider kind. The retired `DAL_OBSCURA_SECRET_PROVIDER_SECRETS` is still rejected.
Bundled Compose and example environment files now use the same selector and no
longer inject that retired inline setting. No arbitrary provider import is added.
See [configuration, rotation and deployment limits](security.md#secret-handling).

TDD exposed fourteen failures in the initial mounted-provider boundary cases.
The final installed wheel passes 163 provider/runtime/CLI/HTTP/schema cases with
zero failures or skips using disposable PostgreSQL; public catalog responses retain
references while discovery receives the rotated value. Exact unchanged plugin
artifacts were locked and admitted in the fresh environment. Ruff/types pass;
repository fast checks apply at commit. A read-only Compose validation attempt was
blocked by the unavailable Podman engine; no demo or engine state was changed.

E12 is partially resolved. This is local POSIX file-provider evidence, not container
mount/rotation, Vault/cloud-secret-manager, IAM/SSO or live TLS qualification.
Projected symlink secret volumes are rejected. Existing identity configurations
and cached/opened provider sessions can retain resolved values and need their own
reload/refresh lifecycle. Egress enforcement, provenance, field/native depth,
provider breadth, and the rest of the plan remain open.
