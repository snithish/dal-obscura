# Governed Iceberg gateway: implementation and validation plan

> **Historical plan, superseded by the live-configuration decision.** Its policy
> drafts, review/publish flow, workspace bundle, operator manifest, and
> publication-generation requirements are retired. Current behavior and reset
> requirements are documented in the [decision](../decisions/2026-09-live-configuration.md)
> and [operator guides](../operators.md).

Status: planning only. No implementation is authorized by this document alone to skip the test-review gate. Prepared 2026-09-09 and revised to reflect the owner's correction: full nested-schema/governance capabilities in the pilot, focused on one Iceberg backend and consumers including DuckDB, Spark and other Arrow frameworks. Do not revive the superseded primitive-only, two-mask or Python-only scope.

Read in order:

1. This document: scope, decisions, architecture and gates.
2. [WORK_PACKAGES.md](WORK_PACKAGES.md): ordered, bounded implementation assignments.
3. [EVALUATION.md](EVALUATION.md): independent correctness, security, performance and product evaluation.
4. [AGENT_HANDOFF.md](AGENT_HANDOFF.md): execution instructions and reusable assignment prompt.
5. [NESTED_AND_CONSUMER_CONTRACT.md](NESTED_AND_CONSUMER_CONTRACT.md): normative nested authorization/masking, framework compatibility and distributed retry contract. Read alongside the master scope before implementing W02/W03/W06/W08.
6. [discovery-packet.md](discovery-packet.md): owner-approved design-partner interview and W15 decision record template.

These documents are the target specification, not a claim about current behavior. Proposed names, commands, types and paths must be created by their assigned packages. Existing commands are explicitly labeled. Resolve any contradiction before implementation; security guarantees take priority over convenience and benchmark targets.

## 1. Outcome and investment rule

Build a focused, self-hosted gateway that returns policy-enforced Arrow batches from Iceberg to DuckDB, Spark and other Arrow-capable frameworks without giving consumers storage credentials. One storage backend; a framework-neutral governed read contract with full nested data and masking capabilities.

Success has two independent gates:

- Engineering: exact authorized results, no known enforcement bypass, bounded resources, explicit failure behavior and a reproducible deployment.
- Product: at least two relevant teams commit time/data/workflows to evaluation, and at least one repeatedly uses the gateway for a concrete unmet need.

Passing tests does not prove demand. Customer interest does not waive a security gate. Run interviews during engineering. Do not treat the four-week customer-discovery window as a promise to finish all engineering in four weeks.

Freeze unrelated expansion. Complete nested governance, existing mask capabilities and DuckDB/Spark/framework integration as required pilot scope. Policy authoring and management UI is required by explicit owner direction. Follow the fresh [UI/UX plan](../ui-v2/README.md); its U00–U10 packages supersede earlier UI-removal recommendations. Do not add another storage backend, identity mechanism, or server-side query engine for speculative demand.

## 2. Fixed first-release scope

### Retain and harden

- Read-only-data Arrow Flight operations: authorized schema, plan, authenticated partition-attempt issuance, fetch and health/readiness. Attempt issuance writes bounded ticket state, never source data.
- Iceberg tables registered by an operator under a logical asset name.
- Framework-neutral versioned Arrow Flight read contract, Python batch/table client, streaming DuckDB relation adapter, retained Java client and Spark datasource. Required pilot integrations: DuckDB, Spark, and a tested PyArrow interoperability example with another framework (Polars by default).
- Strict allow-only policy model with column grants, row restrictions and masks; SQL rendered/executed in DuckDB through an expression allowlist.
- One production identity mechanism: OIDC JWT validation with configured issuer, audience, allowed algorithms, expiry, subject and explicit claim mappings. TLS required outside explicit local development mode.
- One configured tenant/workspace per deployment for the pilot. Keep namespace identifiers in storage/tickets; do not infer a tenant from arbitrary token claims or silently default missing context. Multi-tenant self-service is out of scope.
- PostgreSQL for shared deployments and atomic ticket exchange. SQLite for local demonstration and ordinary isolated tests. Services remain horizontally restartable; durable state stays in the DB. No in-process authoritative ticket store.
- A complete policy authoring and management UI with a separately authorized administrative API, plus an operator CLI for automation. Both use shared validation, preview, publication, and management application services. Follow the new UI plan rather than copying previous screens.
- Explicit migration CLI, structured audit events, limits, cancellation and deployment runbook.

### Full governed-read support, with verified compatibility

Iceberg is the sole table backend. Required read behavior includes pinned snapshots, schema/partition evolution, nested types, positional and equality deletes, correct projection/filtering and parallel task planning. W07 must inventory Iceberg v2/v3 features currently advertised by this repo and validate actual dependency support, including v3-specific read features before advertising them. A version-number check does not prove support. Parquet is the reference physical encoding; inventory other Iceberg encodings separately from adding a new table backend. Missing required capabilities are tracked implementation blockers, not silently removed from scope. Unknown features fail closed until implemented; any proposed release exception requires an explicit owner decision. Do not invent native library APIs.

Required schema contract includes primitive fields, nested structs, lists, maps and their combinations, null containers/elements, decimals and temporal/binary values. Preserve Iceberg field IDs, collection shape and authorized Arrow schema across every consumer. Use typed field-path segments and field IDs internally; distinguish literal dotted names from nested paths. See the nested contract for exact projection and mask rules. A schema change requires explicit policy/schema revalidation, not silent wildcard expansion.

Ancestor-mask bypass must be fixed while preserving legitimate nested reads. Blanket rejection of nested schemas does not satisfy the pilot. Invalid combinations such as requesting a child beneath a parent replaced by a scalar mask reject specifically; ordinary authorized nested projection must work.

Retain and harden all existing mask capabilities: `null`, `redact`, `hash`, `email`, `keep_last` and `default`, including eligible nested leaves. Strictly validate each mask's type applicability, parameters, null behavior and resulting schema. No generic numeric “strictest” ranking for incomparable masks. Hashing is deterministic pseudonymization/equality disclosure, not an anonymization guarantee. The nested contract defines conflict resolution and compound-value masking rules.

Policy composition is explicitly restrictive: matching grants union permitted field paths; row restrictions AND together; comparable masks combine deterministically, and incomparable conflicting masks reject publication. Apply validation across potentially overlapping rules conservatively. No deny-rule syntax; omission means no grant. Tests must show that adding a restrictive matching rule can remove rows. Do not describe this as ordinary union-of-row-grants semantics. Parent/leaf grants and masking follow the canonical field tree in the nested contract.

### Remove from the active product

- Delta, standalone Parquet/CSV/JSON/ORC/Avro/text assets, Unity Catalog adapter.
- Obsolete frontend implementations only after the replacement authoring/management experience passes its acceptance gates. Preserve administrative API capabilities and durable authoring records; redesign their contracts where required by the new UI plan.
- Composite auth, trusted headers, API key auth, shared-secret JWT production paths and identity-based mTLS mapping. Keep TLS server transport and relevant TLS validation; use a local OIDC issuer for demos.
- Dynamic Python module loading for catalogs/auth/secrets. Replace with fixed built-ins and explicit secret references to approved environment/file sources.
- Obsolete aliases, empty test relocation modules, inactive examples, unused dependencies and docs that advertise removed behavior.

Preserve useful code in Git history, not dormant importable modules. Keep tests proving removed options fail. Delete a feature only in W10 after its replacement entry path exists and the deletion inventory is reviewed. Never remove evidence of a security flaw before a replacement test demonstrates rejection/enforcement.

### Out of scope

Server-side general SQL/joins/aggregates/writes, arbitrary UDFs, SaaS tenancy, configurable policy approval automation and a general plugin system. The management UI, ownership, operator capabilities, and explicit publication review are required. Consumers may run their own SQL/joins/aggregates on governed data. Flight SQL/ADBC/JDBC are separately evaluated compatibility options. Distributed task retries and speculation are required for Spark; transparent byte-offset resume of a partially consumed stream is not.

## 3. Security contract

The following identifiers are release-blocking invariants. Tests in EVALUATION.md map to them.

- S01: every emitted value derives from the configured asset, pinned Iceberg snapshot and fields authorized for the authenticated principal.
- S02: policy row restrictions and masks are enforced before any Arrow batch reaches the transport/client, regardless of backend pushdown behavior.
- S03: invalid/unknown policy, malformed ticket/config, unknown schema or unsupported data feature causes rejection. Invalid protection is never interpreted as no protection.
- S04: client filters may reference only explicitly authorized, unmasked field paths; ancestor/descendant and collection-path overlap checks prevent masked-data inference. Internal policy-filter dependencies never become visible fields. Unauthorized explicit leaf requests reject; parent projection is pruned to authorized descendants using the documented schema contract. Wildcard exposes only authorized paths. Empty wire projections reject; Spark count/zero-column reads use the connector contract.
- S05: principal identity is issuer + subject + configured deployment namespace. Tickets bind identity, relevant groups/attributes, policy/config generation, asset binding and snapshot.
- S06: ticket lifetime never exceeds the authenticating token's expiry. Expiry is `now >= expires_at`; one injectable UTC clock policy is used across codec/store/use cases. No defaulting malformed expiry to zero or coercing booleans to timestamps.
- S07: a removed group or changed mapped authorization claim in a newly validated token invalidates the old ticket. Reauthorize on fetch and compare the effective decision as well as a canonical context fingerprint. Unchanged refreshed tokens may work if all bound context is unchanged and expiry permits.
- S08: newly initiated fetches require the current active publication generation. Any generation change invalidates all older tickets in the pilot, even for an unrelated asset. This conservative rule is intentional.
- S09: running streams periodically verify active generation and token expiry before handing further batches to Flight. Verification age must not exceed one second at emission; a stale check forces a bounded refresh. On DB failure, expiry or revision change, terminate without handing over another batch. Already handed-over/network-buffered data cannot be recalled. No claim of instantaneous revocation.
- S10: one attempt ticket has one atomic successful exchange reservation, across replicas and races. Reservation remains consumed on later execution failure. A framework retry obtains a new authenticated, bounded attempt ticket for the same immutable logical plan/partition, with current authorization/generation checks. No refund, stale-plan resurrection or implicit fresh-snapshot replan. Framework attempt isolation, not ticket replay, prevents duplicate committed results.
- S11: ticket/database serialization contains data, never pickled executable objects, credentials, Python class names or import paths. Legacy ticket versions reject without a pickle fallback.
- S12: unauthenticated/unauthorized requests cannot trigger arbitrary URL/path resolution. Only operator-registered logical assets resolve. Validate metadata, manifest, data and delete-file locations against the asset's allowed storage roots/endpoint policy.
- S13: secrets, bearer tokens, tickets, source rows and raw policy literals never appear in ordinary logs/errors/metrics. Errors distinguish safe user mistakes from internal failures without leaking physical paths or SQL/data details.
- S14: query, scan, serialization and transport resources have explicit limits and deterministic cleanup. A client disconnect/deadline must reach query cancellation and reader closure.
- S15: a request uses one immutable publication snapshot for binding, policy, schema validation and ticket issuance; activation uses compare-and-swap and cannot produce a mixed configuration.

Trust model: readers and their input are untrusted; operator publication credentials, runtime hosts, the configured identity issuer, catalog/storage integrity and DB administration are trusted. Enforce separate publisher and gateway DB privileges. A malicious DB administrator or runtime host can subvert authorization; a payload MAC is defense in depth, not a solution to total DB/host compromise. Consumers must have no separate object-store credentials that bypass the gateway. Hiding row values does not establish formal timing, error, cardinality or statistical noninterference; no differential-privacy/anonymization claims.

Offline JWT verification cannot observe a role change inside an already-issued still-valid JWT. Pilot uses short-lived tokens (target five minutes or less), validates new claims on each request, enforces expiry during streams and documents this revocation bound. If a design partner needs immediate identity-provider revocation, stop and design online introspection/revocation explicitly; do not pretend generation checks solve it.

## 4. Target architecture and interfaces

Use existing `common`, `data_plane`, `control_plane` namespaces during implementation to reduce moving parts. Rename the retained minimal control-plane package only if there is a concrete packaging benefit after functional gates pass. No full tree rewrite as a first task.

Dependency direction: domain/contracts -> nothing transport-specific; application -> domain + narrow ports; infrastructure -> application ports; interfaces -> application. Arrow types may appear in data contracts. Flight/FastAPI/SQLAlchemy engine/session types must not enter use-case signatures. Operator code and gateway code share strict publication models, not a giant mutable repository.

Choose use-case classes (`GetSchemaUseCase`, `PlanAccessUseCase`, `FetchStreamUseCase`) as the single orchestration API. Remove duplicate free-function wrappers, Flight adapters, dummy catalog dependencies and the all-purpose AccessFlow once callers migrate. Share a read-authorization component for schema/planning; retain separate result objects with precise responsibilities.

Required conceptual interfaces (names can be adjusted once in W02; behavior cannot):

- `IdentityProvider.authenticate` -> validated identity including issuer, subject, expiry, normalized groups and explicitly mapped attributes.
- `PublicationReader.load_active` -> deeply immutable `PublicationSnapshot`; `current_generation` -> authoritative generation for fetch/emission checks.
- `PolicyEvaluator.evaluate` -> typed `ReadDecision` over the requested schema/columns and normalized identity; no IO.
- `AssetResolver.resolve(snapshot, asset_id)` -> one physical Iceberg binding plus allowlisted storage configuration; no client target fallback.
- `IcebergPlanner.plan(binding, requested_snapshot, projection, restrictions, limits)` -> versioned `ScanPlanV2` and exact authorized output schema.
- `TicketStore.create_many` -> atomic bulk creation; `reserve_once` -> bounded atomic reservation; `cleanup_expired(limit)` -> bounded maintenance.
- `AttemptIssuer.issue(plan_reference, partition_id, attempt_id, identity)` -> authenticated retry/speculation issuance tied to the original immutable scan plan and current generation, with per-plan/partition bounds and idempotency rules.
- `RowTransformer.open` -> context-managed batch iterator/reader with explicit cancel/close; final enforcement always present for governed reads.
- `StreamGuard` -> admission, deadline, generation/expiry freshness and close/cancel ownership.

PublicationSnapshot contains a generation UUID, full-content digest, schema version, fixed namespace/identity config, registered assets, policy definitions, storage root references and execution settings. Values are immutable, not merely a frozen dataclass containing mutable dictionaries. Cache at most current and previous generations plus in-flight leases; retired generations are evicted when no lease uses them. Global admission bounds the number of leased generations/requests. Do not hold one lock across all DB IO.

Operator CLI accepts a strict versioned manifest, validates schema and policy against each registered snapshot, previews synthetic personas, and activates one generation transactionally with an expected-current-generation argument. No arbitrary module strings. Runtime auth/TLS/secret-provider wiring is startup-bound and included in a runtime fingerprint. A publication that requires a different runtime fingerprint makes old replicas unready and prevents new plans/fetches; documented restart/rollout required. Live-reload only assets/policies/compatible query limits through immutable generations. Do not retain the current ambiguous mix of refreshed policy and stale auth settings.

ScanPlanV2 must contain only strictly validated data:

- version, plan ID, asset binding digest, generation/digest, pinned metadata location and Iceberg snapshot ID, schema fingerprint, namespace, projection, normalized approved SQL restrictions/masks;
- task IDs and explicit file/delete-file scan specifications with necessary Iceberg field-ID/schema/partition semantics, or validated deterministic references sufficient to reconstruct those native tasks at the same immutable snapshot;
- no open readers, credentials, arbitrary Python objects, class paths or client-controlled physical locations.

W07 includes a bounded feasibility investigation of native PyIceberg reconstruction. The preferred codec serializes explicit native scan-task data and recreates native tasks with the pinned dependency API. A permitted simpler fallback is deterministic replanning of the pinned snapshot followed by exact task matching; measure metadata cost, bound it, and document it. Never substitute raw Parquet scanning if it loses Iceberg deletes/schema/field-ID semantics. If neither path is correct, stop for expert review.

TicketEnvelopeV2 is small: version, key ID, ticket ID, expiry, nonce and MAC over a canonical reference that binds the stored payload digest and logical plan/partition/attempt identity. Store a separate authenticated digest/MAC for canonical payload data using a purpose-separated key. Check size/version/signature before DB lookup, and stored integrity before constructing any scan objects. Use standard HMAC primitives, secure key generation, constant-time comparison, bounded key rotation and strict parsing; do not invent cryptography. Secret references resolve at execution, outside serialized plans. Require canonical JSON values and reject duplicate keys, NaN/infinity and unknown fields. Logical-plan lifetime and per-attempt ticket lifetime are separate; the nested/consumer contract defines refresh/retry semantics without extending an issued ticket.

Flight protocol changes need a new explicit version. Old clients/tickets receive a documented incompatibility error. Do not spend effort supporting old unsafe payloads. Migration preserves existing durable authoring data for export and explicitly invalidates old tickets; deployment runbook explains expected in-flight failures.

## 5. Storage and deployment decisions

Retain the DB rather than redesigning replay protection as self-contained tickets. Split publisher write privileges, gateway config-read/ticket-exchange privileges and migration privileges. Shared production deployment requires PostgreSQL; local demo uses persistent SQLite with clear single-process limitations. Do not claim SQLite cannot survive restarts.

Initially support the existing local SQL Iceberg catalog for synthetic development and one documented production REST catalog/object-store configuration. Fixed operator registration supplies the physical identifier; no dynamic catalog plugins. W07 proves the selected deployed catalog works with pinned dependencies before promising compatibility. Additional catalog products require partner demand.

Do not drop old tables or run destructive migrations automatically. Export/backup, apply forward migrations, verify content and cut over. Retire old schema tables only in a later explicitly reviewed migration after retention needs are known. A smaller runtime does not require erasing existing records.

## 6. Delivery order and gates

Execution order: W00 -> W01 -> W02 -> W03 -> W04 -> W05 -> W06 -> W07 -> W08 -> W09 -> W10 -> W11 -> W12 -> W13. W14 customer discovery starts alongside W00; W15 release approval requires W13 and W14. A pause/pivot decision can occur earlier and does not require completing engineering. A package can be split into smaller commits but its prerequisites cannot be skipped.

Investment checkpoint: before substantial W07 scan-codec remodeling, require either one concrete prospective partner workflow or explicit owner approval to continue as a bounded learning/research investment. Review evidence again before W10 scope deletion/remodeling. Negative demand evidence can stop the remaining program at any checkpoint; do not finish all packages merely to justify time already spent.

- G0: W00/W01 evidence and new reproducing tests reviewed. No production-data trial.
- G1: W02–W06 strict policy/identity/publication/ticket semantics pass. Still no production-data trial because scan/stream guarantees are not yet complete.
- G2: W07–W12 supported end-to-end path, limits, packaging and failure tests pass. Independent security review required before sensitive-data pilots.
- G3: W13 reproducible evaluation and W14 partner evidence accepted. Owner decides whether a restricted pilot is justified.
- G4: W15 release/pivot decision. No feature expansion merely because engineering work finished.

The current AGENTS.md requires: “Follow TDD: add tests and expectations first, get review from user only then start code implementation.” For each work package, the implementer writes runnable tests/expected outcomes, produces a concise red-phase review packet, and waits for the owner's approval before production edits. Approve several concrete packets together to reduce cost if desired. Acceptance of this plan is not fabricated approval of tests not yet written.

Effort is deliberately not promised as a calendar estimate for a weaker model. Cap each assignment to one behavioral concern, ideally 1–3 production modules plus tests. Security/serialization/concurrency decisions need independent capable review; a cheaper implementer must not be sole approver.

## 7. Baseline and source map

Prior review baseline, not rerun while writing this plan: 361 passing Python tests, 7 skipped, 164.10 seconds; non-heavy suite 352 passed/16 deselected in 9.41 seconds; Ruff and ty passed. Record a fresh baseline in W00, including commit, environment and clean/dirty state.

Current entry points worth preserving as navigation:

- `src/dal_obscura/data_plane/application/use_cases/{get_schema,plan_access,fetch_stream}.py`
- `src/dal_obscura/common/access_control/{models,filters,policy_resolution,compiled_policy}.py`
- `src/dal_obscura/data_plane/infrastructure/adapters/{duckdb_transform,published_config,ticket_hmac,ticket_store_sqlalchemy,identity_oidc_jwks,catalog_registry}.py`
- `src/dal_obscura/common/{ticket_delivery,table_format,config_store}/`
- `src/dal_obscura/data_plane/infrastructure/table_formats/iceberg.py`
- `src/dal_obscura/connectors/python_sdk.py`
- `src/dal_obscura/control_plane/{application,infrastructure,interfaces}/`
- `tests/{application/access_flow,domain/access_control,infrastructure,interfaces,connectors,benchmarks}/`

The previous review found Delta corruption, whole-file Avro/Delta buffering and weak partial-mask precedence. Removing retired backends plus explicit rejection/migration tests resolves the backend findings; do not spend time repairing Delta/Avro. All six masks, nested reads and JVM/Spark remain active scope and require real fixes/tests. Parent-mask bypass, ticket authorization, asset binding, consumer streaming and distributed retries must be corrected rather than avoided by deleting required capabilities.
