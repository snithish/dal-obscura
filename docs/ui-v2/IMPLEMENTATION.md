# Implementation and evaluation plan

For the current codebase, execute the smaller ordered packets in
[EXECUTION_HANDOFF.md](EXECUTION_HANDOFF.md) and record evidence in
[EXECUTION_STATUS.md](EXECUTION_STATUS.md). The U00–U10 scope below remains required;
the handoff makes the missing implementation, security decisions, and local
runtime gates explicit rather than assuming the foundation is production-ready.

The paid-production objective also requires P13–P16 in
[PRODUCTION_READINESS.md](PRODUCTION_READINESS.md), including per-workflow backend
coverage, deployment boundaries, restore/upgrade drills, and whole-product release
evidence. U10/P12 success alone does not establish readiness to serve customers.

This is an implementation handoff, not evidence of completed functionality. Work in atomic units; retain the owner's commit authorization. Follow applicable test-review instructions without repeatedly seeking approval already granted for a concrete packet. Do not modify pickle code. Do not infer permission to deploy or contact users from this plan.

## Architecture decisions

Build a new frontend under proposed `apps/governance-ui/`. Suggested feature boundaries: `assets`, `policy-studio`, `policy-tests`, `changes`, `connections`, `activity`, and `administration`; shared code covers the application shell, accessible primitives, API client, auth state, and design tokens. Keep route components thin. Domain-specific schema/rule widgets belong in their feature, not a universal components dumping ground.

U02 selects the actual framework/tool versions through a bounded spike. Initial candidate: React + TypeScript + a small SPA build, route library, server-state query cache, schema-aware forms, accessible primitives, and an on-demand expression editor. Require maintained dependencies, compatible licenses, accessible tree behavior, clean production builds, and a documented bundle budget. Avoid adding a second JavaScript application server when the Python admin boundary suffices.

Backend remains in `src/dal_obscura/control_plane/`: interface routes call narrow application services for asset registration, draft revision, evaluation, publication, and management. Reuse repositories only behind those services. UI and CLI consume the same compiler/evaluator/publication semantics. No policy SQL evaluation in the browser and no duplicate frontend permission resolver.

Separate server state from unsaved editor state. Cache keys include authorization/workspace scope and resource revision; clear private caches on logout or capability changes. Server state must not leak through stale route caches between identities. Lazy-load large editors and schema viewers.

## Proposed API contract work

Existing API routes are candidates for adaptation, not a ready-made new UI contract. Inventory them before assigning final endpoint names. Version new contracts and generate TypeScript request/response types from the reviewed OpenAPI schema.

- Session/capabilities: authenticated actor, authorized workspace, available actions; no browser-readable access/refresh token response.
- Assets: scoped paginated search; details; schema by fingerprint; register/update/disable with dependency validation.
- Drafts: create from active generation; read/update with expected revision or ETag; explicit conflict response; restricted discard. Autosave never activates.
- Validation: structured issues with safe codes, rule/field references, severity, and source revision/digest. All six mask types and nested paths use canonical backend models.
- Evaluation: bounded synthetic persona/fixture input; decision explanation, governed output schema, and synthetic transformed output. Bind result to all input revisions and evaluator version.
- Publication review: authoritative current-vs-proposed diff, test freshness, runtime compatibility, affected scope, and unknown-impact markers. Verify permissions for every affected asset, not only the page currently open.
- Publish: expected generation, draft revision/digest, and idempotency reference. Atomic activation, operation status lookup, explicit conflict/unknown-outcome reconciliation. Never trust client-submitted “validation passed”.
- History/activity: scoped cursor pagination, safe event detail, compare revisions, restore as draft. Sensitive policy diffs require dedicated authorization.
- Management: connections, owners/operator grants, health and capability inventory, secret-reference status. Mutation endpoints enforce server-side invariants and audit attribution.

Use typed Invalid / Forbidden / Not found / Conflict / Stale / Capacity / Unavailable errors and safe request references. Do not convert missing capabilities into an empty successful response. Distinguish validation failures from failed network/storage checks.

## Work packages

### U00 — Scope, workflows, and contract inventory

Inputs: this plan; gateway nested/security contracts; current routes/services/tests and migration history.

Deliver: current-to-required capability inventory; role/action/resource matrix; golden-journey storyboards; schema/policy fixture corpus; explicit “existing / reusable after review / new / blocked” labels. Preserve durable data; do not resurrect removed bundles as the design basis.

Acceptance: every required screen/action maps to a job, permission, backend capability, and failure state. Mark backend gaps rather than inventing successful UI behavior. Update the W-package dependency map below. Commit scope/contracts separately from code.

### U01 — Interaction and visual prototype

Depends on U00. Build a disposable clickable prototype with clearly marked synthetic data. Cover J1–J6 and at least session expiry, denied metadata, validation error, mask conflict, schema drift, concurrent edit, and uncertain publication result. Produce two limited visual treatments; choose one before production styling.

Run five task sessions with representative authors/operators when available; prepare scripts and record findings without contacting people unless authorized. Until sessions occur, prototype status is review-required, not validated. Owner visual review and usability evidence are distinct gates.

Acceptance: users identify active versus draft policy; explain AND row restrictions and inherited masks; predict publication scope; complete nested edits and conflict recovery. Keep a severity-ranked issue log and iterate on critical errors. No frontend scaffolding sprint substitutes for this gate.

### U02 — New shell and component foundation

Depends on U01 direction. Spike the nested tree, expression editor, diff, routing, and auth/session integration. Select pinned frontend dependencies. Build the shell, tokens, navigation, forms, status/error patterns, and component fixtures.

Tests: keyboard/focus behavior, narrow layouts, 200% zoom, reduced motion, accessible errors, loading/empty states, route guards as UX only. Record initial bundle sizes and tree interaction timing.

Acceptance: responsive shell and representative components work in component tests; build from a clean checkout; no generated/bundled UI files manually embedded into Python source. Include a production asset-serving strategy and CSP-compatible build.

### U03 — Administrative identity and API security

Depends on U00/U02 contracts; connects to W04/W11. Implement session boundary and server-side action/resource capabilities before live management mutations. Scope operator roles and owned assets explicitly. Retain local OIDC fixtures for deterministic tests.

Tests first: read token cannot author; owner cannot self-assign publish; publisher cannot edit unrelated scope; direct API requests enforce permissions; CSRF rejection; expired session; forbidden history/preview/export; logout clears UI caches; malicious catalog labels are inert text. Test capability changes against an already open browser session.

Acceptance: role matrix passes through real API calls. No secrets in browser storage, network payloads, screenshots, telemetry, or logs. Session revocation/expiry behavior documented and independently reviewed before production credentials.

### U04 — Asset and connection management

Depends on U03; uses W05/W07 catalog/schema contracts. Implement authorized inventory, onboarding, logical registration, ownership, capability status, schema drift, connection diagnostics, and dependency-aware disable/removal.

Tests: pagination/search scope; unsupported backend rejection; malicious path/URL input; missing catalog; unavailable secret reference; new/deleted schema field; forbidden connection mutation; no source data deletion. Seed large inventories and nested schemas.

Acceptance: J1 reaches a registered synthetic asset; J6 identifies and repairs representative configuration failures. Unknown compatibility is displayed as unknown/unverified. No fake healthy state.

### U05 — Nested policy studio

Depends on U02–U04; requires W02/W03 semantics. Split into atomic slices: schema selection; principal/attribute editing; row expression authoring; masks/inheritance/conflicts; draft persistence/recovery.

Tests: literal dotted names versus nested fields; lists/maps/null containers; map key authorization; scalar-masked parent child rejection; parent pruning; wildcard schema drift; all six masks; overlapping rules; unsupported advanced expression remains intact; stale autosave conflict.

Acceptance: J2 completes with persisted draft and exact round-trip canonical policy. Frontend does not invent field IDs, alter SQL, weaken masks, or overwrite another author's revision. Save errors preserve edits and are visibly distinct from successful save.

### U06 — Tests and decision explanations

Depends on U05 plus a server evaluation contract. Implement synthetic personas/fixtures, allowed/denied expectations, schema/mask/row assertions, explain links, freshness, bounded execution, and cancellation. Reuse canonical evaluator and transformation code; browser never performs authoritative access decisions.

Tests use independently specified expected values, including denied persona, multi-group AND restrictions, hidden dependencies, null containers, and mask conflicts. Mutations that skip a mask/filter must fail relevant tests. Changing draft/schema/persona invalidates prior evidence.

Acceptance: J3 and pre-publication tests work without real data-read privileges. Outage/cancellation produces an explicit non-success result. Clearly label bounded persona coverage; do not imply whole-workspace or population proof.

### U07 — Review, publish, verify, restore

Depends on U06 and W05 atomic generation semantics. Implement review diffs, exact revision binding, publication outcome reconciliation, runtime observation, and restore-as-draft. Include changes involving multiple assets with per-asset permission checks.

Tests: simultaneous publishers on PostgreSQL (one wins); stale generation; edited-after-review draft; duplicate submission; timeout after commit; wrong-scope draft; schema change between validation/publication; old test results; runtime incompatibility; restore requiring revalidation. Validate publication through API and CLI paths.

Acceptance: J4/J5 cannot overwrite concurrent activation or publish unreviewed content. Every successful publication has safe audit attribution. UI explains generation-wide ticket invalidation and distinguishes committed publication from runtime readiness.

### U08 — Activity, administration, and consumer handoff

Depends on U03/U04/U07. Complete scoped audit/history, operator/owner management, connection references, safe diagnostics, and supported Python/DuckDB/Spark/Arrow setup snippets. Provide operational status with timestamp and source of each observation.

Tests: revoked capability disappears after server recheck; audit pagination does not reveal inaccessible assets; logs redact secrets; config requiring restart is not shown active immediately; snippets match supported client contracts and never contain bearer tokens.

Acceptance: authors and operators can complete routine tasks using the UI. Underlying record IDs and deployment internals appear only where needed for support. No unsupported backend or consumer appears as available.

### U09 — Quality, security, and usability gate

Depends on U04–U08. Keep fast component/unit tests separate from browser workflows, real API/PostgreSQL concurrency tests, and resource benchmarks. Run browser tests on the chosen supported Chromium/Firefox/WebKit versions. Check accessibility manually with keyboard and a screen reader as well as automation.

Use the corpus: 10,000 assets; 5,000-field depth-12 schema; 100 rules; long Unicode names; missing/unknown identities; three generations; conflicting drafts; expired sessions; catalog/DB outages. Publish environment, p50/p95 latency, bundle sizes, memory measurements, and reproducible commands.

Acceptance: all J1–J6 browser tests pass; five-user usability targets in README pass or are explicitly held pending participants; no critical/high unresolved authorization or data-loss finding; WCAG audit findings resolved to the agreed AA bar; performance targets measured. Independent security review required; self-authored tests are insufficient approval evidence.

### U10 — Package, migrate, and release

Depends on U09 and applicable gateway gates. Package new UI/admin service separately from Flight privilege boundaries. Provide a local synthetic OIDC demo and production reverse-proxy/TLS/CSP guidance. Verify clean-install startup, deep-link refresh, cache invalidation, migrations, and private asset serving.

Retain authoring records and export capability. Any migration is additive or explicitly reviewed with backup/restore proof. When replacing an existing surface, cut over through a documented route/deployment switch; remove obsolete assets only after replacement acceptance. Rollback must not reactivate known insecure application behavior or alter pickle logic.

Acceptance: fresh installation completes J1 through actual nested DuckDB/Spark/Arrow verification; restore drill succeeds; operator documentation matches shipped actions; owner accepts the release packet. No production rollout occurs merely because this plan exists.

## Sequence and master-plan mapping

U00 → U01 → U02 → U03 → U04 → U05 → U06 → U07 → U08 → U09 → U10. Implement packages in small vertical slices; within a package, independent documentation/fixtures may proceed alongside backend work. No automatic agent delegation is implied.

- W02/W03 supply typed nested paths, masks, validation, and effective decision semantics to U05/U06.
- W04 supplies identity guarantees; U03 adds explicit administrative capability/session requirements.
- W05 supplies publication binding/concurrency; U07 supplies review UX and stale/unknown-outcome handling.
- W07 supplies verified Iceberg schema/capability information to U04; missing read capabilities remain gateway blockers.
- W09 supplies bounded evaluation/operation principles to U06/U09.
- W10 retains UI/admin authoring as required scope and removes only unsupported backends/obsolete implementations after acceptance.
- W11/W12 include UI/admin packaging, least privilege, frontend tests, and CI; U09/U10 provide evidence.
- W13 includes independent UI/API security evaluation. W14 usability participants and market/design-partner evidence are different evidence sets. W15 release decision includes both UI and gateway readiness.
- W06's pickle conflict remains a separate owner-directed boundary. This UI plan neither changes serialization nor claims W06 complete.

## Effort and implementation discipline

Plan in milestone gates rather than an unconditional delivery date: (1) validated prototype U00–U01; (2) real nested draft/testing slice U02–U06; (3) safe management/publication U07–U08; (4) evidence and rollout U09–U10. Re-estimate after U00 API inventory and U01 task sessions. Backend evaluation and publication gaps may dominate effort; a smaller frontend does not remove them.

Each implementation assignment must name one behavior, dependencies, contract changes, tests/expected outputs, touched modules, and completion evidence. Suggested commit units are `test(ui): cover nested field selection`, `feat(ui): add nested policy field editor`, and `feat(admin): reject stale draft updates`; keep production/test changes coherent under the project's test-review rules. No giant “rewrite UI” commit.

Do not mark a package complete for screenshots, a running dev server, mocked success responses, or test counts alone. Report implemented versus simulated behavior and outstanding security/UX findings. If an API contract is missing, deliver its specification/tests rather than silently approximating backend policy in JavaScript.

## Release evidence packet

Record candidate commit, package status, browser/API/DB versions, role matrix, J1–J6 outcomes, test commands, expected-output fixtures, performance measurements, accessibility review, participant findings, independent security findings, migration/restore evidence, and limitations. Attach sanitized screenshots of happy and failure states. Record exactly which capability is verified and which still blocks release.

Planning artifacts are ready when this plan and its master-plan links are coherent. Production readiness requires the work and evidence above; neither the visual map nor this document is a production prototype.
