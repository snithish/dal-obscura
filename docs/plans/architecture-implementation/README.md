# Architecture implementation plan

## Status and authority

This is an implementation specification, not an instruction to begin changing application code. The current request authorizes preparing this plan. Start implementation only when the user asks to implement it. That instruction approves the redesign described here once; do not ask for approval at every task.

Baseline: commit `80dde1e476c26629022d15b980148496e6de44b5`, reviewed on 2026-10-14. An existing uncommitted root `AGENTS.md` rewrite belongs to the user. Preserve it. Read the current root instructions and [testing guide](../../testing.md) before executing this plan.

The [task manifest](tasks.json) defines task order and status. The [module map](module-map.json) defines destinations for existing Python files. The [governance inventory](governance-inventory.json) records existing functions and HTTP service calls. These are planning inputs, not generated production code. Paths in those manifests are relative to the repository root. They describe the baseline, not permission to overwrite later changes.

## Instructions to the implementing agent

1. Read this entire document once. Then execute exactly one ready task at a time, in numeric order. Do not delegate unless the user authorizes delegation.
2. Before a task, inspect its source files, relevant tests and current Git diff. Verify the predecessor's acceptance record. If a referenced file has moved, use the module map and Git history. Do not recreate an obsolete module.
3. For a new behavior, write the specified behavioral test first and observe its expected failure. For a mechanical move, preserve existing assertions and run the existing owning tests before and after the move. Do not manufacture a failing test for a rename.
4. Implement the full task, including callers, fixtures, mocks, scripts, configuration references and documentation affected by it. Import-path edits in callers outside the task's owner are allowed. Unrelated refactors are not.
5. Do not move to the next task until required checks pass. An unavailable prerequisite is a blocked check, not a pass. Continue only independent work that does not rely on the blocked task.
6. Record exact commands, exit status, test counts, changed paths, removed paths and known limitations in `docs/plans/architecture-implementation/progress.md`. Create that file when implementation begins. Do not claim a measured improvement from static inspection.
7. Mark a task complete in `tasks.json` only after its acceptance gates pass. Do not mark the complete program finished until T14 passes. Do not commit, push, merge, deploy or modify shared services unless separately authorized.

If interrupted, resume from the first incomplete task. Re-read its progress entry and inspect the actual diff. Never assume that a previous agent's “done” statement is evidence.

## Fixed design decisions

Implement the following decisions as written. Do not substitute another architecture.

- Keep Python 3.14, uv, SQLAlchemy, PostgreSQL, DuckDB, Arrow Flight, React, TanStack Query and the existing policy draft reducer. Keep the UI's pinned Node and pnpm versions.
- Keep the current HTTP and Flight processes. Add no network service, queue, scheduler, event bus, cache server, database table or migration revision.
- Keep all HTTP paths, request/response fields, status codes, error envelopes, Flight protobuf messages, ticket reference fields, stored ticket JSON fields, scan envelope version, CLI command names and plugin entry-point identifiers unchanged.
- Keep strict SQL revocation checking immediately before each output batch. Add no revocation cache, polling interval, lease or asynchronous invalidation protocol.
- Implement cooperative input checkpoints and one shared execution-admission owner. Do not implement worker processes, hard-kill deadlines, weighted memory admission or an RSS ceiling in this program. Those recommendations remain separate evidence-gated proposals.
- Change internal Python imports directly. Do not add forwarding modules, re-export compatibility aliases or a second implementation of policy semantics.
- Keep identity provider implementation paths, especially `dal_obscura.identity.oidc.OidcJwksIdentityProvider`, stable because configuration stores provider identifiers. Identity, runtime and plugin implementations remain in their current packages as explicitly classified adapters.
- Keep server dependency extras and package release boundaries. Do not create a new wheel for the kernel or split the Python client distribution.
- Use typed immutable values between owners. JSON conversion belongs at persistence or transport boundaries. Existing immutable policy value types should be reused rather than replaced.
- Do not rename tests during T01 through T13. Preserve their existing owner paths so commands below remain valid. Updating test import paths is required. A separate test-tree rearrangement is outside this plan.

The review's counts are supporting evidence, not acceptance targets. Do not optimize file line counts or the number of hooks. The completion criteria are dependency direction, lifecycle ownership and observable behavior.

## Target dependency rules

These rules apply to runtime imports, function-local imports and type-only imports. Enforce the final rules in T13. Until then, track remaining old-layout edges explicitly; do not add new ones.

| Owner | Allowed dependencies inside the service |
| --- | --- |
| `kernel/identity` | Its own modules and standard library |
| `kernel/policy` | Its own modules and `kernel/identity` |
| `kernel/plans` | Its own modules, `kernel/policy`, `kernel/identity` |
| `application/reads` | Its own modules and `kernel` |
| `application/governance` | Its own modules and `kernel`; `application/reads/ports.py` for compiler and row-transform contracts only |
| `adapters` | `kernel`, application contracts, and other concrete adapters composed within a single adapter boundary |
| Existing `identity`, `plugins`, `runtime` | Treat as concrete adapters; may depend on kernel/application contracts but not HTTP or CLI handlers |
| `interfaces` | Kernel values and application operations; no SQL sessions or concrete providers in request handlers |
| `bootstrap` | All owners, solely to construct and close the application |

External libraries permitted in the kernel: Arrow schema/value types, sqlglot for existing filter parsing, and SDK path/schema contracts. SQLAlchemy, FastAPI, DuckDB connection creation and concrete provider implementations are forbidden there. There is no new general-purpose `utils` or `common` package.

The CLI process entrypoints may call bootstrap. The HTTP and Flight server factories accept already constructed application dependencies. Neither factory chooses a provider, opens an engine, nor builds a plugin registry. Transport-specific TLS, cookies, headers and response serialization remain in interfaces.

Existing `connectors`, generated `flight`, `logging_config` and `observability` stay at their current locations. Do not turn this program into a general rename of every directory.

## Required internal contracts

### Identity and policy

Move `ControlPlaneActor` to `kernel/identity/actor.py` with its current validation and issuer/group encoding behavior. Move the pure name encoding functions from `identity/names.py` to `kernel/identity/names.py`; update callers directly. Move `Principal` from `policy/models.py` to `kernel/identity/principal.py`. Keep `PrincipalConditionValue` with policy models because it describes policy conditions, not authenticated identity. Keep `MaskRule`, `AccessRule`, `DatasetPolicy`, `Policy`, `AssetPolicy` and `AccessDecision` in `kernel/policy/models.py`.

Move `ExecutionProjection` to `kernel/plans/projection.py`. Admission helpers must not import it if that creates a policy-to-plans cycle: put the orchestration that constructs an execution projection in `application/reads/admission.py`. Keep filter validation and policy decisions in the policy kernel. Do not resolve a cycle by moving an import into a function or `TYPE_CHECKING` block.

### Physical projection compiler

Define `CompiledProjection` in `kernel/plans/projection.py` with exactly these fields: `select_list: tuple[str, ...]` and `output_schema: pyarrow.Schema`. It replaces the existing `ProjectionPlan` name internally.

Define `ProjectionCompiler` in `application/reads/ports.py` with a method `compile(schema, columns, masks) -> CompiledProjection`. Preserve the current argument meaning. `columns` is an ordered sequence of canonical field paths and `masks` is a mapping of paths to existing `MaskRule` values.

Implement `DuckDBProjectionCompiler` in `adapters/duckdb/compiler.py` using the existing projection traversal, SQL expressions and literal type inference. Move connection setup into `adapters/duckdb/connection.py`. Both read planning and row transformation must use this implementation through the port. Never maintain a second Arrow output-schema compiler.

`ReadService` receives a required `projection_compiler` constructor argument. There is no default that imports or constructs a concrete compiler. Preview evaluation receives the compiler through application composition as well.

### Captured configuration

Define immutable values in `kernel/plans/snapshot.py`:

- `PublishedCatalog`: name, plugin ID, revision, and recursively immutable options.
- `PublishedSource`: backend ID, table identifier, and recursively immutable source options.
- `PublishedSchema`: encoding, ordered field records, stable-ID flag and existing digest. Absence remains `None`; do not manufacture an empty admitted schema.
- `PublishedAssetSnapshot`: asset UUID, catalog value, target, source value, existing `AssetPolicy`, optional admitted schema. The asset UUID remains optional where the current snapshot contract permits it; ticket issuance still requires a resolved asset identity.

Use field names matching the existing stored meaning. A mapping is copied and wrapped read-only; nested lists become tuples; nested mappings are recursively frozen. JSON scalars retain their types. Reject unsupported mutable objects instead of stringifying them. Serialization creates fresh dictionaries and lists. Arrow schemas remain Arrow schemas. Do not write a generic object serializer.

Define `PublishedSnapshotReader` in `application/reads/sources.py` with `get_asset_and_catalog(catalog: str, target: str) -> PublishedAssetSnapshot`. The provider registry receives this protocol, not `LiveConfigStore`. Move `AdmittedPluginSnapshot` protocol out of SQL code into the application source contracts. The SQL snapshot adapter builds these values within the same repeatable transaction used today, then closes that transaction before provider IO. It must not change which policy revision or catalog revision is captured. Keep `LiveRuntime` behavior stable; moving its declaration is not permission to change runtime refresh behavior.

### Governed read plan

Replace `AuthorizedRead` with `GovernedReadPlan` in `kernel/plans/governed.py`. Preserve all existing values: base schema, decision, visible/execution projection, authorization columns, requested filter, effective full filter and output schema. Add the corresponding `CompiledProjection` value; derive `output_schema` from it rather than maintaining two independently writable schema fields.

The plan contains no provider instance, SQL session, secret material or bearer token. It may reference existing immutable policy/identity values. Runtime source ownership remains in the application source context.

Keep `PlanRequest` as the public internal request type in `kernel/plans/request.py`. Keep the existing validation and ordered-column semantics. Merge application `Source`, `ScanTask`, `Plan`, `AccessContext` and their ports into `application/reads/sources.py`, eliminating the current source-contract/planning type cycle. These are runtime application values and must not be serialized with a generic serializer.

Keep the current ticket JSON format. Encode the same fields from the new plan. Decode using the existing strict validation. The new typed plan is an internal representation, not a new persisted protocol.

### Governance commands and persistence

Use concrete application classes `AssetsCommands`, `CatalogsCommands`, `PoliciesCommands`, `WorkspaceCommands`, `AuditQueries` and `PluginCommands`. Place each beside its feature under `application/governance`; audit and plugin commands use their own modules. Each public method uses the exact `command_class` and `command_method` in the inventory's `command_surface`. Preserve its business inputs, but take no `Session` or `ControlContext` argument. Inventory helper functions are implementation inputs, not additional public commands. Convert FastAPI/Pydantic request wrappers into plain typed inputs before dispatch. Preserve HTTP handler names so generated operation IDs stay stable.

Each class receives a `GovernanceUnitOfWorkFactory` and only the provider/compiler ports it actually needs. Every public governance command/query in command_surface requires keyword-only `actor: ControlPlaneActor` and `correlation_id: str | None`. Routes obtain the actor through the existing authentication dependency, including routes that previously authenticated only through a decorator. Never accept an actor from an HTTP body. Use the current HTTP request ID as correlation_id; non-HTTP callers pass their own correlation ID or None. Avoid a new global context variable dependency in persistence.

Define unit-of-work and repository protocols in `application/governance/ports.py`. A unit of work owns `assets`, `catalogs`, `policies`, `workspace` and `audit` repositories, plus explicit `commit()`. In T08 add `browser_sessions`, `login_transactions` and `login_rate_limits`, backed by the existing session-store implementations; these add no tables. Exiting without a successful commit rolls back and closes. Repository methods are the existing public persistence functions with the leading session argument removed and a bound session supplied by the SQL implementation. Private SQL helpers stay private to SQL adapters.

Implement SQL repositories and unit of work under `adapters/sql`. Put the implementation in `adapters/sql/unit_of_work.py`; name the concrete types `SqlGovernanceUnitOfWork` and `SqlGovernanceUnitOfWorkFactory`. A repository must not commit. The application command decides when to commit. Keep locks, compare-and-swap predicates, row ordering and transaction isolation unchanged.

Read-only provider operations use three explicit phases: capture in a short unit of work, external IO after exit, then revision validation/completion in a new unit of work. An application method owns all three. HTTP must not manually sequence them.

Do not wrap `ControlContext` in a protocol and call that decoupling. No application function may expose an ORM record or SQLAlchemy type. Where a current persistence function returns an ORM record, define the smallest immutable result containing the fields actually read by its callers and return it from the adapter.

### Execution lifecycle

`application/reads/execution.py` owns `ExecutionSupervisor`, `ExecutionLease` and the resource-error contract. A single supervisor instance is created per Flight process and injected into every read service in that process. Retain `max_active_streams` and the current immediate overload rejection behavior.

The supervisor's `open(source_factory, transform_factory, guard)` returns a managed lazy iterator. Acquire a slot on the first `next()`, before calling `source_factory`. A never-started stream holds no slot and opens no provider. If initialization fails, release its slot and close any resources already acquired. Closing is idempotent.

Split guard methods into `check_progress()` and `check_disclosure()`:

- `check_progress()` checks cancellation, stream deadline, ticket expiry and authenticated identity expiry before each source pull and after each source pull. It does not query SQL.
- `check_disclosure()` performs those checks and the existing SQL `ensure_active` call after transformation, immediately before returning each output batch.
- Use the same captured clock/deadline values as the current stream guard. Cancellation uses an injected predicate, defaulting to false only when the transport supplies no cancellation signal. Add keyword-only `cancel_check: Callable[[], bool] | None = None` to the internal `ReadService.fetch` method. The Flight handler passes `context.is_cancelled`; that method exists in the installed Arrow version. Do not store the RPC context in a ticket or persistent object.
- A canceled or expired scan that filters out every row must stop at its next input checkpoint. Do not promise interruption inside a blocked native/provider call.

The source factory performs the current lazy provider opening. The transform factory takes a guarded input iterator and the compiled projection. Change `RowTransformPort.apply_filters_and_masks_stream` to accept `(batches, projection: CompiledProjection, row_filter: RowFilter | None)`. The transform builds its query from `projection.select_list` and the full row filter. Validate actual input batches against the declared execution schema returned by `Source.execute`, which can be a projection of the base schema. Do not compare projected input batches with the full base schema. Preserve the existing provider metadata checks. Compilation is not repeated inside the transform. Keep row filters applied to original input before masks. Remove the transform's separate admission semaphore only after the supervisor owns admission for all its application callers.

Keep input/output byte checks and the current DuckDB memory limit. These remain rejection limits, not a claimed process RSS guarantee. Keep existing exception classes or move their definitions without changing transport error classifications. Cleanup failure must not replace an active primary exception.

### Frontend ownership

Keep current URLs, hash routes, visible copy, accessibility labels and API types. Do not add a router or state library.

- `features/identity/SessionBoundary.tsx` owns session generation, the private `QueryClient`, private request abort scope, login/logout and auth-expiry teardown. It exports `useSessionScope()` from that same module. Return session, generation, query client, a request-registration function returning `{signal, finish}`, and an `isCurrent(generation)` predicate. `finish` is called in `finally`.
- On logout/auth expiry, invalidate the generation and hide/unmount private children synchronously before awaiting the server. Abort registered requests and clear that generation's query cache. Preserve failed-logout retry behavior. No private query result may update a new generation.
- `features/assets/AssetRoute.tsx` owns selected-asset queries, schema, grants and access. Use declarative queries with existing session-scoped keys. Render only the selected asset's results. Do not copy query responses into root component state.
- `features/policy/PolicyEditorController.tsx` owns draft state, save/revoke/preview, preview persona generation and draft revision fences. Reuse `usePolicyDraft` and its reducer. A late successful save must not erase edits made after submission. Cancellation must never clear the unsaved draft.
- `features/activity/useActivityQueries.ts`, `features/connections/useConnectionsQueries.ts` and `features/settings/useSettingsQueries.ts` own their respective queries. Define their feature result types beside the hooks, not in view components. Do not import activity types from connections or settings.
- `app/main.tsx` mounts providers and an `app/Workspace.tsx` composition component. Workspace owns shell/navigation/command palette presentation and dirty-leave confirmation, not session mechanics or API mutation state.
- `data/api.ts` remains the transport client and generated-type adapter. `data` must not import `features` or `app`. Keep its existing auth-expired event contract; SessionBoundary consumes the event.

Use `SessionScope -> AssetRoute -> PolicyEditorController` ownership. Dirty-leave confirmation is an explicit callback/boolean contract to Workspace. Do not introduce a global mutable feature registry.

## Verification commands

Run all commands from the repository root. Record real results; never copy the review's test counts into a completion record.

### Q0: environment and baseline

```sh
git status --short
git rev-parse HEAD
uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen
pnpm --dir apps/governance-ui install --frozen-lockfile
```

Verify runtime versions against `.python-version`, `pyproject.toml`, UI `package.json` and the native reader toolchain. If the baseline differs, inspect intervening changes before applying the module map. Do not reset to the baseline.

### Q1: Python quality and local contracts

Re-run the sync command after source moves or changes. Installed wheels otherwise retain old code.

```sh
uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check
uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket'
```

For a task's focused run, use its listed test directories in place of `tests packages`, retaining `-o pythonpath=`. New tests belong in those existing directories.

### Q2: UI qualification

```sh
pnpm --dir apps/governance-ui test
pnpm --dir apps/governance-ui test:stories
pnpm --dir apps/governance-ui test:e2e
pnpm --dir apps/governance-ui check
pnpm --dir apps/governance-ui check:api-types
pnpm --dir apps/governance-ui build
```

### Q3: final qualification

Provision a disposable PostgreSQL instance and set `DAL_OBSCURA_POSTGRES_TEST_URL` to its `postgresql+psycopg` URL. Do not use an existing demo or shared database. Follow the prerequisites and native-provider setup in the testing and conformance guides.

```sh
DAL_OBSCURA_RUN_CONSUMER_TESTS=1 uv run --no-sync python scripts/require_no_skips.py \
  --junitxml /tmp/dal-architecture-python.xml -- \
  uv run --no-sync pytest tests packages -o pythonpath= --ignore=tests/benchmarks
mvn -f connectors/jvm/pom.xml verify
```

Also run Q2 and the documented installed-wheel, separate-worker S3 and live SSO lanes. Identity composition changes make live SSO required. Missing credentials or external infrastructure must be recorded as a qualification blocker, not silently waived.

### Q4: comparable performance evidence

Use the existing harness rather than inventing workload sizes or claiming results from the review probes:

```sh
sh scripts/run_capacity_benchmarks.sh /tmp/dal-architecture-before
uv run --no-sync python scripts/schema_capture_performance.py --fields 4000 --iterations 5 --output /tmp/dal-architecture-schema-before.json
```

Run the same commands at T14 with `after` in place of `before`. The capacity harness refuses to overwrite its output directory. If the named directory exists, choose a new explicit suffix, record it, and do not delete prior evidence. Preserve benchmark input data, assertions, repetition count and environment across runs. Updating import paths or constructor calls for the new architecture is allowed; changing workloads to improve results is not.

The existing harness does not automatically establish a complete 1/4/16 concurrency matrix or slow-provider SLA. Record which required cases it covers. For missing cases, extend the existing performance test support before production changes with deterministic fixtures and recorded parameters; obtain baseline measurements then reuse the same fixtures after T09. If that baseline is unavailable, mark that comparison unmeasured and make no improvement claim. Functional cancellation behavior still requires T09's deterministic tests.

## Ordered work packets

<a id="t00"></a>

### T00: capture the baseline

Dependencies: none. Change scope: progress record, baseline fixtures/measurement harness support and temporary evidence only. No production changes.

1. Read the manifests, architecture, testing, governance UI and plugin conformance guides.
2. Run Q0 and Q1. Record baseline failures separately; do not weaken assertions to hide them.
3. Run `uv run --no-sync python scripts/export_control_plane_openapi.py --check`, then copy `apps/governance-ui/openapi/control-plane.json` to `/tmp/dal-architecture-openapi-baseline.json`. Capture current ticket/scan-envelope fixtures in test code using existing valid fixtures, not credentials or production data. Do not regenerate a stale baseline until its cause is understood.
4. Record plugin identifiers and accepted identity-provider configuration values. Find string-based imports with `rg`, not only Python import statements.
5. Create `progress.md` with one task entry per manifest task, initially pending.
6. Capture performance baselines before any production changes using Q4 above. Record workload definitions and toolchain. If a lane cannot run, mark it blocked; do not later invent a before measurement.

Done when baseline results and public compatibility surfaces are recorded. A failing relevant baseline test blocks its dependent task until the cause is understood.

<a id="t01"></a>

### T01: move identity values and pure policy contracts

Dependencies: T00. Scope: module-map T01 entries, callers and their test imports.

1. Move actor, names and Principal as specified. Do not move OIDC implementations or change stored provider names.
2. Move pure policy modules to `kernel/policy`. Leave physical projection and read-admission orchestration for T02.
3. Move `PlanRequest` and `ExecutionProjection` to the named kernel modules; update import direction without policy-to-plans cycles.
4. Preserve existing identity validation, issuer scoping, attribute normalization, mask behavior and JSON encodings.
5. Remove vacated modules once all references are changed. Keep empty package markers only for real remaining packages.

Focused tests: `tests/policy tests/identity tests/control/test_access_identity.py tests/read/test_query_models.py`. Run Q1.

Done when existing tests retain their assertions, kernel contracts have no concrete adapter imports, and no old imports survive for moved symbols.

<a id="t02"></a>

### T02: isolate the physical compiler and admission operation

Dependencies: T01. Scope: projection/connection modules, application read contracts, read service and preview compiler callers.

1. Move projection implementation and connection setup using the module map. Introduce the required compiler port and result value.
2. Move the existing `_authorize` orchestration and admission helper orchestration into `application/reads/admission.py`; expose one `admit_read(context, principal, request, compiler)` operation.
3. Construct one governed plan from that operation. `ReadService.schema` and `.plan` both call it.
4. Inject the compiler into read admission and preview evaluation. Pass its compiled result into the transform through the specified RowTransformPort contract; the transform must not compile again. Preserve every SQL expression and Arrow schema assertion initially.
5. Remove old physical compiler imports and unused helper exports. Do not retain a second compiler for tests.

Required cases: nested structs/lists/maps, NULL parent values, parent masks, hidden predicate columns, wildcard selection, changed output types from masks, scalar default literal typing and Arrow metadata. Use existing exact-value fixtures. Add missing cases only where the moved boundary could change behavior.

Focused tests: `tests/policy tests/read/planning tests/read/execution tests/control/policies`. Run Q1.

Done when all schema producers use one physical compiler and no kernel module imports DuckDB or read execution.

<a id="t03"></a>

### T03: complete read application and provider separation

Dependencies: T02. Scope: remaining read/source mappings and bootstrap wiring.

1. Move read service, ticket contracts/payload/signing and execution error contracts to destinations in the module map.
2. Merge runtime source contracts in `application/reads/sources.py`. Split `ScanTaskCodec` protocol from concrete `SourceTaskCodec`; the protocol belongs to application ports and the implementation to provider adapters.
3. Move concrete source implementations into provider adapters. Their constructor receives ports and configured dependencies; it does not import an application composition root.
4. Make `ReadService` depend only on application/kernel ports and values. Update actual construction sites, not defaults that synthesize missing implementations.
5. Preserve all ticket fields, hash input ordering, signature verification, exchange counts and decoder rejection rules.

Focused tests: `tests/read tests/sources tests/storage/test_ticket_store_sqlalchemy.py tests/interfaces`. Run Q1.

Done when application reads imports no concrete adapters and existing signed-ticket and scan-envelope fixtures still decode identically.

<a id="t04"></a>

### T04: replace dictionary snapshots with typed captures

Dependencies: T03. Scope: SQL snapshot adapter, typed snapshot values, provider published-source resolver and callers.

1. Move SQL storage modules per the map, preserving migration revision, DDL, packaging and isolation settings. Update migration resource paths, CLI entry-point targets, Docker/script references and imports. Do not hand-edit generated artifacts.
2. Build `PublishedAssetSnapshot` from the current SQL reads. Convert policy once at this boundary, then pass the typed value to source resolution.
3. Replace dictionary lookups in published policy/catalog/schema admission with typed fields. Preserve optional schema and synthetic field-ID behavior.
4. Delete `LiveAsset.compiled_config` and obsolete conversion helpers after callers move. Do not add persisted snapshot tables or a cache.
5. Add a test that mutates original nested option dictionaries/lists after capture; the snapshot must remain unchanged. Add a test that serialization returns detached containers. Preserve repeatable-snapshot race tests.

Focused tests: `tests/storage tests/control/test_live_config_admission.py tests/read/test_live_request_context.py tests/sources`. Run Q1. Run `tests/integration/test_live_snapshot.py` against disposable PostgreSQL without marker exclusions.

Done when captured configuration is detached and immutable, schema admission is unchanged, and provider IO begins after transaction closure.

<a id="t05"></a>

### T05: establish governance ports and SQL unit of work

Dependencies: T04. Scope: governance ports, SQL repositories, actor/error/request metadata contracts.

1. Move control error types to `application/governance/errors.py`. SQL adapters may raise these declared application contract errors; they may not import command implementations.
2. Define the unit-of-work protocols and SQL implementation described above. Adapt existing persistence functions without changing their queries, locks or return meaning.
3. Replace returned ORM objects with immutable values at the adapter boundary. Do not let `Session`, ORM models, cursors or lazy relationships escape.
4. Pass audit request IDs explicitly from application metadata into audit persistence. Keep HTTP request-ID generation and response behavior unchanged.
5. Test commit, rollback on body failure, rollback when commit is absent, and session closure on commit failure. Use real disposable storage for transaction behavior.

Focused tests: `tests/storage tests/control/test_audit_repository.py tests/control/test_structured_conflicts.py`. Run Q1.

Done when repository methods never commit and all SQL writes are controlled by an explicit unit of work.

<a id="t06"></a>

### T06: migrate policy and asset commands

Dependencies: T05. Scope: asset/policy services, their routes and relevant CLI callers.

1. Implement AssetsCommands and PoliciesCommands from the inventory. Replace session parameters with repositories accessed inside command-owned units of work.
2. Keep asset capability checks and row locks before mutations. Require the same revision preconditions and preserve missing-precondition versus stale-revision errors.
3. Keep policy replace, optional active-ticket revocation and audit write in one transaction. Preserve who may revoke tickets.
4. Move schema and policy-preview capture/IO/completion sequencing out of HTTP helpers into command methods. Inject provider/schema/compiler ports.
5. Routes authenticate, parse, dispatch once, then serialize. Remove their transaction lambdas and private business helpers after migration.

Required races: stale policy save changes nothing; failed audit write rolls back policy and revocation; non-owner cannot request revocation; catalog/schema edit between capture and completion rejects stale work; preview remains synthetic and cannot become authentication evidence.

Focused tests: `tests/control/assets tests/control/policies tests/interfaces/http tests/storage`. Run Q1 and relevant PostgreSQL race tests.

Done when asset/policy routes contain no SQL session or capture/IO/completion sequencing.

<a id="t07"></a>

### T07: migrate catalogs, workspace, audit and plugin commands

Dependencies: T06. Scope: remaining governance functions and their routes.

1. Implement remaining command/query classes from the inventory. Move catalog discovery's three phases into CatalogsCommands.
2. Preserve discovery cursor binding to catalog revision, actor and requested page limit. Preserve per-session admission and plugin registry identity.
3. Preserve catalog option depth/size restrictions, inline-secret rejection, secret scope authorization and outbound-destination checks.
4. Move workspace runtime/auth-provider mutation transactions and audit queries to application operations. Keep revision fences and redaction unchanged.
5. Implement plugin commands with an application port to the existing startup-admitted registry. Do not load plugins dynamically from requests.
6. Delete ControlContext and ControlPlaneDeps.with_transaction after all callers are removed. Retain transport authentication/CSRF logic until T08 supplies application session ports.

Focused tests: `tests/control tests/interfaces/http tests/plugins tests/storage`. Run Q1 and PostgreSQL concurrency tests.

Done when every governance route dispatches to an application operation and the old control implementation tree is empty or removed according to the map.

<a id="t08"></a>

### T08: centralize startup and session composition

Dependencies: T07. Scope: HTTP factory, CLI startup, bootstrap, identity session adapters and transport dependency contracts.

1. Create `bootstrap/read.py` and `bootstrap/control.py`. Move concrete engine/registry/secret/provider/compiler/application construction into them.
2. Keep CLI entrypoints at their current public targets. They parse flags/environment and call bootstrap; preserve help output and exit behavior.
3. Make HTTP create_app accept constructed dependencies rather than a session maker plus optional concrete adapters. Update production callers and test factories together; do not leave compatibility overloads.
4. Create `application/governance/sessions.py` with `SessionCommands`. Move the database-owning methods listed in inventory `session_command_surface` from ControlPlaneDeps into this class, preserving their signatures apart from self. Replace their raw session scopes with the unit of work. Expose the three existing stores through repository protocols with the same public methods. Move LoginTransaction and LoginRateLimitDecision value types to `kernel/identity/session.py`; keep SQL-backed store implementations in `identity/sessions.py`. HTTP continues to own cookies, CSRF/origin validation, bearer/nonce verification dispatch and OIDC redirect responses.
5. Preserve bootstrap-secret behavior, nonce/code exchange validation and login/logout expiry rules. Keep stored identity provider implementation identifiers unchanged.
6. Give startup-created resources explicit close ownership. Do not detach background cleanup threads from their shutdown owner.

Focused tests: `tests/interfaces tests/identity tests/control/test_browser_sessions.py tests/runtime`. Run Q1. Run live SSO before final completion.

Done when request handlers import no SQLAlchemy/concrete provider modules and both services start through the same composition paths exercised by test factories.

<a id="t09"></a>

### T09: add execution supervisor and input checkpoints

Dependencies: T08. Scope: read lifecycle, transform, provider lazy opening and Flight stream cancellation.

1. Write an owning test whose source advances a fake clock between input batches, with a filter rejecting every row. The existing output-only guard must fail this new expectation before implementation.
2. Implement the exact lifecycle contract above. Move admission to the shared supervisor and preserve lazy opening.
3. Wrap source input with progress checkpoints; retain the SQL disclosure check after transformation. Keep provider SDK deadline checks as an additional boundary.
4. Test cancellation using an explicit injected signal, not wall-clock sleeps. Use provider-controlled barriers for concurrent cases.
5. Remove the transform-local semaphore after all production execution uses the supervisor. Adapt direct transform tests to distinguish compiler/transform behavior from admission behavior.

Required assertions: a zero-output scan stops before the next input pull after cancellation/expiry; one output means one SQL disclosure check; the second stream is rejected at capacity before opening its source; closing or failing the first releases capacity; never-started streams open no source; primary errors survive cleanup failure; early stop closes each owned resource once.

Focused tests: `tests/read tests/interfaces/flight tests/storage/test_ticket_store_sqlalchemy.py`. Run Q1. Record before/after SQL counts and source-pull counts. Do not claim latency or RSS gains.

Done when one process-level supervisor owns admission and progress, while full-filter and disclosure semantics are unchanged.

<a id="t10"></a>

### T10: introduce the UI session boundary

Dependencies: T09. Scope: SessionBoundary, app composition and identity browser tests.

1. Move login/logout/auth-expiry state and request/cache lifecycle from App to SessionBoundary.
2. Increment/invalidate generation before clearing and unmounting authenticated children. Every private async continuation must check its captured generation before updating state.
3. Preserve sessionQueryScope encoding and failed-logout retry behavior. Keep request cleanup in finally and do not assume abort prevents all responses.
4. Mount Workspace under this boundary. Keep presentation controls local to Workspace.
5. Add browser tests with deferred responses: logout during asset load, logout during save, and sign-in as another identity before the old response resolves. Assert old private values never reappear.

Run Q2. Existing identity and shell browser suites are required, not only stories or unit tests.

Done when App no longer owns session teardown or the global mutation-controller registry, and stale private results cannot cross generations.

<a id="t11"></a>

### T11: move asset and policy workflows into their features

Dependencies: T10. Scope: AssetRoute, PolicyEditorController, asset/policy views and tests.

1. Move selected asset detail/schema/grant/access queries to AssetRoute. Preserve query keys and cancellation semantics.
2. Move save/revoke/preview and persona state to PolicyEditorController. Keep reducer semantics and submission edit-version fences.
3. Connect dirty-leave confirmation through explicit props/callbacks. Preserve drafts on request failure, cancellation and revision conflict.
4. Delete manual root asset hydration and root policy mutation state after the feature paths own all callers.
5. Test save completing after a newer local edit, navigation while preview is pending, changing persona during preview, stale revision response, duplicate save prevention and failed logout while a draft exists.

Run Q2. Assert exact policy values and visible draft state, not only request counts.

Done when root composition has no asset API queries or policy mutation logic and existing policy authoring behavior is preserved.

<a id="t12"></a>

### T12: split management queries and remove UI reverse imports

Dependencies: T11. Scope: activity/connections/settings hooks, feature types, Workspace and shared data layer.

1. Replace useManagementQueries with the three specified feature hooks. Keep activity pagination and summary/observations behavior.
2. Move feature result types out of ActivityView. Connections and settings must not import activity view/types.
3. Wire Workspace to feature route components. Keep global shell/navigation state local to app composition.
4. Delete the obsolete dispatcher and root management result merging. Do not change visible copy or endpoints.
5. Add import-boundary checks using the existing Node test runner and installed TypeScript parser. Verify imports/exports/dynamic imports, not a fragile text regex. Permit app to import features; forbid data importing app/features and features importing app/main.

Run Q2. Verify activity pagination and admin-only management visibility in browser tests.

Done when feature query ownership matches the target contract and the shared data layer has no view dependencies.

<a id="t13"></a>

### T13: enforce boundaries and remove obsolete paths

Dependencies: T12. Scope: architecture tests, stale references, documentation and package resource configuration.

1. Add Python AST import-boundary tests for the final dependency rules. Include nested/function imports and type-only imports. Resolve `from package import module` correctly. Test the checker with forbidden-edge fixtures so a broken checker cannot pass an empty graph.
2. Remove all temporary exemptions. Dynamic provider identifiers are a separate allowlisted startup contract, not a general escape hatch.
3. Verify every module-map entry. No moved source may remain as a forwarding file. Unchanged public entrypoints and generated protocol files remain intact.
4. Search tests, scripts, Dockerfiles, packaging, configuration examples and docs for retired imports. Preserve stored identity identifiers explicitly listed as unchanged.
5. Update architecture docs and root guidance to the implemented dependency rules. Keep this plan and progress as delivery records; do not copy their task inventory into architecture documentation.
6. Build/install wheels outside Git. Verify migrations are packaged and the current revision is discoverable. CLI help, OpenAPI and ticket fixtures must match the baseline contract.

Run Q1 and Q2. Run installed-wheel checks with source-path injection disabled.

Done when the complete dependency policy is enforced, no compatibility wrappers remain, and package/entrypoint behavior survives installation.

<a id="t14"></a>

### T14: qualify and deliver

Dependencies: T13. Scope: verification and fixes necessary to satisfy the agreed contracts only.

1. Run Q3 and all remaining required lanes. Do not use marker exclusions in the final no-skips suite.
2. Run Q4 on the same host and compare against T00 evidence. Record workloads, source version, toolchain, first-batch latency, total duration, RSS, retained bytes and SQL counts where measured. Distinguish deterministic behavior checks from timing measurements. A missing required performance lane is a qualification blocker; an optional unmeasured comparison is a stated limitation, not proof of improvement.
3. Reconfirm original-value filtering, exact output schemas, immutable capture, disjoint distributed work, native deletes, credential-free tickets and preserved UI drafts/session fences.
4. Inspect the final diff, dead-code references and all task records. Explain any measurement regression; do not silently reduce coverage or enlarge limits to pass.
5. Deliver changed ownership summary, exact qualification evidence, measured limits and remaining blocked checks. Keep process isolation, revocation services and weighted admission excluded.

Done only when all required checks pass. If an external lane cannot run, mark the implementation “implemented, qualification blocked,” identify the missing prerequisite, and leave T14 incomplete.

## Stop conditions

Stop the affected task and report the concrete conflict if any of these occurs:

- A required change alters persisted schema, ticket/scan format, public API behavior or stored identity/plugin identifiers.
- Exact Arrow output values/schema cannot be preserved by the existing compiler implementation.
- A proposed dependency needs a forbidden edge or a new persistent subsystem.
- A test failure reveals an unrelated existing behavior bug requiring a product decision.
- The baseline changed enough that a mapped source now has a different owner or public contract.

Do not “solve” these cases with aliases, fallback code, silent exception handling, relaxed tests, arbitrary timeouts or a new framework. Record the finding and continue only independent tasks.

## Copy-paste execution prompt

```text
Implement the approved architecture plan in docs/plans/architecture-implementation/README.md.
Read the complete plan, tasks.json, module-map.json, governance-inventory.json and current AGENTS.md.
Work serially from the first incomplete task. Follow the fixed contracts and task acceptance gates.
Preserve unrelated changes. Do not expand scope or implement excluded designs.
Reinstall affected Python wheels before tests. Record actual commands/results in progress.md.
Mark tasks complete only after their checks pass. Never silently skip required qualification.
Do not commit, push, merge or deploy unless I separately authorize it.
If a stop condition occurs, state the concrete conflict and continue only independent work.
```
