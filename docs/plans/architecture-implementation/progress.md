# Architecture implementation progress

Implementation authorized on 2026-10-15. Work proceeds serially, with no delegation.
The user subsequently authorized committing progress and pushing the current
branch before continuing. Merge and deployment remain unauthorized.

## Authorized checkpoint

Checkpoint includes T00–T04 and the independently verified T05 implementation.
At that checkpoint, T05 was blocked on the recorded acceptance-scope conflict;
the user later approved the scope and T05 passed as recorded below. The unrelated
root AGENTS.md rewrite remains unstaged. The branch is
`codex/policy-authoring-hardening`, tracking the same branch on `origin`.
`git diff --check` passed before staging. Existing verification results below
describe the code in this checkpoint; no qualification gate is waived.

Commands and results:

- `git diff --check`: exit 0.
- `git add -- examples/auth/_shared/scripts/build_runtime.py examples/demo/keycloak/scripts/provision_demo.py examples/nested-mor/scripts/verify.py pyproject.toml scripts/export_control_plane_openapi.py scripts/schema_capture_performance.py src tests docs/plans/architecture-implementation`: initial sandbox attempt failed to create the worktree index lock; the authorized escalated retry exited 0. AGENTS.md was excluded.
- `git diff --cached --check`: exit 0; the staged AGENTS.md path list was empty.
- `git commit -m 'refactor: establish kernel and application architecture boundaries'` with checkpoint and acceptance-conflict body paragraphs: exit 0, commit `44ae67f3`, 208 files changed. All four commit hooks passed: Ruff format, Ruff lint, Ty type check and Pytest non-heavy. The hooks temporarily stashed and restored the unrelated AGENTS.md changes.
- `git push origin HEAD:refs/heads/codex/policy-authoring-hardening`: exit 0, remote advanced from `80dde1e4` to `44ae67f3`.

Continuation review confirmed that T06 depends on T05 acceptance and every
subsequent task depends on that serial chain. The scope question was presented
again after the user's continuation request. T05 was held incomplete pending
that answer; the later explicit scope approval and passing gates are recorded below.
Commit/push authorization did not waive its acceptance gate.
Independent review confirmed SQL repository methods contain no commit and the
unit of work retains explicit rollback/close handling. No dependent code was
changed while awaiting clarification. This record update requires no Python
wheel reinstall or application tests because it changes documentation only.
No commit, push, merge, deployment or shared-service change is authorized.

## T00: capture baseline

Status: complete. No production files changed in T00.

The complete plan, task manifest, module map, governance inventory and current
AGENTS.md were read, along with architecture, testing, governance UI, configuration,
deployment, SDK and conformance guidance. HEAD matches the plan baseline,
`80dde1e476c26629022d15b980148496e6de44b5`. Initial status was ` M AGENTS.md`
and `?? docs/plans/`. Those existing changes are preserved.

### Commands and results

All commands run from the repository root. Logs and wheels stay outside Git.

- `git status --short`, `git rev-parse HEAD`, `git diff --stat`: exit 0.
- `uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen`:
  exit 0, four workspace distributions installed.
- `uv run --no-sync ruff check .`: exit 0, all checks passed.
- `uv run --no-sync ruff format --check .`: exit 0, 380 files formatted.
- `uv run --no-sync ty check`: exit 0, all checks passed.
- `uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-q1-before.log 2>&1`:
  sandbox attempt exited 2 before pytest because the uv cache was inaccessible.
  Escalated attempt exited 1: 1096 passed, 32 failed, 79 errors, 115 deselected,
  38.64 seconds. This attempt is invalid evidence: the capacity harness ran
  `uv run` concurrently and changed the same environment to editable installs.
  Missing installed migration, descriptor and native-writer resources followed.
  The agent caused this collision. A fresh temporary wheel environment is used
  for the replacement run.
- `uv run --no-sync python scripts/export_control_plane_openapi.py --check`:
  exit 0. `cp apps/governance-ui/openapi/control-plane.json /tmp/dal-architecture-openapi-baseline.json && shasum -a 256 /tmp/dal-architecture-openapi-baseline.json`:
  exit 0, SHA256 `a90d16b44a839e4956c3e647c723e4370dd3ca82e10969e90262a8da3a1fac68`.
- `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc search --refresh 'signed ticket payload scan envelope fixture'`:
  sandbox attempt exited 1 on daemon-log access. Escalated retry exited 0:
  600 unchanged files, no indexing errors.
- `PATH=/Users/nithish/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:$PATH pnpm --dir apps/governance-ui install --frozen-lockfile`:
  exit 0 using pnpm 12.3.4 and Node 24.19.0. The initial command with the default
  shell runtime is recorded separately when it exits.
- `uv build --wheel --out-dir /tmp/dal-architecture-baseline-wheels > /tmp/dal-architecture-wheel-build.log 2>&1`:
  exit 0. `unzip -l /tmp/dal-architecture-baseline-wheels/dal_obscura-0.3.0-py3-none-any.whl`:
  exit 0; migrations, the baseline revision and script template are present.
- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-baseline-venv uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen > /tmp/dal-architecture-clean-sync.log 2>&1`:
  exit 0. Installed `direct_url.json` has an empty `dir_info`, and migrations exist.
- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-baseline-venv uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-q1-clean.log 2>&1`:
  exit 0, 1207 passed, 115 deselected in 112.34 seconds.
- The same command with output `/tmp/dal-architecture-q1-fixtures.log`, after
  adding both fixture tests: exit 0, 1209 passed, 115 deselected in 82.69 seconds.
- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-baseline-venv uv run --no-sync pytest tests/read/tickets/test_scan_envelopes.py::test_scan_envelope_matches_architecture_baseline tests/read/tickets/test_ticket_hmac.py::test_signed_ticket_matches_architecture_baseline -o pythonpath= > /tmp/dal-architecture-fixtures-red.log 2>&1`:
  exit 1, both tests failed because their evidence files had not yet been captured.
- A one-off `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-baseline-venv uv run --no-sync python -` capture used
  `runpy.run_path` to call the existing `envelope()` and `_ticket_payload()`
  builders and wrote their encoded values to the two fixture files. Exit 0.
  The tests themselves do not import other test modules.
- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-baseline-venv uv run --no-sync pytest tests/read/tickets tests/acceptance -o pythonpath= > /tmp/dal-architecture-fixtures-green.log 2>&1`:
  exit 0, 49 passed in 0.93 seconds. A separate temporary `unittest.mock.patch`
  on `Path.read_text` changed the expected scan version to 99 and one signature
  digit. Both tests then failed at byte equality (2 failed in 0.41 seconds),
  and the probe exited 0 after asserting `pytest.ExitCode.TESTS_FAILED`.
  Neither production code nor evidence files were mutated by that probe.
- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-baseline-venv uv run --no-sync ruff check . && UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-baseline-venv uv run --no-sync ruff format --check . && UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-baseline-venv uv run --no-sync ty check`:
  exit 0; lint and types passed, 380 files formatted. An earlier sandbox attempt
  exited 2 at uv cache access before the checks started.
- A temporary Python probe read every module-map source and checked its SHA256:
  exit 0, all 126 files match the baseline. Inventory contains 30 commands,
  91 baseline functions, 32 repository methods and 8 session commands.
  Every task specification anchor resolves.
- Initial `pnpm --dir apps/governance-ui install --frozen-lockfile`: exit 1,
  `ERR_PNPM_PNPM_ENGINE_IDENTITY_UNVERIFIABLE`, because sandbox network access
  prevented registry signature verification. The retry with Node 24 and network
  access passed as recorded above; no signature verification was disabled.
- `git diff --check`: exit 0.

### Toolchain

Python 3.14.8 satisfies `.python-version` and `>=3.14,<3.15`. uv is 0.7.5.
The default shell has Node 26.11.1 and pnpm 12.11.2, which do not satisfy the UI
pins. The bundled Node 24.19.0 and project-selected pnpm 12.3.4 satisfy them.
Rust 1.94.0 matches the native reader pin. Java is 17.0.20.1; Maven is 3.10.0.

### Compatibility surfaces

The accepted persisted identity-provider identifier is
`dal_obscura.identity.oidc.OidcJwksIdentityProvider`. Validation allows only this
module. Its fields remain ordinal, module, args, enabled and revision, with the
existing OIDC argument validation and redaction.

Plugin entry-point groups remain `dal_obscura.catalogs.v3`,
`dal_obscura.table_formats.v3` and `dal_obscura.secret_backends.v3`.
Catalog IDs are `iceberg.catalog`, `iceberg.rest`, `manifest`, `delta.directory`
and `delta.catalog`. Format IDs are `iceberg`, `parquet.dataset` and `delta`.
Secret-backend IDs are `environment` and `mounted_file`.

### Performance evidence

`sh scripts/run_capacity_benchmarks.sh /tmp/dal-architecture-before > /tmp/dal-architecture-capacity-before.log 2>&1`
ran with the unchanged five repetitions. Its automatic sync selected editable
installs; its results cannot establish the required wheel baseline. The agent
interrupted its process group with `kill -INT -- -74257` (exit 0); the harness
exited 1. `ps -Ao pid,ppid,pgid,comm | awk '$3 == 74257 {print}'` returned no
remaining processes. The original partial directory is retained. Workspace
`uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen`
then exited 0 and restored wheels.

`UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-baseline-venv UV_NO_SYNC=1 sh scripts/run_capacity_benchmarks.sh /tmp/dal-architecture-before-wheels > /tmp/dal-architecture-capacity-before-wheels.log 2>&1`
exited 0. Workloads and five repetitions were unchanged. It began after the final
Q1 suite exited, and no other test suite ran alongside it. All 15 suite runs
passed: four masking tests, one multifile test and two Flight/streaming tests per
repetition. The summary is `/tmp/dal-architecture-before-wheels/summary.json`.

Mean times across runs were 6.798 ms for filtering, 15.041 ms for masking,
11.308 ms for filtering with top-level masks, 11.460 ms for filtering with nested
masks, 56.121 ms for multifile scans and 18.925 ms for the complex Flight read.
The combined 10M/25M streaming workload averaged 74.615 seconds.
Every 25M probe planned five files of 5M rows, returned four endpoints and 3055
chunks, and bounded chunks at 8192 rows. Large-probe RSS deltas in bytes were
38977536, 24166400, 34619392, 39763968 and 38584320. Small-probe deltas were
50757632, 48824320, 45924352, 163840 and 38813696. RSS sampling is noisy; these
values establish neither an RSS ceiling nor a performance gain.
The harness asserts first-batch latency is less than half total read time, but
does not retain those individual measurements or retained Arrow bytes in its JSON.
The full 1/4/16 concurrency matrix and slow-provider SLA remain unmeasured.
They are not replaced by the process-local throughput results. T14 must assess
required missing lanes explicitly; no improvement claim may use missing baselines.

`uv run --no-sync python scripts/schema_capture_performance.py --fields 4000 --iterations 5 --output /tmp/dal-architecture-schema-before.json > /tmp/dal-architecture-schema-before.log 2>&1`
exited 0. Schema capture median was 4.1334 ms and 95153 Python peak bytes,
with five SQL queries per request. Evaluation median was 10.2796 ms and
115390 Python peak bytes, with seven SQL queries per request. Each evaluation
read policy rules once; neither operation read admitted field records.
These are preliminary measurements taken while other checks ran. They do not
measure native allocations or establish RSS guarantees. The replacement baseline
ran without competing checks:

`UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-baseline-venv uv run --no-sync python scripts/schema_capture_performance.py --fields 4000 --iterations 5 --output /tmp/dal-architecture-schema-before-wheels.json > /tmp/dal-architecture-schema-before-wheels.log 2>&1`:
exit 0. Schema median was 3.7610 ms and 95059 Python peak bytes. Evaluation median
was 9.6922 ms and 115502 Python peak bytes. SQL counts remained five and seven,
respectively, with the same policy/admitted-field reads as the preliminary run.

`UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-baseline-venv uv run --no-sync python /tmp/dal-architecture-lifecycle-before.py > /tmp/dal-architecture-lifecycle-before.json`:
exit 0. This deterministic probe uses three one-row batches, a clock advanced by
each input pull, and the real transform and managed stream. A filter rejecting all
rows with ticket expiry at clock 2 consumed three inputs and emitted no outputs;
it performed zero SQL disclosure checks and returned no expiry error. With all
three rows visible and expiry at 100, three pulls produced three output batches
and three SQL disclosure checks. The script and output are retained for T09.

Changed paths: this progress record, `tasks.json`,
`tests/read/tickets/test_scan_envelopes.py`, `tests/read/tickets/test_ticket_hmac.py`,
`tests/read/tickets/fixtures/scan_envelope_v1.json` and
`tests/read/tickets/fixtures/signed_reference.txt`. No paths removed.
The existing stored-payload fixture at `tests/acceptance/fixtures/ticket_payload_v2.json`
and its canonical-encoding assertions remain unchanged. All production files and
the user's AGENTS.md remain unchanged. No timing improvement is claimed.
Final diff review found only the authorized fixtures and tracking changes.

## T01: move identity and policy contracts

Status: complete. T00 acceptance verified before changes.

Moved actor, names, Principal, pure policy modules, PlanRequest and
ExecutionProjection to the approved kernel owners. Updated callers directly.
Principal has no policy re-export. The old policy marker remains empty because
admission and physical compilation still live there until T02.

Commands and results:

- `uv run --no-sync pytest tests/policy tests/identity tests/control/test_access_identity.py tests/read/test_query_models.py -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t01-before.log 2>&1`: exit 0, 95 passed, 1 deselected in 1.81 seconds.
- `uv run --no-sync python /tmp/dal-architecture-t01-move.py > /tmp/dal-architecture-t01-moves.json && uv run --no-sync ruff check --fix src tests scripts examples packages && uv run --no-sync ruff format src tests scripts examples packages`: exit 0, 47 import-format fixes. The move script accidentally traversed ignored example build copies. Repair used `/tmp/dal-architecture-restore-build.py` to reverse only this turn's import edits in 41 files. Its forward-transform equality checks passed; 33 files match their wheel bytes and pre-existing differences in eight files were preserved. A prior whole-file restoration probe exited 1 on those differences and wrote nothing. Further bulk edits use tracked paths and explicit new modules.
- `uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura`: exit 0, service wheel rebuilt and installed before tests.
- Focused command above with output `/tmp/dal-architecture-t01-after.log`: exit 0, 95 passed, 1 deselected in 1.70 seconds. The deselected socket test is outside Q1; no identity behavior changed.
- `uv run --no-sync ruff check . && uv run --no-sync ruff format --check . && uv run --no-sync ty check`: exit 0, 386 files formatted, lint and types pass.
- `uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t01-q1.log 2>&1`: exit 0, 1209 passed, 115 deselected in 74.82 seconds.
- `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index`: exit 0, 18 added, 9 deleted, 77 reprocessed, no indexing errors.
- Temporary AST audit: exit 0, all kernel imports exclude concrete adapters, including nested and type-only imports. The 36 caller test files retain their non-import ASTs after normalizing moved module strings used by subprocess fixtures and monkeypatches. A first probe omitted that normalization and exited 1; it did not reveal a test assertion change.
- `uv run --no-sync python scripts/export_control_plane_openapi.py --check`: exit 0.
- `rg` search for retired T01 module and file paths outside delivery records: no matches. `git diff --check`: exit 0.

Remaining old-layout edges: service and source code still use `read`, `sources`,
physical `policy.projection`, `policy.admission`, `storage`, `control`, identity
adapter contracts and current composition modules. No such edge originates in
kernel. Their planned owner migrations remain T02 through T08; no exemptions
were added to production or tests.

Changed or removed paths (new kernel paths included; user AGENTS.md excluded):

```text
examples/demo/keycloak/scripts/provision_demo.py
src/dal_obscura/control/access.py
src/dal_obscura/control/assets/access.py
src/dal_obscura/control/assets/schema.py
src/dal_obscura/control/assets/service.py
src/dal_obscura/control/audit_service.py
src/dal_obscura/control/plugins.py
src/dal_obscura/control/policies/compiler.py
src/dal_obscura/control/policies/evaluation.py
src/dal_obscura/control/policies/service.py
src/dal_obscura/control/workspace/attributes.py
src/dal_obscura/control/workspace/service.py
src/dal_obscura/identity/claims.py
src/dal_obscura/identity/contracts.py
src/dal_obscura/identity/http_session.py
src/dal_obscura/identity/names.py
src/dal_obscura/identity/oidc.py
src/dal_obscura/identity/sessions.py
src/dal_obscura/interfaces/flight/contracts.py
src/dal_obscura/interfaces/http/routes/assets.py
src/dal_obscura/interfaces/http/routes/audit.py
src/dal_obscura/interfaces/http/routes/catalogs.py
src/dal_obscura/interfaces/http/routes/deps.py
src/dal_obscura/interfaces/http/routes/plugins.py
src/dal_obscura/interfaces/http/routes/policies.py
src/dal_obscura/interfaces/http/routes/session.py
src/dal_obscura/interfaces/http/routes/settings.py
src/dal_obscura/interfaces/http/routes/workspace.py
src/dal_obscura/kernel/__init__.py
src/dal_obscura/kernel/identity/__init__.py
src/dal_obscura/kernel/identity/actor.py
src/dal_obscura/kernel/identity/names.py
src/dal_obscura/kernel/identity/principal.py
src/dal_obscura/kernel/plans/__init__.py
src/dal_obscura/kernel/plans/projection.py
src/dal_obscura/kernel/plans/request.py
src/dal_obscura/kernel/policy/__init__.py
src/dal_obscura/kernel/policy/attribute_conditions.py
src/dal_obscura/kernel/policy/authorization.py
src/dal_obscura/kernel/policy/filters.py
src/dal_obscura/kernel/policy/mask_types.py
src/dal_obscura/kernel/policy/models.py
src/dal_obscura/kernel/policy/policy_resolution.py
src/dal_obscura/policy/__init__.py
src/dal_obscura/policy/admission.py
src/dal_obscura/policy/attribute_conditions.py
src/dal_obscura/policy/authorization.py
src/dal_obscura/policy/filters.py
src/dal_obscura/policy/mask_types.py
src/dal_obscura/policy/models.py
src/dal_obscura/policy/policy_resolution.py
src/dal_obscura/policy/projection.py
src/dal_obscura/read/contracts.py
src/dal_obscura/read/execution/contracts.py
src/dal_obscura/read/execution/transform.py
src/dal_obscura/read/request.py
src/dal_obscura/read/service.py
src/dal_obscura/sources/access.py
src/dal_obscura/sources/adapters.py
src/dal_obscura/sources/contracts.py
src/dal_obscura/sources/planning.py
src/dal_obscura/sources/published.py
tests/benchmarks/test_masking_row_filter_benchmarks.py
tests/control/assets/test_asset_grant_authorization.py
tests/control/assets/test_schema_service.py
tests/control/test_access_identity.py
tests/control/test_browser_sessions.py
tests/control/test_live_config_admission.py
tests/integration/test_recovery_upgrade.py
tests/interfaces/cli/maintenance/test_maintenance_cli.py
tests/interfaces/http/assets/test_schema_api.py
tests/interfaces/http/identity/test_actor_auth.py
tests/plugins/test_delta_format_plugin.py
tests/plugins/test_public_catalog_adapter.py
tests/plugins/test_public_format_adapter.py
tests/policy/test_access_models.py
tests/policy/test_null_default_policy.py
tests/policy/test_policy_resolution.py
tests/policy/test_row_filters.py
tests/read/execution/test_duckdb_filters.py
tests/read/execution/test_duckdb_masks.py
tests/read/execution/test_duckdb_streaming.py
tests/read/planning/test_planning_admission.py
tests/read/planning/test_planning_filters.py
tests/read/planning/test_planning_projection.py
tests/read/test_fetch.py
tests/read/test_live_request_context.py
tests/read/test_query_models.py
tests/read/tickets/test_scan_envelopes.py
tests/read/tickets/test_tickets.py
tests/sources/test_catalog_registry.py
tests/storage/test_live_config_store.py
tests/support/flight/flight.py
tests/support/identity.py
tests/support/qualification/build_connector_fixture.py
tests/support/read/read_scenarios.py
tests/support/read/reads.py
tests/support/read/use_cases.py
tests/test_e2e_smoke.py
```


## T02: isolate compiler and admission

Status: complete. T01 acceptance verified before work.

DuckDBProjectionCompiler is the only physical compiler. CompiledProjection and
GovernedReadPlan live in kernel plans; output_schema derives from the compiled
value. ReadService.schema and plan share admit_read. ReadService requires an
injected compiler, and preview composition supplies the same port. The transform
accepts a compiled projection and never compiles again. Policy decisions remain
in kernel/policy/authorization.py; orchestration lives in application/reads.

Commands and results:

- `uv run --no-sync pytest tests/policy tests/read/planning tests/read/execution tests/control/policies -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t02-before.log 2>&1`: exit 0, 164 passed, 2 deselected in 0.48 seconds.
- `uv run --no-sync pytest tests/read/planning/test_planning_projection.py::test_schema_plan_and_fetch_use_the_injected_compiled_projection -o pythonpath= > /tmp/dal-architecture-t02-red.log 2>&1`: exit 1, expected missing CompiledProjection import before implementation. The test then passed in the focused and full suites. It asserts exact supplied output schema and fetched literal values, so recompilation by the transform would fail it.
- `uv run --no-sync python /tmp/dal-architecture-t02-move.py` and `uv run --no-sync python /tmp/dal-architecture-t02-callers.py`: exit 0. Only tracked callers and explicit new modules were edited.
- Initial `ruff check --fix` exited 1 on unformatted generated edits and an import before a module docstring. The docstring placement was repaired; `uv run --no-sync ruff format . && uv run --no-sync ruff check --fix . && uv run --no-sync ty check` then exited 0.
- `uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura`: exit 0; wheel rebuilt before tests.
- `uv run --no-sync pytest tests/policy tests/read/planning tests/read/execution tests/control/policies tests/interfaces/http/assets -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t02-focused.log 2>&1`: exit 0, 209 passed, 8 deselected in 6.70 seconds.
- `uv run --no-sync ruff check .`, `uv run --no-sync ruff format --check .`, `uv run --no-sync ty check`: exit 0 each, 391 files formatted.
- `uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t02-q1.log 2>&1`: exit 0, 1210 passed, 115 deselected in 74.56 seconds.
- `uv run --no-sync pytest tests/read/execution/test_duckdb_streaming.py::test_duckdb_transform_memory_is_bounded_in_subprocess -o pythonpath= > /tmp/dal-architecture-t02-memory.log 2>&1`: exit 0, both payload cases passed without marker exclusions in 4.95 seconds.
- Compiler AST audit: exit 0, all 20 helpers and the compile method body match baseline after renaming the returned value type. Kernel imports remain free of concrete adapters, including nested/type-only imports.
- `uv run --no-sync python scripts/export_control_plane_openapi.py --check`: exit 0. Retired-symbol/path search found no matches outside delivery records. `git diff --check`: exit 0.
- `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index`: exit 0, 10 added, 5 deleted, 21 reprocessed, zero indexing errors.

Moved paths: policy/projection.py to adapters/duckdb/compiler.py,
read/execution/connection.py to adapters/duckdb/connection.py and read/contracts.py
to application/reads/contracts.py. Removed policy/admission.py and its now-vacant
package marker. New paths: application/reads/admission.py, application/reads/ports.py,
kernel/plans/governed.py and package markers for application, application/reads,
adapters and adapters/duckdb. Modified kernel/plans/projection.py and
kernel/policy/authorization.py. All paths are under src/dal_obscura.

Other changed production paths: read/service.py, read/execution/contracts.py,
read/execution/transform.py, control/policies/evaluation.py, interfaces/cli/read.py,
interfaces/http/app.py, interfaces/http/routes/deps.py and
interfaces/http/routes/policies.py. Direct caller changes are in
 tests/support/read/reads.py, tests/support/read/use_cases.py,
tests/interfaces/http/assets/test_schema_api.py,
tests/read/planning/test_planning_projection.py,
tests/read/execution/test_duckdb_filters.py,
tests/read/execution/test_duckdb_masks.py,
tests/read/execution/test_duckdb_streaming.py and
tests/benchmarks/test_masking_row_filter_benchmarks.py.

Existing exact Arrow/schema/mask assertions remain. Benchmark inputs, assertions
and repetition counts are unchanged; compilation stays inside each timed run,
now before the transform call. Final diff review found no second compiler,
compatibility wrapper or persisted/public format change. Remaining temporary
old-layout edge from application/reads/admission.py to sources/access.py is
removed by T03's source-contract merge. Existing read/ticket/service/source
and governance SQL/composition edges remain assigned to T03 through T08.


## T03: separate read application and providers

Status: complete. T02 acceptance verified before work.

Merged runtime source, ScanTask, Plan and AccessContext contracts into
application/reads/sources.py. Read service, ticket contracts/payload and stream
ownership now live in application/reads. Identity requests/errors moved to kernel;
IdentityPort and ScanTaskCodec live in read ports. Concrete providers, task codec,
DuckDB transform/limits and HMAC signing moved to adapters. Provider constructors
require their configured registry; test and production composition supplies it.

Commands and results:

- `uv run --no-sync pytest tests/read tests/sources tests/storage/test_ticket_store_sqlalchemy.py tests/interfaces -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t03-before.log 2>&1`: exit 0, 512 passed, 49 deselected in 25.60 seconds.
- `uv run --no-sync python /tmp/dal-architecture-t03-move.py`: exit 0. Initial lint exited 1 on line wrapping; formatting fixed it. The first type check found one optional test-composition registry argument. The test factory now constructs the startup registry explicitly when absent.
- `uv run --no-sync ruff check . && uv run --no-sync ruff format --check . && uv run --no-sync ty check && uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura`: exit 0; 387 files formatted, service wheel rebuilt before tests.
- Focused command above with output `/tmp/dal-architecture-t03-focused.log`: exit 0, 512 passed, 49 deselected in 27.00 seconds.
- `uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t03-q1.log 2>&1`: exit 0, 1210 passed, 115 deselected in 73.48 seconds. Signed-ticket, stored-payload and scan-envelope fixtures are included.
- Temporary AST dependency audit: exit 0. Every application/reads import, including nested and type-only imports, points to that owner or kernel. No concrete adapter remains in read application code.
- `uv run --no-sync python scripts/export_control_plane_openapi.py --check`: exit 0. Retired read/source/identity-contract import search: no matches outside delivery records. `git diff --check`: exit 0.
- `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index`: exit 0, 18 added, 22 deleted, 64 reprocessed, zero errors.

No test paths, ticket fields, hash ordering, signatures, exchange counts or scan
decoding rules changed. Final diff review preserved stream behavior and exception
classification. Remaining provider-to-SQL snapshot imports belong to T04;
control SQL ownership and startup/session composition remain T05 through T08.
The application read owner has no temporary dependency exemptions.

T03 module changes (paths relative to src/dal_obscura):

```text
identity/contracts.py -> kernel/identity/authentication.py, application/reads/ports.py
read/__init__.py -> application/reads/__init__.py
read/execution/__init__.py -> (removed)
read/execution/contracts.py -> application/reads/ports.py, application/reads/execution.py
read/execution/limits.py -> adapters/duckdb/limits.py
read/execution/stream.py -> application/reads/execution.py
read/execution/transform.py -> adapters/duckdb/transform.py
read/service.py -> application/reads/service.py
read/tickets/__init__.py -> application/reads/tickets/__init__.py
read/tickets/contracts.py -> application/reads/tickets/contracts.py
read/tickets/envelope.py -> application/reads/ports.py, adapters/providers/task_codec.py
read/tickets/payload.py -> application/reads/tickets/payload.py
read/tickets/signing.py -> adapters/tickets/signing.py
sources/__init__.py -> adapters/providers/__init__.py
sources/access.py -> application/reads/sources.py
sources/adapters.py -> adapters/providers/adapters.py
sources/catalogs.py -> adapters/providers/catalogs.py
sources/contracts.py -> application/reads/sources.py
sources/discovery.py -> adapters/providers/discovery.py
sources/planning.py -> application/reads/sources.py
sources/published.py -> adapters/providers/published.py
sources/schema.py -> adapters/providers/schema.py
```

Other changed paths: application/reads/admission.py and ports.py, interfaces/cli/read.py, interfaces/flight/{contracts,server,streaming}.py, identity/{claims,http_session,oidc}.py, runtime/configuration.py, control/assets/{schema,service}.py, control/catalogs/service.py and control/policies/evaluation.py. Caller imports and explicit registry construction were updated in their existing tests/read, tests/sources, tests/interfaces, tests/control, tests/plugins, tests/storage, tests/acceptance, tests/benchmarks and tests/support owner files. The module map identifies every removed source; no forwarding files remain.


## T04: create immutable typed snapshots

Status: complete. T03 acceptance verified before work.

PublishedCatalog, PublishedSource, PublishedSchema and PublishedAssetSnapshot
replace LiveAsset/LiveCatalog and compiled_config. Options and ordered field
records detach recursively into read-only mappings and tuples. JSON methods
return fresh dictionaries/lists and reject unsupported objects. AssetPolicy is
converted once in SQL capture. Absence of schema remains None. The provider
registry accepts PublishedSnapshotReader, with no SQL snapshot import.
LiveRuntime moved to application source contracts without changing its fields
or refresh behavior. The unused get_asset convenience method and dictionary
conversion helpers were removed; all callers use one capture operation.

Commands and results:

- `uv run --no-sync pytest tests/storage tests/control/test_live_config_admission.py tests/read/test_live_request_context.py tests/sources -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t04-before.log 2>&1`: exit 0, 81 passed, 2 deselected in 1.32 seconds.
- The two new storage tests for nested option detachment and detached serialization ran before implementation with output `/tmp/dal-architecture-t04-red.log`: exit 1, both failed on missing kernel/plans/snapshot.py. Both pass in the focused/full suites after implementation.
- `docker run -d --rm --name dal-architecture-pg-3036-a13e55 --cidfile /tmp/dal-architecture-pg-3036-a13e55.cid -e POSTGRES_PASSWORD=architecture-test-only -e POSTGRES_DB=architecture -p 127.0.0.1::5432 docker.io/library/postgres:17.10-alpine`: exit 0. This created an isolated test container with no existing volume. `docker port` and `pg_isready` exited 0; loopback port was 42817.
- `DAL_OBSCURA_POSTGRES_TEST_URL=postgresql+psycopg://postgres:architecture-test-only@127.0.0.1:42817/architecture uv run --no-sync pytest tests/integration/test_live_snapshot.py -o pythonpath= > /tmp/dal-architecture-t04-postgres-before.log 2>&1`: exit 0, 6 passed without marker exclusions in 0.78 seconds.
- `uv run --no-sync python /tmp/dal-architecture-t04-production.py` and `uv run --no-sync python /tmp/dal-architecture-t04-tests.py`: exit 0. SQL source moves, resource references, packaging and caller imports updated directly. No generated artifact was edited.
- Initial lint/type checks found fixture tuple construction and three boundary typing issues. Tuple literals replaced tuple(list literals). JSON mapping key validation now narrows each key explicitly. A path cast follows actual runtime string validation. AdmittedPluginSnapshot declares a Collection of identity keys because callers only test membership; both concrete PluginRegistry and existing test snapshots satisfy that port without importing concrete plugin types.
- `uv run --no-sync ruff check .`, `uv run --no-sync ruff format --check .`, `uv run --no-sync ty check`: exit 0 each, 388 files formatted.
- `uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura`: exit 0, service wheel rebuilt before tests.
- `uv run --no-sync pytest tests/storage tests/control/test_live_config_admission.py tests/control/assets/test_schema_evolution.py tests/read/test_live_request_context.py tests/sources -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t04-focused.log 2>&1`: exit 0, 85 passed, 2 deselected in 1.65 seconds.
- PostgreSQL command above with output `/tmp/dal-architecture-t04-postgres-after.log`: exit 0, 6 passed without marker exclusions in 0.73 seconds. It proves repeatable-read/read-only capture across a concurrent writer commit, absent schema preservation, and stale-resource CAS rejection.
- `uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t04-q1.log 2>&1`: exit 0, 1212 passed, 115 deselected in 73.44 seconds.
- Temporary audit: migration revision/DDL bytes exactly match baseline SHA256 `17b00df5259883936c9907d4544b4f197c83523cd99734f70a489d97a2ab83fb`. The snapshot transaction method has an identical AST. Kernel/read imports point inward. Installed migration resource and unchanged dal-obscura-migrate command resolve at their new SQL owner.
- `uv run --no-sync python scripts/export_control_plane_openapi.py --check` and `uv run --no-sync dal-obscura-migrate --help > /tmp/dal-architecture-t04-migrate-help.txt`: exit 0 each. Retired SQL/type/compiled_config search: no matches outside delivery records. `git diff --check`: exit 0.
- `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index`: exit 0, 18 added, 17 deleted, 58 reprocessed, zero errors.
- `docker stop dal-architecture-pg-3036-a13e55 && docker ps -a --filter name=dal-architecture-pg-3036-a13e55 --format '{{.Names}}'`: exit 0. Container removed; no matching container remains.

Changed paths: every T04 SQL move in module-map.json, including migrations and
package markers; kernel/plans/snapshot.py; application/reads/sources.py;
adapters/providers/published.py; pyproject.toml migration entry-point target;
all owning SQL import callers in src, tests, scripts and tracked examples.
Typed fixture/assertion updates are in tests/storage/test_live_config_store.py,
tests/control/test_live_config_admission.py,
tests/control/assets/test_schema_evolution.py,
tests/read/test_live_request_context.py and tests/integration/test_live_snapshot.py.
The corruption assertion now covers capture because policy conversion belongs at
that boundary. The rejected historical plugin identifier in its negative fixture
was restored to its original bytes rather than renamed as a module import.
No new table, migration revision, persisted protocol, provider identifier, wrapper
or runtime refresh behavior was introduced. Final diff review preserves existing
schema identities/digests and transaction-before-provider-IO evidence.


Prerequisite inspection: `docker --version` reports 5.6.2 (Podman-compatible CLI). `docker info --format json` and image listing exited 0. PostgreSQL 17.10-alpine is cached. The initial Docker-specific `.ServerVersion` template exited 125 because that field is not supported by this runtime; no service was changed.

## T05: establish governance persistence ports

Status: complete. The user approved the gate scope; all recorded T05 checks passed.

Acceptance update: the user explicitly approved applying T05's gate to the new
repository paths after the checkpoint was pushed. With the recorded passing
transaction, audit, boundary and Q1 checks, T05 is now complete. Legacy writers
were migrated in T06–T08; no gate or required qualification was discarded.
The blocked-state narrative below records the earlier checkpoint accurately.

### Earlier checkpoint record

Checkpoint status: acceptance blocked pending clarification. T04 acceptance verified before
work. Independent implementation and checks are complete; T05 is not marked
complete, and T06 through T14 have not started.

### Concrete acceptance conflict

README.md line 311 says, "Done when repository methods never commit and all SQL
writes are controlled by an explicit unit of work." T06/T07 separately schedule
legacy governance-command migration, and T07 line 342 schedules deletion of
ControlContext and ControlPlaneDeps.with_transaction. The current wrapper still
commits raw SQLAlchemy sessions. Identity database methods remain scheduled for
T08, and the read-ticket SQL adapter owns its existing atomic commits.
Applying the broad gate literally in T05 requires moving later-task work into
T05 or adding a temporary bridge outside the fixed dependency rules. Neither
change was assumed. Clarification was requested on whether T05's gate covers the
new repository paths while those known legacy paths remain tracked. No answer
has been received. No legacy transaction owner was replaced, and no dependent
task was started. This is a scope/gate conflict, not a failed test or missing
external service.

### Independent work completed

Added the exact 32 repository methods from the inventory and
GovernanceUnitOfWork/GovernanceUnitOfWorkFactory ports. SQL repositories bind the
owning active session and never commit. SqlGovernanceUnitOfWork owns commit,
rollback and closure, rejects calls after exit, and preserves primary errors if
cleanup fails. Workspace ORM results became immutable WorkspaceState values;
asset/audit page values moved to application contracts. Existing SQL queries,
locks, CAS predicates and ordering remain in their adapters.

Control errors moved to application/governance/errors.py. HTTP request context
moved to interfaces/http/request_context.py. Persistence no longer imports HTTP
request context. Routes pass correlation IDs explicitly through the existing
legacy service signatures into audit persistence. HTTP behavior and request-ID
generation remain unchanged.

### Commands and results

- `uv run --no-sync pytest tests/storage tests/control/test_audit_repository.py tests/control/test_structured_conflicts.py -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t05-before.log 2>&1`: exit 0, 31 passed, 2 deselected in 0.44 seconds.
- `uv run --no-sync pytest tests/storage/test_governance_unit_of_work.py -o pythonpath= > /tmp/dal-architecture-t05-red.log 2>&1`: exit 2, expected collection error because the new unit-of-work module did not exist. The six resulting cases now pass: commit, absent commit, body failure, commit failure, commit plus close failure, and use after exit. All use migrated disposable SQLite storage; session instrumentation only injects commit/close failures.
- `uv run --no-sync python /tmp/dal-architecture-t05-persistence.py`: exit 0. Added ports/SQL implementations, moved values/errors/request context, and passed audit metadata explicitly. Its initial lint command exited 1 on formatting. A first comment-preservation probe exited 1 because it selected an existing audit-query filter parameter; it was corrected to select only audit-writing functions.
- `uv run --no-sync python /tmp/dal-architecture-t05-preserve-comments.py && uv run --no-sync ruff format . && uv run --no-sync ruff check . && uv run --no-sync ty check`: exit 0. The probe proves audited legacy function bodies match baseline after removing only the new metadata parameter/argument and preserves their original comments.
- `uv run --no-sync ruff format tests/storage/test_governance_unit_of_work.py && uv run --no-sync ruff check . && uv run --no-sync ruff format --check . && uv run --no-sync ty check && uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura`: exit 0, 393 files formatted and service wheel rebuilt before tests. An earlier attempt stopped at one unformatted type annotation; no tests ran on stale wheels.
- `uv run --no-sync pytest tests/storage tests/control/test_audit_repository.py tests/control/test_structured_conflicts.py tests/interfaces/http -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t05-focused.log 2>&1`: exit 0, 240 passed, 17 deselected in 24.39 seconds. Existing HTTP tests verify persisted policy audit correlation equals the response request ID.
- `uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t05-q1.log 2>&1`: exit 1, 1217 passed, 1 failed, 115 deselected in 73.42 seconds. The sole failure expected a call without the newly explicit correlation_id=None. The expectation was updated to include that exact metadata value; the lock/authorization/write ordering assertion was preserved.
- `uv run --no-sync pytest tests/control/assets/test_asset_grant_authorization.py tests/storage/test_governance_unit_of_work.py tests/interfaces/http/workspace/test_audit_api.py -o pythonpath= > /tmp/dal-architecture-t05-caller.log 2>&1`: exit 0, 11 passed in 0.86 seconds.
- Full Q1 command above with output `/tmp/dal-architecture-t05-q1-final.log`: exit 0, 1218 passed, 115 deselected in 72.25 seconds.
- `uv run --no-sync python scripts/export_control_plane_openapi.py --check`, `uv run --no-sync ruff check .`, `uv run --no-sync ruff format --check .`, `uv run --no-sync ty check`, `git diff --check`: exit 0 each.
- Temporary AST audit: exit 0, all 32 protocol and SQL repository method sets match the inventory, repository classes never commit, governance contracts import no SQL/framework/concrete adapters, and SQL audit contains no current_request_id dependency. A first temporary probe had a syntax typo and exited 1 before running; the corrected probe passed.
- `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index`: exit 0, 7 added, 2 deleted, 38 reprocessed, zero errors. Final refresh after the caller expectation update also exited 0: 3 reprocessed, 613 unchanged, zero errors.

### Changed paths and remaining work

New paths: application/governance/{__init__,ports,values}.py,
adapters/sql/unit_of_work.py and tests/storage/test_governance_unit_of_work.py.
Moved control/errors.py to application/governance/errors.py and control/context.py
to interfaces/http/request_context.py. Modified adapters/sql/{assets,audit,
workspace}.py for detached values and explicit metadata; SQL error imports now
point to application contracts. Updated the existing auditing service methods in
control/{assets,policies,catalogs,workspace}/service.py and control/plugins.py,
their HTTP route callers, and direct error/context import callers.
The grant lock-order test now expects explicit correlation_id=None.
Production paths above are relative to src/dal_obscura; test paths are relative
to the repository root.

No test or qualification was silently waived. Q1 intentionally excludes the
listed heavy/integration/socket markers. T04's PostgreSQL lane passed separately.
T06 through T14, including Q2, final no-skips Q3, JVM, installed-release, separate
S3-worker and live SSO qualification, remain pending behind this acceptance gate.
Q4 after measurements also remain pending. No performance improvement is claimed.
All task-created test containers/processes have been closed. Existing AGENTS.md
and ignored example build edits are preserved. No commit, push, merge or deploy
was performed.

## T06: migrate assets and policies

Status: complete. T05 acceptance scope explicitly approved by the user.

### Implementation and commands so far

AssetsCommands and PoliciesCommands now own their SQL units of work and the
capture/IO/completion phases. Asset/policy handlers dispatch once and serialize
plain values. The identity-attribute handler moved unchanged to the workspace
route module, its inventory owner, retaining its name and URL. Workspace command
migration remains T07. Shared catalog option/capture validation moved to its
application owner because T06 schema admission needs it; legacy catalog callers
use that same implementation. No catalog command or persistent schema changed.
Provider schema IO and synthetic claim mapping use injected ports. Preview's
policy helpers are separate from command orchestration to avoid an import cycle.
Legacy audit/attribute callers bind SqlAssetsRepository until their T07 migration.

- `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc search --refresh --limit 5 policy audit failure rollback revision schema discovery`: exit 0, index refreshed with zero errors; existing race coverage located.
- `uv run --no-sync pytest tests/control/assets tests/control/policies tests/interfaces/http tests/storage -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t06-before.log 2>&1`: exit 0, 297 passed, 17 deselected in 23.76 seconds.
- `uv run --no-sync python /tmp/dal-architecture-t06-move.py`: exit 1 at an attempted removal of a directory containing preserved Python cache files. No ignored files were removed. Resumed the remaining code via `uv run --no-sync python -` after removing that unnecessary directory-removal step: exit 0.
- Initial new transaction-test red run, `/tmp/dal-architecture-t06-red.log`: exit 2 on the missing installed command module. This occurred before the affected wheel was rebuilt and is not relied upon as installed-code evidence.
- `uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura > /tmp/dal-architecture-t06-red-install.log 2>&1`: exit 0.
- `uv run --no-sync pytest tests/control/policies/test_policy_transactions.py -o pythonpath= > /tmp/dal-architecture-t06-red-installed.log 2>&1`: exit 2, expected ImportError for missing PoliciesCommands in the freshly installed wheel, before command implementation.
- `uv run --no-sync python /tmp/dal-architecture-t06-commands.py`: exit 0. Initial lint/type probes failed on formatting, moved option constants/imports and unconverted callers; their output is `/tmp/dal-architecture-t06-lint-initial.log` and `/tmp/dal-architecture-t06-types-initial.log`.
- Automatic approval review rejected the proposed bulk caller script because truncating test files at fixture markers risked losing coverage. That script never ran. Replaced it with targeted production and test-seam edits that preserve all test functions and assertions; no approval bypass or gate waiver was used.
- `uv run --no-sync python /tmp/dal-architecture-t06-production-callers.py` and `uv run --no-sync python /tmp/dal-architecture-t06-test-seams.py`: exit 0. Type checks still identified the obsolete private-route test import and remaining fixture inputs. Updated that test to the command entry point while preserving its exact lock/authorize/write ordering assertion.
- `uv run --no-sync python /tmp/dal-architecture-t06-fixture-ports.py` and `uv run --no-sync python /tmp/dal-architecture-t06-fixture-uow.py`: exit 0. The subsequent format/type probes caught an indentation error in one fixture migration; fixed before reinstalling/testing. Every existing test and assertion was retained.
- `uv run --no-sync ty check`: exit 0 after caller fixes. `uv run --no-sync ruff format src tests scripts`: exit 0; the following lint probe found four unused/import-order issues in the real-provider fixture, still to fix.
- `uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura > /tmp/dal-architecture-t06-install-1.log 2>&1`: exit 0, before the migrated focused run.
- Static inspection: asset/policy route files contain no Session, with_transaction, lambda or SQL repository references; application governance contains no concrete adapter, SQLAlchemy, HTTP framework or legacy control imports.
- `docker run --rm -d --name dal-architecture-t06-pg-3036 -e POSTGRES_PASSWORD=architecture-test-only -e POSTGRES_DB=architecture -p 127.0.0.1::5432 docker.io/library/postgres:17.10-alpine`: exit 0. Task-only container, no persistent volumes; cleanup required after race checks.

### Acceptance evidence

- Focused command above, output `/tmp/dal-architecture-t06-focused-1.log`: exit 0, 299 passed, 17 deselected in 24.49 seconds.
- `docker port dal-architecture-t06-pg-3036 5432/tcp` and `docker exec dal-architecture-t06-pg-3036 pg_isready -U postgres -d architecture`: exit 0; disposable loopback port 34037, ready.
- `DAL_OBSCURA_POSTGRES_TEST_URL=postgresql+psycopg://postgres:architecture-test-only@127.0.0.1:34037/architecture uv run --no-sync pytest tests/interfaces/http/assets/test_schema_api.py tests/integration/test_live_snapshot.py -o pythonpath= > /tmp/dal-architecture-t06-postgres.log 2>&1`: exit 0, 28 passed in 4.73 seconds, no marker exclusions or skips. Provider-controlled races retain catalog/schema revision, policy revision, read-capability and identity-mapping fences.
- `PYTHONPATH=/tmp uv run --no-sync pytest 'tests/control/policies/test_policy_transactions.py::test_failed_policy_command_preserves_policy_ticket_and_audit[audit]' -o pythonpath= -p dal_t06_early_commit_probe > /tmp/dal-architecture-t06-falsify.log 2>&1`: exit 1, expected assertion failure on changed persisted policy when a process-only mutation commits before audit. The temporary plugin changed no source files and was not loaded in subsequent runs. This proves the new rollback test detects an early-commit regression.
- `uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t06-q1.log 2>&1`: exit 0, 1220 passed, 115 deselected in 73.74 seconds.
- Final review removed three obsolete SQL-query monkeypatch fixtures through exact patches after confirming they contained no tests or assertions. Removed the unused policy-list helper after tracing all callers; preview uses the repository directly. Used the captured schema value's owning import directly.
- `uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura > /tmp/dal-architecture-t06-install-final.log 2>&1`: exit 0 after final source cleanup.
- Final focused command above, output `/tmp/dal-architecture-t06-focused-final.log`: exit 0, 299 passed, 17 deselected in 24.42 seconds. Final Q1 command above, output `/tmp/dal-architecture-t06-q1-final.log`: exit 0, 1220 passed, 115 deselected in 72.25 seconds.
- Final `uv run --no-sync ruff check .`, `uv run --no-sync ruff format --check .`, `uv run --no-sync ty check`, `uv run --no-sync python scripts/export_control_plane_openapi.py --check`, and `git diff --check`: exit 0 each; 398 files formatted. Automatic approval review initially could not execute the quality command because of an account usage limit; after the user's continuation request the authorized retry succeeded. No failed review was bypassed.
- Temporary AST audit via `uv run --no-sync python -`: exit 0. All ten AssetsCommands and three PoliciesCommands public methods exactly match command_surface and require keyword-only actor/correlation_id. Application governance has no concrete/SQL/framework/legacy-control import. Every pre-existing changed test function remains present.

Changed ownership: control/assets/{access,schema,service}.py and
control/policies/{compiler,evaluation,service}.py moved to the mapped application
features. Pure preview helpers live in policies/preview.py; shared catalog
validation/capture helpers live in application/governance/catalogs/service.py.
Added adapter providers/governance.py, schema/identity ports and capture values.
Updated HTTP asset/policy/workspace routes, app composition and dependency bundle,
legacy audit/attribute repository bindings, schema benchmark import/patch targets,
and all test/real-provider fixture callers. Added two persisted-state transaction
cases in tests/control/policies/test_policy_transactions.py. No migration, lockfile,
public request/response shape, operation ID, stored identifier or ticket format
changed. Legacy ControlContext and session SQL remain explicitly owned by T07/T08.
Final no-skips, UI, JVM, separate-worker S3, live SSO and after-performance lanes
remain T13/T14 work; no performance improvement or full qualification is claimed.

## T07: migrate remaining governance

Status: complete. T06 acceptance checks passed.

### Implementation

Implemented CatalogsCommands, WorkspaceCommands, AuditQueries and PluginCommands
with required keyword-only actor/correlation_id. Catalog commands own capture,
provider IO and revision completion. Publication preserves one transaction and
sorted catalog lock order. Cursor encryption, scope, lifetime and limits are
unchanged; per-session provider admission moved intact to the provider adapter.
Plugin operations use the same startup registry through an application port;
the concrete lifecycle enum conversion belongs to the adapter. No request-time
plugin discovery was introduced.

AuditQueries calls its business filter correlation_filter so correlation_id can
carry the required request metadata. HTTP still accepts exactly correlation_id
as the query parameter. Workspace attribute previews use a synthetic mapper
port. The existing descriptive-attribute definition validator moved unchanged
to kernel/identity/attributes.py so settings validation and identity mapping
share one pure implementation; stored OIDC implementation paths remain stable.

Every governance route now dispatches once, using plain business inputs and its
existing response model. Removed ControlPlaneDeps.with_transaction and traced
ControlContext callers before deleting control/runtime.py and the unused
tests/support/control/seed.py helpers. The old control implementation tree now
contains no Python source. Session/authentication SQL remains scheduled for T08.

### Commands and results so far

- `uv run --no-sync pytest tests/control tests/interfaces/http tests/plugins tests/storage -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t07-before.log 2>&1`: exit 0, 470 passed, 29 deselected in 24.93 seconds.
- `uv run --no-sync python /tmp/dal-architecture-t07-boundaries.py`, `uv run --no-sync python /tmp/dal-architecture-t07-commands.py` and `uv run --no-sync python /tmp/dal-architecture-t07-routes.py`: exit 0 each. Initial lint/type probes failed on a missing capture-value import, newly required actor dependency syntax and moved test seams; output `/tmp/dal-architecture-t07-lint-1.log` and `/tmp/dal-architecture-t07-types-1.log`.
- `uv run --no-sync python /tmp/dal-architecture-t07-callers.py`: exit 0. Follow-up lint and type checks exited 0, recorded in `/tmp/dal-architecture-t07-lint-2.log` and `/tmp/dal-architecture-t07-types-2.log`.
- `uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura > /tmp/dal-architecture-t07-install-1.log 2>&1`: exit 0 before the first migrated focused run.
- Focused command above, `/tmp/dal-architecture-t07-focused-1.log`: exit 1, 443 passed, 27 failed, 29 deselected in 26.35 seconds. All failures traced to new closure-local FastAPI dependency expressions inside deferred annotations; affected routes saw actor as a query input. OpenAPI export also exited 1 on the same unresolved ForwardRef. No assertions or contracts were relaxed.
- `uv run --no-sync python /tmp/dal-architecture-t07-auth-fix.py`: exit 0. Bound the dependency object in each router and used it as the default, preserving ordinary global actor annotations. Retained publication helper keyword arguments so the existing provider-controlled concurrency barrier still observes the real writes. Lint/format/type checks passed after this correction.
- Reinstall command above, `/tmp/dal-architecture-t07-install-2.log`: exit 0. Focused command above, `/tmp/dal-architecture-t07-focused-2.log`: exit 0, 470 passed, 29 deselected in 26.23 seconds.
- `DAL_OBSCURA_POSTGRES_TEST_URL=postgresql+psycopg://postgres:architecture-test-only@127.0.0.1:34037/architecture uv run --no-sync pytest tests/interfaces/http/catalogs tests/integration/test_live_snapshot.py -o pythonpath= > /tmp/dal-architecture-t07-postgres.log 2>&1`: exit 0, 53 passed in 7.99 seconds, no marker exclusions or skips. Used the same task-owned disposable container as T06; publication visibility, rollback, stale fences and configuration snapshot races passed.
- `uv run --no-sync python scripts/export_control_plane_openapi.py --check`: exit 0 after the dependency fix. Lint, format and type checks also passed; 395 formatted files at this point.
- Final review removed unused provider/secret/egress fields from ControlPlaneDeps; current app composition still supplies those settings to the owning commands/adapters. Final lint/type, wheel reinstall, focused and Q1 commands are running with output `/tmp/dal-architecture-t07-{lint,types,install,focused}-final.log` and `/tmp/dal-architecture-t07-q1.log`. No completion is claimed before their results.

Changed paths: mapped application/governance/{catalogs,workspace,audit_service.py,
plugins.py}; adapters/providers/governance.py and plugin_control.py; application
ports; kernel/identity/attributes.py and identity/claims.py; all governance HTTP
routes and composition/dependency bundle; operator CLI, schema contract and test
imports/seams. Removed all remaining source files in control/, including its
package marker and runtime.py. No test function was deleted or renamed.

### Acceptance evidence

Final focused command above, `/tmp/dal-architecture-t07-focused-final.log`: exit 0,
470 passed, 29 deselected in 25.82 seconds. Final Q1 command above,
`/tmp/dal-architecture-t07-q1.log`: exit 0, 1220 passed, 115 deselected in 75.22
seconds. Final lint, format and type checks passed before wheel reinstallation;
OpenAPI export and `git diff --check` also passed. Temporary AST audit via
`uv run --no-sync python -`: exit 0, all 30 inventory commands match, every public
command requires actor/correlation_id, inward governance imports pass, all
governance handlers dispatch without transaction lambdas or SQL imports, the old
control source tree is empty, and all pre-existing test functions remain.

`COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index` was run after T06 and again after
T07 with output `/tmp/dal-architecture-t06-index.log` and
`/tmp/dal-architecture-t07-index.log`. T06 refresh exited 0; T07 refresh result
will be recorded after completion. Qualification lanes intentionally excluded by
Q1 remain T13/T14 requirements. No performance improvement is claimed.

## T08: centralize startup and sessions

Status: complete. T07 acceptance checks passed.

### Implementation and commands so far

Added bootstrap/control.py and bootstrap/read.py. HTTP create_app receives
constructed ControlPlaneDeps and a readiness callback; bootstrap builds the
operations, SQL factory, providers, registry and identity callbacks. Test
factories and OpenAPI export now use build_control_app, the same composition
path as HTTP startup. Public CLI entrypoint targets, help text and exit handling
remain intact. Flight TLS loading lives at interfaces/flight/tls.py.

SessionCommands owns the inventory's eight database methods. The SQL unit of
work binds the existing browser-session, login-transaction and rate-limit stores
through ports with the same public methods. Their queries/locks and the original
conditional commit on browser resolution are preserved. LoginTransaction,
LoginRateLimitDecision and resolver callable types live in kernel/identity/session.py.
HTTP retains cookies, CSRF/origin checks, verified-claim dispatch and redirect
responses; actor/config serialization moved out of the concrete identity module.
Stored OIDC implementation identifiers remain unchanged.

Read startup now owns stop/join for ticket cleanup and the health thread, closes
the health socket and registers Flight shutdown before dependency teardown.
The health listener loads its HTTP-only configuration synchronously, closing the
socket on configuration failure. No thread uses daemon=True or an unowned loop.

- `uv run --no-sync pytest tests/interfaces tests/identity tests/control/test_browser_sessions.py tests/runtime -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t08-before.log 2>&1`: exit 0, 391 passed, 48 deselected in 27.12 seconds.
- `uv run --no-sync pytest tests/interfaces/cli/read/test_worker_lifecycle.py -o pythonpath= > /tmp/dal-architecture-t08-red.log 2>&1`: exit 2, expected missing bootstrap module before implementing cleanup ownership. The new test performs real disposable SQL cleanup, waits on an explicit event, closes twice and checks no cleanup worker remains.
- `uv run --no-sync python /tmp/dal-architecture-t08-sessions.py` and `uv run --no-sync python /tmp/dal-architecture-t08-control-composition.py`: exit 0. Initial lint/type probes identified moved imports and caller seams plus the not-yet-created read bootstrap; output `/tmp/dal-architecture-t08-lint-1.log` and `/tmp/dal-architecture-t08-types-1.log`.
- `uv run --no-sync python /tmp/dal-architecture-t08-read-composition.py`: exit 0. Moved read construction and transport TLS helpers, added worker ownership, normalized relocated imports and updated CLI/test seams. Subsequent lint/type checks passed, `/tmp/dal-architecture-t08-lint-2.log` and `/tmp/dal-architecture-t08-types-2.log`.
- `uv run --no-sync ruff format --check .`, `uv run --no-sync ruff check .`, `uv run --no-sync ty check`: exit 0 each, 402 formatted files. `uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura > /tmp/dal-architecture-t08-install-1.log 2>&1`: exit 0 before tests.
- Focused command above, `/tmp/dal-architecture-t08-focused-1.log`: exit 1, 387 passed, 5 failed, 48 deselected in 28.27 seconds. All five failures patched the old app module's exchange_authorization_code seam. Updated that import to bootstrap, retaining every assertion.
- `uv run --no-sync pytest tests/interfaces/http/identity/test_oidc_login.py tests/interfaces/cli/read/test_worker_lifecycle.py tests/interfaces/flight/test_data_plane_health.py -o pythonpath= > /tmp/dal-architecture-t08-caller.log 2>&1`: exit 0, 45 passed in 2.42 seconds, including the health socket bind-failure contract.
- `uv run --no-sync python scripts/export_control_plane_openapi.py --check`: exit 0 after HTTP factory injection.
- `uv run --no-sync pytest tests/test_e2e_smoke.py -o pythonpath= > /tmp/dal-architecture-t08-startup.log 2>&1`: exit 0, 1 passed in 1.51 seconds, no exclusions. This starts the installed CLI and exercises real local Iceberg/Flight reads with disposable OIDC/JWKS setup. It is not the final live-SSO qualification lane.
- Added a successful health shutdown test to the existing owner suite. `uv run --no-sync pytest tests/interfaces/flight/test_data_plane_health.py -o pythonpath= > /tmp/dal-architecture-t08-health-owner.log 2>&1`: exit 1, 8 passed, 1 failed. The real worker failed during Uvicorn's deprecated WebSocket backend import under warnings-as-errors. The initial race diagnosis was corrected after inspecting the full traceback and installed Uvicorn source. Health serves HTTP only; set ws="none" and load configuration before starting the worker. No warning filter or test relaxation was added.
- Re-ran lint, format and types: exit 0 each. Reinstalled the affected wheel with the command above, output `/tmp/dal-architecture-t08-install-2.log`: exit 0. Ownership test results in `/tmp/dal-architecture-t08-owners.log` are pending inspection; T08 remains incomplete until all required checks pass.

T07 index refresh completed with exit 0: 9 added, 12 deleted, 21 reprocessed,
zero errors. Task-owned PostgreSQL container remains available for subsequent
qualification and must be stopped before delivery. Full live SSO, no-skips,
installed-release, JVM, UI and separate-worker S3 qualification remain required
before final completion; no external lane is silently waived.

### Acceptance evidence

- `uv run --no-sync pytest tests/interfaces/flight/test_data_plane_health.py tests/interfaces/cli/read/test_worker_lifecycle.py -o pythonpath= > /tmp/dal-architecture-t08-owners.log 2>&1`: exit 0, 10 passed in 1.68 seconds, no marker exclusions. Successful close joins workers, releases the bound health socket and tolerates repeated close. Startup configuration failure closes its socket synchronously.
- `uv run --no-sync python /tmp/dal-architecture-t08-audit.py > /tmp/dal-architecture-t08-audit.log 2>&1`: exit 0. All three SQL identity store class ASTs are identical to the checkpoint. All eight SessionCommands signatures and normalized method bodies, including conditional commits, match the original dependency methods. Moved serializers and read CLI help match their originals. HTTP factory/health/routes import no SQL or concrete adapter modules. Every pre-existing test function remains.
- Final focused command above, `/tmp/dal-architecture-t08-focused-final.log`: exit 0, 392 passed, 49 deselected in 28.26 seconds.
- `uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t08-q1.log 2>&1`: exit 0, 1221 passed, 116 deselected in 75.84 seconds.
- Final OpenAPI export, Ruff lint, Ruff format check, Ty and `git diff --check`: exit 0 each, 402 formatted files. Affected wheel was reinstalled before the final suites. No installed code was changed during those suites.
- `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index > /tmp/dal-architecture-t08-index.log 2>&1`: refresh started after verification; result to be recorded after completion.

Changed paths: added bootstrap/{__init__,control,read}.py, kernel/identity/session.py,
application/governance/sessions.py and interfaces/flight/tls.py. Updated unit-of-work
ports/SQL bindings, identity session/value imports, HTTP factory/health/dependencies
and session transport, both CLI modules, OpenAPI export and all direct construction
callers/tests. Added worker lifecycle test and successful health shutdown test in
their existing owner directories. Stored schema, identity identifiers, ticket/scan
formats, public HTTP operation IDs and CLI entrypoint targets remain unchanged.
T08's live-SSO-before-final-completion requirement remains an explicit T14 lane;
the local OIDC smoke test is not substituted for it.

## T09: supervise execution lifecycle

Status: complete. T08 acceptance checks passed.

### Implementation

ExecutionSupervisor/ExecutionLease now own lazy admission before source execute,
input schema checks, cooperative progress and idempotent teardown. ReadService
requires the injected owner; the Flight bootstrap constructs one supervisor for
its process. HTTP synthetic previews receive a supervised RowTransformPort from
their bootstrap, so removing the transform-local semaphore leaves no production
application caller outside admission. Direct physical-transform tests and
benchmarks remain distinct from application admission tests.

StreamGuard.check_progress checks cancellation, stream deadline, ticket expiry
and identity expiry before/after source pulls, without SQL. check_disclosure
performs those checks plus strict SQL ensure_active immediately before each
returned batch. Fetch takes optional keyword-only cancel_check; Flight passes
the installed context.is_cancelled method. Source execute's declared execution
schema, including metadata, governs actual input validation rather than the full
base schema. Existing compiled projection/filter semantics and byte/memory limits
remain unchanged. Closed leases release captured factories as well as resources.

Provider initialization ValueError/PermissionError retain their old Flight
classifications through explicit application subtypes. These distinguish errors
whose execution phase became lazy from already-lazy expiry/input failures. No
ticket, scan, HTTP or protobuf field changed, and no revocation cache was added.

### Commands and results

- `uv run --no-sync pytest tests/read tests/interfaces/flight tests/storage/test_ticket_store_sqlalchemy.py -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t09-before.log 2>&1`: exit 0, 219 passed, 32 deselected in 2.04 seconds.
- Initial zero-output regression attempt, `/tmp/dal-architecture-t09-red.log`: exit 1 on signer expiry because the fake execution clock differed from the real signer clock. This is not accepted as behavioral red evidence. A second attempt with the existing fixed-payload FakeTicketCodec was also invalid because it did not round-trip the newly planned reference. The test now uses real signing with an isolated signer-module clock matching its explicit fake execution clock.
- The first `/tmp/dal-architecture-t09-red-controlled-clock.log` probe exited 4 because the already-edited test builder imported the not-yet-installed supervisor. No result from these setup failures is used as qualification.
- `uv run --no-sync python /tmp/dal-architecture-t09-core.py`: exit 0. Initial lint/type probes caught future-import positioning and old admission constructor/import callers; output `/tmp/dal-architecture-t09-lint-1.log` and `/tmp/dal-architecture-t09-types-1.log`.
- `uv run --no-sync python /tmp/dal-architecture-t09-valid-red.py`: final exit 0. This checked and saved only the eight files containing this task's execution/composition edits, temporarily rebuilt the prior execution boundary, reinstalled the service wheel, and ran the public plan/fetch regression. `/tmp/dal-architecture-t09-red-valid.log`: exit 1 for the expected DID NOT RAISE TimeoutError when all rows were filtered. The script restored every implementation byte in finally and verified no concurrent changes were overwritten. An earlier probe correctly refused to accept its fixed-payload-codec Unauthorized failure and also restored all eight files. No unrelated work was changed.
- `uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura > /tmp/dal-architecture-t09-install-1.log 2>&1`: exit 0 after restoration. Public regression, `/tmp/dal-architecture-t09-green.log`: exit 0, 1 passed in 0.22 seconds.
- `uv run --no-sync python /tmp/dal-architecture-t09-callers.py`: exit 0. Updated the existing admission assertions to the shared owner without deleting/renaming tests. Lint/types passed after these caller changes.
- Reinstall command above, `/tmp/dal-architecture-t09-install-2.log`: exit 0. Focused command above, `/tmp/dal-architecture-t09-focused-1.log`: exit 0, 229 passed, 32 deselected in 2.08 seconds. Cases cover deferred source opening, capacity rejection before opening another source, four initialization failures, primary-error preservation, zero-output cancellation, exact input schema/metadata rejection, and SQL disclosure counts.
- `uv run --no-sync python /tmp/dal-architecture-t09-fetch-cases.py`: exit 0. Added public fetch cancellation/laziness and projected-execution-schema cases using the same controlled source-codec seam.
- `uv run --no-sync pytest tests/read/test_fetch.py tests/read/execution/test_stream_ownership.py tests/read/execution/test_duckdb_streaming.py -o pythonpath= > /tmp/dal-architecture-t09-owned.log 2>&1`: exit 1, 49 passed, 3 failed. Two failures came from an import replacement damaging the embedded subprocess script's indentation. The third new expectation incorrectly retained provider metadata in DuckDB's normalized output; the unchanged compiler explicitly strips that metadata. Corrected the script import and pinned the existing normalized output schema. Strict declared-input metadata validation remains separately asserted.
- Same owning command, `/tmp/dal-architecture-t09-owned-final.log`: exit 0, 52 passed in 4.54 seconds, including both subprocess memory probes without marker exclusions.
- `uv run --no-sync python /tmp/dal-architecture-lifecycle-after.py > /tmp/dal-architecture-lifecycle-after.json`: exit 0. Uses the same three one-row inputs, filter and advancing fake clock as the captured T00 probe. Before filtered expiry: 3 input pulls, 0 disclosure SQL, no error. After: 2 input pulls, 0 disclosure SQL, Ticket expired. Three visible inputs remain 3 output batches and exactly 3 SQL disclosure checks. This is deterministic lifecycle evidence, not a latency/RSS gain claim.
- Installed Arrow API probe via `uv run --no-sync python -`: ServerCallContext.is_cancelled and FlightCancelledError both exist.
- `uv run --no-sync python /tmp/dal-architecture-t09-flight-errors.py`: exit 0. Preserved initialization error mappings and retained original resource-failure assertions while sharing their native test harness. Reinstall command above, `/tmp/dal-architecture-t09-install-3.log`: exit 0 before real transport checks.
- First native Flight/CLI command, `/tmp/dal-architecture-t09-flight.log`: exit 2 after 20 passes because Arrow read_all converts ArrowCancelled into SIGINT. A focused `-vv --full-trace` rerun confirmed the installed implementation at pyarrow/_flight.pyx:1188 and error.pxi:264; it also exited 2. Changed only the new cancellation harness to bounded read_chunk calls, exposing FlightCancelledError directly. Existing read_all resource checks remain unchanged; no signal handlers or warning filters were altered.
- `uv run --no-sync pytest tests/interfaces/flight/test_service_streaming.py tests/test_e2e_smoke.py -o pythonpath= > /tmp/dal-architecture-t09-flight-final.log 2>&1`: exit 0, 25 passed in 1.87 seconds, no exclusions. Covers real cancellation/resource/initialization classifications and installed CLI Iceberg/Flight startup.
- Added two public deferred-provider-error cases proving original error category and slot release. Final focused command above, `/tmp/dal-architecture-t09-focused-final.log`: exit 0, 233 passed, 36 deselected in 2.18 seconds.
- `uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t09-q1.log 2>&1`: exit 0, 1235 passed, 120 deselected in 75.94 seconds. A preceding import-order lint failure was fixed; subsequent Ruff lint, format and Ty checks passed. Native/socket and memory lanes above are separate passing evidence, not replaced by Q1 exclusions.

Changed paths: application/reads/{execution,service}.py;
adapters/duckdb/transform.py; bootstrap/{read,control}.py; Flight server/streaming;
existing read composition helpers and admission/ownership/fetch/Flight tests.
The transform's StreamAdmissionError definition moved directly to the application
execution contract, with no alias. Input/output byte checks and DuckDB memory
limits remain intact. Exact schema/value and ticket-format tests passed.
Final review passed: one execution admission semaphore, supervised production
transform callers, required injected ReadService supervisor, and Flight cancellation
injection. AST audit using `uv run --no-sync python -` checked all application
imports for adapters/interfaces/bootstrap/SQLAlchemy and retained every existing
read/Flight test function; exit 0. No finding required another production edit.
`uv run --no-sync python scripts/export_control_plane_openapi.py --check`,
`uv run --no-sync ruff format --check .`, and `git diff --check` passed.
`COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index > /tmp/dal-architecture-t09-index.log 2>&1`
completed: 625 files listed, 15 reprocessed, 610 unchanged, zero errors.

## T10: own UI session lifecycle

Status: complete. T09 acceptance checks passed.

SessionBoundary now owns the private QueryClient, generation counter, request
registration/finish, session bootstrap/login/logout, public recovery options and
auth-expiry handling. Invalidating increments the ref before aborting or clearing
caches. Logout and expiry flush the generation-keyed subtree replacement before
calling the server. Workspace keeps presentation, navigation and current feature
workflows; main mounts providers, the boundary and Workspace. Session scope
encoding is unchanged. Private root continuations check their captured generation,
including error/finally and confirmation paths. Existing feature-local cancellation
and edit fences remain in place. No transport contract or generated type changed.

Commands and results:

- `PATH=/Users/nithish/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:$PATH pnpm --dir apps/governance-ui test:e2e e2e/identity/session-generation.spec.ts > /tmp/dal-architecture-t10-before.log 2>&1`: exit 1, two failures. Reinstalling the synthetic authenticated route fixture shadowed its bootstrap endpoint; these are fixture setup failures, not accepted regression-red evidence. Added an explicit bootstrap route after the replacement fixture.
- `uv run --no-sync python /tmp/dal-architecture-t10-workspace.py`: exit 0, extracted session ownership and added generation fences. `pnpm --dir apps/governance-ui check`, pinned PATH as above, `/tmp/dal-architecture-t10-check-1.log`: exit 0.
- `pnpm --dir apps/governance-ui test:e2e e2e/identity/auth-shell.spec.ts e2e/identity/session-generation.spec.ts e2e/shell`, pinned PATH, `/tmp/dal-architecture-t10-focused-1.log`: exit 0, 50 passed in 13.2 seconds. Includes old asset-response and new load/save crossing-identity cases.
- `pnpm --dir apps/governance-ui test`, pinned PATH, `/tmp/dal-architecture-t10-unit.log`: exit 0, 73 passed, zero skipped.
- `pnpm --dir apps/governance-ui test:stories`, pinned PATH, `/tmp/dal-architecture-t10-stories.log`: exit 1, 50 passed but three suites failed importing generated Storybook setup with SyntaxError: Illegal return statement. Repeating the identical command, `/tmp/dal-architecture-t10-stories-retry.log`: exit 0, all 12 files and 59 tests passed in 4.05 seconds. No dependency, warning or test exclusion changes were made.
- `pnpm --dir apps/governance-ui test:e2e`, pinned PATH, `/tmp/dal-architecture-t10-e2e.log`: exit 0, 109 passed in 25.9 seconds. Added explicit failed-logout recovery with a dirty draft, held revocation response and exact retry/signed-out notices.
- `pnpm --dir apps/governance-ui check` and `pnpm --dir apps/governance-ui check:api-types`, pinned PATH, `/tmp/dal-architecture-t10-check-final.log` and `/tmp/dal-architecture-t10-api-types.log`: both exit 0.
- `pnpm --dir apps/governance-ui build`, pinned PATH, `/tmp/dal-architecture-t10-build.log`: exit 0, all bundle budgets passed.
- `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index > /tmp/dal-architecture-t10-index.log 2>&1`: exit 0, 627 files listed, two added, three reprocessed, zero errors.
- Final review made the synchronous teardown explicit with flushSync and extracted Workspace.tsx so main is composition only. Final rechecks passed: `/tmp/dal-architecture-t10-e2e-final.log` exit 0,
109 passed in 25.7 seconds; `/tmp/dal-architecture-t10-check-final-2.log` and
`/tmp/dal-architecture-t10-build-final.log` exit 0. All Q2 commands passed.
Final code review found no remaining session ownership or stale-result issue. `git diff --check` passed; unrelated AGENTS.md remains untouched.

These browser routes are synthetic API tests and do not qualify live SSO, which
remains required at T14.

## T11: own asset and policy workflows

Status: complete. T10 acceptance checks passed.

AssetRoute owns four declarative queries with the original session/asset keys and
query-provided abort signals. Only that selected asset renders. Workspace receives
name/catalog presentation metadata through an explicit callback; no full response
or schema is copied into root state. The obsolete navigation callback for manual
asset hydration was removed after tracing its sole caller.

PolicyEditorController owns usePolicyDraft, save/revoke/preview, persona epoch,
request registration/finally cleanup and generation/mount guards. The reducer is
unchanged. It initializes through the existing load transition once per selected
asset; background query refreshes cannot erase a newer draft. Existing save payload
ordering, revision/edit fences, SQL/API behavior and notices are preserved. Dirty
policy/access state reaches Workspace through an explicit boolean callback.

Commands and results (all pnpm commands use
`PATH=/Users/nithish/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin:$PATH`):

- `pnpm --dir apps/governance-ui test:e2e e2e/assets e2e/policy e2e/shell/async-state.spec.ts > /tmp/dal-architecture-t11-before.log 2>&1`: exit 0, 42 passed in 14.1 seconds before production edits.
- `uv run --no-sync python /tmp/dal-architecture-t11-extract.py`: exit 0. Moved the existing workflow bodies and payloads to their feature owners, initialized through the same reducer, and replaced manual root hydration with declarative AssetRoute queries.
- `pnpm --dir apps/governance-ui check > /tmp/dal-architecture-t11-check-1.log 2>&1`: exit 0.
- `pnpm --dir apps/governance-ui test:e2e e2e/assets e2e/policy e2e/shell/async-state.spec.ts e2e/identity/session-generation.spec.ts > /tmp/dal-architecture-t11-focused-1.log 2>&1`: exit 0, 45 passed in 15.1 seconds.
- `pnpm --dir apps/governance-ui test:e2e e2e/policy/workflow-ownership.spec.ts > /tmp/dal-architecture-t11-workflows.log 2>&1`: exit 0, two passed in 3.9 seconds. Assertions include exact submitted rules/revision, newer local rule name, duplicate prevention, new submission using the advanced revision, and old preview isolation after navigation. Existing persona, stale-revision and failed-logout cases remain in the full suite.
- `uv run --no-sync python /tmp/dal-architecture-t11-falsify.py > /tmp/dal-architecture-t11-falsification.log 2>&1`: exit 0. Temporarily made successful save mark a newer edit saved. The owning browser test failed on Unsaved changes as required, `/tmp/dal-architecture-t11-falsified.log` exit 1. Finally restored the reducer byte-for-byte; no test assertions were weakened.
- `pnpm --dir apps/governance-ui test:e2e > /tmp/dal-architecture-t11-e2e.log 2>&1`: exit 0, 111 passed in 26.6 seconds.
- `pnpm --dir apps/governance-ui test > /tmp/dal-architecture-t11-unit.log 2>&1`: exit 0, 73 passed, zero skipped.
- `pnpm --dir apps/governance-ui test:stories > /tmp/dal-architecture-t11-stories.log 2>&1`: exit 0, 59 passed across all 12 files in 4.13 seconds.
- `pnpm --dir apps/governance-ui check > /tmp/dal-architecture-t11-check-final.log 2>&1`, `pnpm --dir apps/governance-ui check:api-types > /tmp/dal-architecture-t11-api-types.log 2>&1`, and `pnpm --dir apps/governance-ui build > /tmp/dal-architecture-t11-build.log 2>&1`: all exit 0; bundle budgets passed.
- Final diff review found no asset query or policy mutation call in app composition, no old manual hydration caller, and no reducer changes. `git diff --check` passed. `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index > /tmp/dal-architecture-t11-index.log 2>&1` refreshed the index; no Python wheel changed in T10/T11. Unrelated AGENTS.md is preserved.


## T12: own management queries

Status: complete. T11 acceptance checks passed.

Management queries now live in useActivityQueries, useConnectionsQueries and
useSettingsQueries with their result types. Activity alone owns audit filters,
summary/observations and cursor pagination. Applied filters stay in one existing
session-cache entry across navigation and clear on teardown. Connections/settings
use their unchanged scoped management keys and admin capability gates. Workspace
mounts each feature route and has no management query dispatcher or result merge.
The shared QueryView contains only the existing loading/retry presentation.
Activity stories now exercise the activity view directly and retain all scenarios.

Parser-based Node boundary tests inspect import declarations, re-exports,
ImportEquals declarations and literal dynamic imports. They inspect nested syntax
and type-only imports and include forbidden fixtures and allowed edges. Production
data cannot import app/features; features cannot import app composition;
connections/settings cannot import activity. Stories are independently typed and
qualified; production checking has no module exemptions.

Commands and results (pnpm uses the pinned Node PATH recorded under T10):

- `pnpm --dir apps/governance-ui test > /tmp/dal-architecture-t12-red.log 2>&1`: exit 1 before migration, rejecting the legacy dispatcher edges to features/activity/ActivityView and app/navigation/routes. Fixture parser cases passed. This is accepted boundary-red evidence.
- `uv run --no-sync python /tmp/dal-architecture-t12-hooks.py`: exit 0. Migrated query ownership and types, removed useManagementQueries and ManagementView, and retained existing view bodies and copy.
- `pnpm --dir apps/governance-ui check > /tmp/dal-architecture-t12-check-1.log 2>&1`: exit 0. `pnpm --dir apps/governance-ui test > /tmp/dal-architecture-t12-unit.log 2>&1`: exit 0, 75 passed, zero skipped, including both new AST boundary cases.
- `pnpm --dir apps/governance-ui test:e2e > /tmp/dal-architecture-t12-e2e.log 2>&1`: exit 0, 111 passed in 29.9 seconds. `pnpm --dir apps/governance-ui test:stories > /tmp/dal-architecture-t12-stories.log 2>&1`: exit 0, 59 passed across 12 files.
- Type and generated API checks, `/tmp/dal-architecture-t12-check-final.log` and `/tmp/dal-architecture-t12-api-types.log`: exit 0. `pnpm --dir apps/governance-ui build > /tmp/dal-architecture-t12-build.log 2>&1`: exit 1, JavaScript gzip 201.45 KiB exceeded the unchanged 200 KiB gate.
- Preserved the prior single lazy management payload through explicit Rollup chunk grouping, leaving source ownership separate and preventing implicit dependency capture. First build, `/tmp/dal-architecture-t12-build-chunks.log`: exit 1, 200.13 KiB. Including settings' existing AttributeMappings dependency in that same payload removed an unnecessary separate chunk. `/tmp/dal-architecture-t12-build-final.log`: exit 0, JavaScript gzip 199.8682 KiB, CSS gzip 43.7256 KiB, fonts 83.7227 KiB; no limits changed. This is bundle evidence, not a runtime performance claim.
- The next full browser check, `/tmp/dal-architecture-t12-e2e-final.log`: exit 1, 110 passed and the existing history/draft guard case timed out. `pnpm --dir apps/governance-ui test:e2e e2e/assets/asset-navigation.spec.ts --grep 'browser history' --repeat-each 5 > /tmp/dal-architecture-t12-history-red.log 2>&1`: exit 1, three failures and seven passes. The new policy-controller callback added an asynchronous effect hop before navigation learned of pending form edits.
- Fixed the owning callback to report form dirtiness synchronously, report reducer status in layout phase, and let navigation read the current explicit dirty boolean through a callback. Mount and generation guards remain; no test timeout, assertion or capability check changed. Same repeated command, `/tmp/dal-architecture-t12-history-green.log`: exit 0, 10 passed in 6.1 seconds.
- Final unit, story and type checks, `/tmp/dal-architecture-t12-unit-final.log`, `/tmp/dal-architecture-t12-stories-final.log` and `/tmp/dal-architecture-t12-types-final.log`: exit 0, 75 unit and 59 story tests passed. Added an exact applied-filter value assertion after navigation to the existing audit pagination browser test.
- `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index > /tmp/dal-architecture-t12-index.log 2>&1`: exit 0, 639 files listed, eight added, one deleted, six reprocessed, zero errors. Final full browser check `/tmp/dal-architecture-t12-e2e-qualified.log`: exit 0,
111 passed in 28.4 seconds. Final index command repeated to
`/tmp/dal-architecture-t12-index-final.log`: exit 0. Final review confirmed all
three feature owners, no shared data reverse imports, and no remaining legacy
dispatcher references outside delivery history. All Q2 gates passed. `git diff --check` passed.


## T13: enforce boundaries and remove obsolete paths

Status: complete. T12 acceptance checks passed.

Added Python AST dependency enforcement for kernel, application, concrete adapters
(including existing identity/plugins/runtime), interfaces and bootstrap composition.
The checker walks nested and type-only imports, resolves module imports from
packages and namespace packages, and inspects dynamic import expressions. The
startup provider identifier allowlist remains a separate existing contract. There
are no temporary module exemptions. The root version export is the unchanged
public transport metadata contract.

The gate exposed remaining CLI concrete operations. Maintenance's frozen count
value and atomic invalidation moved unchanged to adapters/sql/maintenance.py;
bootstrap owns engine lifetime and the retained CLI still parses/prints exactly
as before. Image subprocess admission/inspection and read environment composition
now run in bootstrap; CLI module paths and public command names remain intact.
Operator's redundant PluginAdmissionError catch/import was removed because the
same tuple already catches its ValueError base. Existing test functions and
assertions were retained, with monkeypatches redirected to the actual owner.

Architecture documentation now states the implemented dependency direction,
transaction/value ownership, input/disclosure checkpoints and UI generation/draft
fences. Current user-edited AGENTS.md already links the owning rules and enforces
the required workflow; it was read and preserved without overwriting that work.

Commands and results:

- `uv run --no-sync pytest tests/architecture/test_import_boundaries.py -o pythonpath= > /tmp/dal-architecture-t13-boundaries-red.log 2>&1`: exit 1, 18 fixture tests passed and the production graph rejected concrete CLI SQL/plugin/runtime edges.
- `uv run --no-sync pytest tests/interfaces/cli tests/runtime -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t13-cli-before.log 2>&1`: exit 0, 129 passed, three deselected in 2.61 seconds before the mechanical move.
- `uv run --no-sync python /tmp/dal-architecture-t13-cli.py`: exit 0. Initial Ruff found long test lines/import ordering/complexity; Ty found Sequence versus list at the existing inspection API. Split the checker by owner, formatted imports/lines, and preserved the original list input at that call. No lint/type exemption was added.
- `uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen --reinstall-package dal-obscura > /tmp/dal-architecture-t13-install-1.log 2>&1`: exit 0 before owning tests. First focused run, `/tmp/dal-architecture-t13-focused-1.log`: exit 0, 148 passed, three deselected in 2.85 seconds.
- Same reinstall command, `/tmp/dal-architecture-t13-install-2.log`: exit 0 after the final bootstrap argument fix. `uv run --no-sync pytest tests/architecture tests/interfaces/cli tests/runtime -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t13-focused-final.log 2>&1`: exit 0, 152 passed, three deselected in 2.94 seconds.
- Added namespace-parent fixtures during checker review. `/tmp/dal-architecture-t13-namespace-red.log`: exit 1, one expected failure and 22 passes; importing adapters through the root namespace was incorrectly classified as external. Resolved namespace parents from known descendants and classified the service root explicitly. `/tmp/dal-architecture-t13-boundaries-final.log`: exit 0, all 23 boundary tests passed. Fixtures also cover aliased dynamic imports and forbidden computed imports.
- `uv run --no-sync python /tmp/dal-architecture-t13-docs.py`: exit 0 after one apply_patch attempt failed atomically on mismatched context. No partial document mutation resulted from that failed patch.
- `uv run --no-sync ruff check .`, `uv run --no-sync ruff format --check .`, `uv run --no-sync ty check`, outputs `/tmp/dal-architecture-t13-{lint,format,types}-qualified.log`: all exit 0, 406 Python files formatted.
- `uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t13-q1.log 2>&1`: exit 0, 1258 passed, 120 deselected in 75.75 seconds.
- `uv run --no-sync python /tmp/dal-architecture-t13-map-audit.py > /tmp/dal-architecture-t13-map-audit.log 2>&1`: exit 0. All 126 entries verified: 50 retain, 60 move, two remove, nine split, five merge; all targets exist and all 75 retired sources are absent. Generated protocol digests match baseline and every existing modified Python test function remains.
- Retired-import search across tracked code, packages, scripts, Docker/configuration files and non-plan docs found only the deliberately rejected legacy provider identifier in test_live_config_admission.py. Architecture delivery records retain their historical paths intentionally.
- `uv build --wheel --out-dir /tmp/dal-architecture-t13-wheels > /tmp/dal-architecture-t13-wheel-build.log 2>&1`: exit 0. `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-release-3036-t13 uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen > /tmp/dal-architecture-t13-release-sync.log 2>&1`: exit 0, created a fresh environment. `uv pip install --python /tmp/dal-architecture-release-3036-t13/bin/python --no-deps --reinstall /tmp/dal-architecture-t13-wheels/dal_obscura-0.3.0-py3-none-any.whl > /tmp/dal-architecture-t13-release-install.log 2>&1`: exit 0, exact artifact installed.
- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-release-3036-t13 uv run --no-sync pytest tests/acceptance tests/storage/test_schema_migrations.py tests/interfaces/cli tests/read/tickets -o pythonpath= -m 'not heavy and not integration and not socket' > /tmp/dal-architecture-t13-release-tests.log 2>&1`: exit 0, 138 passed, five deselected in 13.20 seconds. This is installed-release evidence, not final no-skips qualification.
- Initial provenance probe failed because /tmp resolves to /private/tmp on macOS. Resolving both paths fixed the probe without changing the environment. `/tmp/dal-architecture-t13-release-resources.log`: exit 0; imports resolve inside the fresh environment, direct_url identifies the exact wheel archive (no editable directory), migration script template is packaged, current revision is 20261011_0001 and migration SHA256 remains 17b00df5259883936c9907d4544b4f197c83523cd99734f70a489d97a2ab83fb.
- With that fresh UV_PROJECT_ENVIRONMENT, `uv run --no-sync python scripts/export_control_plane_openapi.py --check` and public CLI help for dal-obscura, dal-obscura-migrate, dal-obscura-control-plane, dal-obscura-maintenance passed. The additional `dal-obscura serve --help` probe used a nonexistent subcommand and is not accepted as evidence; the read CLI help literal was independently verified unchanged, and its actual entrypoint is checked at T14. Outputs are `/tmp/dal-architecture-t13-openapi.log` and `/tmp/dal-architecture-t13-*-help.txt`.
- Full Q2, using the pinned Node PATH under T10: `pnpm --dir apps/governance-ui test`, `test:stories`, `test:e2e`, `check`, `check:api-types`, `build`; `/tmp/dal-architecture-t13-ui-{unit,stories,e2e,types,api-types,build}.log`: all exit 0. Results: 75 unit, 59 story, 111 browser tests (28.4 seconds); unchanged bundle budgets passed at 199.8682 KiB JS gzip.
- `uv run --no-sync python /tmp/dal-architecture-t13-final-audit.py > /tmp/dal-architecture-t13-final-audit.log 2>&1`: exit 0. Guidance links resolve; moved SQL operation/count class ASTs are identical; read help literal and packaged migration help match previous evidence. Final diff review found no compatibility path, protocol/schema change or new persistent subsystem.
- `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index > /tmp/dal-architecture-t13-index.log 2>&1`: exit 0, 642 files listed, four added, 11 reprocessed, zero errors. `git diff --check`: exit 0.


## T14: qualify and deliver

Status: complete. T13 acceptance and all required executable qualification gates passed. Measurement limits are stated below.

- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-release-3036-t13 DAL_OBSCURA_POSTGRES_TEST_URL=postgresql+psycopg://postgres:architecture-test-only@127.0.0.1:34037/architecture DAL_OBSCURA_RUN_CONSUMER_TESTS=1 uv run --no-sync python scripts/require_no_skips.py --junitxml /tmp/dal-architecture-python-after.xml -- uv run --no-sync pytest tests packages -o pythonpath= --ignore=tests/benchmarks > /tmp/dal-architecture-t14-q3.log 2>&1`: exit 0, 1368 passed in 245.05 seconds. No marker exclusions and zero skipped/failing/error cases in the JUnit audit. The only ignored directory is the plan's separately measured benchmarks.
- JUnit audit through `uv run --no-sync python -`: exit 0. All eleven cloud-object-store cases passed, including fresh-process restricted reads for Delta COW/MoR, Iceberg file/row-group COW and position/equality deletes, native Glue snapshot portability, and S3 path admission. Their existing independent expected rows, pinned snapshots and disjoint-read assertions remain intact. An HTTP S3 emulator runs in another process; these are real native-client IO and separately spawned task workers, not thread-pool-only qualification.
- `docker pull quay.io/keycloak/keycloak:26.7.4 > /tmp/dal-architecture-t14-keycloak-pull.log 2>&1`: exit 0. Prepared `/tmp/dal-architecture-t14-live.py` to use the existing realm, seed/provision/read-check and browser fixtures against exact installed CLIs. It creates generated private credentials outside Git, an isolated control database, local Iceberg catalog/warehouse, a disposable Keycloak container and owned host service processes. Cleanup terminates all owned processes, removes the container and drops only that generated database, including on failure. Existing example .env files and services are untouched.
- First live harness run, `/tmp/dal-architecture-t14-live.log`: exit 1 after discovery/migration/seed/provision passed; the harness requested nonexistent `dal-obscura serve`. Corrected it to the existing default `dal-obscura` command. Product code did not change.
- Corrected real CLI help audit in the fresh environment, `/tmp/dal-architecture-t14-cli-help.log`: exit 0. Every real public command's --help succeeded; invoking actual read main with --help returned the exact preserved literal. The earlier invalid serve probe is explicitly excluded from evidence.
- Second live harness run, `/tmp/dal-architecture-t14-live-retry.log`: exit 1. Real Keycloak-authenticated Flight checks passed for four granted personas, two successful NULL-masked no-grant personas, unauthenticated rejection, nested structs/lists/maps, original-value row filters, masking and multiple file tickets. Browser sign-in/session/inventory/reload passed, but logout returned 403. Vite's string proxy option sets changeOrigin=true and rewrote Host, unlike the production NGINX gateway. Corrected only the scratch harness to preserve Host. No origin/CSRF/capability check was changed; failed stacks were cleaned and diagnostic credentials redacted before inspection.
- `/tmp/dal-architecture-t14-live-qualified.log`: exit 1 on an intermittent FlightUnauthorizedError for the first no-grant persona after four granted personas passed. This run did not reach the browser check. The earlier run had passed both no-grant personas. No product code or assertion was changed, and this rejection is not counted as a successful denial test.
- `/tmp/dal-architecture-t14-live-diagnostic.log`: exit 0. Added a read-only container/host clock probe and an additional independent JWT verification before returning each unchanged live token to the existing read checker. All native checks and the unchanged browser test passed. The timing probe alone does not establish the cause of the previous rejection.
- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-release-3036-t13 uv run --no-sync python /tmp/dal-architecture-t14-live.py > /tmp/dal-architecture-t14-live-final.log 2>&1`: exit 0 after removing that additional token verifier. The original unmodified check_demo.py and live-oidc-demo.spec.ts both passed against real Keycloak and the exact installed release. Browser test retries remain zero. Login, persisted session/inventory reload, CSRF/origin-validated logout, provider logout and fresh sign-in passed. All owned processes/container/database were cleaned. The private directory is /var/folders/hj/cy2l0bkn4bx4spz62wzd1lj40000gn/T/dal-architecture-t14-live-sbiqpsd0; its generated credentials are not copied here or into Git.
- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-release-3036-t13 mvn -f connectors/jvm/pom.xml verify > /tmp/dal-architecture-t14-jvm.log 2>&1`: exit 0, BUILD SUCCESS in 36.546 seconds. XML audit: 52 tests, zero failures/errors/skips across eight reports. Includes the Java client, Spark datasource, fixture testkit and seven real Spark read integration tests.
- `docker stop dal-architecture-t06-pg-3036 > /tmp/dal-architecture-t14-postgres-cleanup.log 2>&1`: exit 0. The task-owned disposable PostgreSQL container was removed by its --rm setting. `docker ps --format '{{.Names}}' > /tmp/dal-architecture-t14-containers-after.txt`: exit 0, no containers remain. No existing user service was changed.
- `/tmp/dal-architecture-t14-artifacts.json` records the final source and exact wheel digests. Service source SHA256 is 86dab1fa77dc3340c5fc5c6a0164405d398d60b2c5520509f1f622c42ed93309; wheel SHA256 is 7ffb8334fc55ac0a33f1c4645af00a5158fa5953d02adbc8d6b690fc93aa1a14. Python remains 3.14.8 and uv.lock SHA256 remains 4e3c216ad1a8e8010b9e84f30daf07d01b85df12b43bbfeb46d4556e0a27afc6, matching the baseline.
- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-release-3036-t13 UV_NO_SYNC=1 sh scripts/run_capacity_benchmarks.sh /tmp/dal-architecture-after-wheels > /tmp/dal-architecture-capacity-after-wheels.log 2>&1`: exit 0. All 15 suite runs passed with the unchanged five repetitions, workloads, inputs and assertions. All other test suites and disposable services finished before this started. Baseline evidence is preserved in /tmp/dal-architecture-before-wheels; no evidence directory was overwritten. The aggregate is /tmp/dal-architecture-after-wheels/summary.json.
- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-release-3036-t13 uv run --no-sync python scripts/schema_capture_performance.py --fields 4000 --iterations 5 --output /tmp/dal-architecture-schema-after-wheels.json > /tmp/dal-architecture-schema-after-wheels.log 2>&1`: exit 0, run after capacity completed with no competing suite.
- Comparison audit through `uv run --no-sync python - > /tmp/dal-architecture-performance-comparison.json`: exit 0. It verifies all ten before/after streaming records have identical row/file/endpoint/chunk/limit parameters and all schema/evaluation SQL counts match exactly.

Measured mean timings, before to after:

- Row filter: 6.798 to 6.297 ms; masks: 15.041 to 14.131 ms.
- Filter with top-level masks: 11.308 to 10.472 ms; filter with nested masks: 11.460 to 10.308 ms.
- Multifile scan: 56.121 to 50.848 ms.
- Complex Flight: 18.925 to 19.204 ms, an observed 1.47% increase. Run-mean ranges overlap: 18.584–19.815 ms before and 18.444–21.253 ms after.
- Combined 10M/25M streaming: 74.6153 to 75.5246 seconds, an observed 1.22% increase. Run-mean ranges overlap: 72.3197–78.8004 seconds before and 73.2667–76.9390 seconds after.
- Schema capture median: 3.7610 to 3.9590 ms (+5.26%), with Python peak allocation 95,059 to 99,417 bytes. Evaluation median: 9.6922 to 9.8551 ms (+1.68%), with Python peak allocation 115,502 to 117,609 bytes. Absolute median increases are about 0.198 and 0.163 ms. No latency threshold or workload was changed. These samples do not isolate a causal source for the small timing/allocation increases; no implementation speedup is claimed from the lower means in the other probes.

Every streaming repetition processed 35M input/output rows across the two probes.
The large probe still plans five files of 5M rows, returns four endpoints and 3055
chunks, and bounds chunks at 8192 rows. The existing first-batch-before-half-total
assertions passed. Large-probe RSS deltas after, in bytes: 42,090,496; 42,369,024;
39,944,192; 42,369,024; 34,881,536. The before range was 24,166,400–39,763,968
bytes; the after range is 34,881,536–42,369,024. Small-probe deltas after:
37,437,440; 38,649,856; 52,887,552; 39,075,840; 33,882,112 bytes. Sampling is
noisy and the observed large-probe RSS increased. The unchanged 1 GiB test limit
passed; it is not a production RSS guarantee.

Required Q4 harness and schema measurements passed. The full 1/4/16 concurrency
matrix, slow-provider SLA, individual first-batch durations and retained Arrow
bytes lack T00 comparison baselines. Q4 explicitly permits those unavailable
comparisons to be recorded as unmeasured with no improvement claim. They remain
unmeasured and provide no concurrency/SLA/capacity or RSS certification. The
functional cooperative-cancellation/input-expiry and exact disclosure-count gates
were independently qualified in T09 and in the final no-skips suite.

Additional final diagnostics and review:

- `UV_PROJECT_ENVIRONMENT=/tmp/dal-architecture-release-3036-t13 uv run --no-sync python /tmp/dal-architecture-t14-clock-probe.py > /tmp/dal-architecture-t14-clock-probe.log 2>&1`: exit 0 after performance measurements completed. The tenth newly minted real Keycloak token was correctly rejected with ImmatureSignatureError: The token is not yet valid (iat). Its iat was 0.012253 seconds ahead of the host clock. No token, password or private key was printed or retained in this report; no clock or leeway setting was changed. This reproduces an environment timing precondition consistent with the earlier intermittent rejection; the earlier raw token was not retained, so attributing that specific failure remains an inference. The unmodified live lane passed with valid token times. The diagnostic container was removed.
- `uv run --no-sync python - > /tmp/dal-architecture-t14-final-audit.log`: exit 0. Every one of the wheel's 149 service resources matches current source bytes. All prior task gates are recorded; the unrelated AGENTS.md diff is still exactly 142 additions/64 deletions. No retired source package or forwarding file remains.
- `uv run --no-sync python /tmp/dal-architecture-t13-map-audit.py > /tmp/dal-architecture-t14-map-audit.log 2>&1`: exit 0, all 126 mapped entries and retained test functions verified again. Generated protocol digests and the original migration remain unchanged.
- `uv run --no-sync ruff check .`, `uv run --no-sync ruff format --check .`, and `uv run --no-sync ty check`, logs `/tmp/dal-architecture-t14-{lint,format,types}.log`: exit 0; 406 Python files formatted. `git diff --check`: exit 0. Final container inventory is empty.
- Full Q2 evidence under T13 applies to the identical final UI source and built assets: 75 unit, 59 story and 111 browser tests, TypeScript, generated API types and bundle budgets all passed. No UI or service source changed during T14; wheel/source byte equality was checked after every required lane. No required executable qualification was replaced by a narrower suite.

Final delivery: all T00–T14 tasks passed their acceptance gates. The implemented
owners, immutable snapshots, exact schema/value semantics, transaction boundaries,
strict per-output SQL revocation and UI drafts/generation fences are qualified.
No network service, persisted schema/revision, ticket/scan protocol, stored provider
identifier, process isolation, weighted admission or revocation cache was added.
The authorized checkpoint commits were pushed earlier. At qualification completion,
changes after that checkpoint remained uncommitted. The user subsequently
authorized committing and pushing the completed implementation on
codex/policy-authoring-hardening. The user's unrelated AGENTS.md rewrite remains
preserved and excluded from that delivery. No merge or deployment is authorized.

The first final record-consistency audit flagged T05's historical blocked-status
label even though its acceptance update and manifest already recorded completion.
Relabeled that paragraph as the earlier checkpoint and added an explicit current
complete status. The repeated audit confirms all 15 current statuses agree;
no implementation or qualification result changed.

Final `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index > /tmp/dal-architecture-t14-index-delivery.log 2>&1`:
exit 0, 642 files listed, two reprocessed, zero errors. Final `git diff --check`:
exit 0. All 15 current task statuses and completion-evidence records agree.



## Current cumulative workspace path inventory

The following list excludes the user-owned AGENTS.md rewrite. It includes the
planning inputs that were already untracked. Those inputs remain unchanged
except for tasks.json status/evidence and the new progress record.

```text
docs/plans/architecture-implementation/README.md
docs/plans/architecture-implementation/governance-inventory.json
docs/plans/architecture-implementation/module-map.json
docs/plans/architecture-implementation/progress.md
docs/plans/architecture-implementation/tasks.json
examples/auth/_shared/scripts/build_runtime.py
examples/demo/keycloak/scripts/provision_demo.py
examples/nested-mor/scripts/verify.py
pyproject.toml
scripts/export_control_plane_openapi.py
scripts/schema_capture_performance.py
src/dal_obscura/adapters/__init__.py
src/dal_obscura/adapters/duckdb/__init__.py
src/dal_obscura/adapters/duckdb/compiler.py
src/dal_obscura/adapters/duckdb/connection.py
src/dal_obscura/adapters/duckdb/limits.py
src/dal_obscura/adapters/duckdb/transform.py
src/dal_obscura/adapters/providers/__init__.py
src/dal_obscura/adapters/providers/adapters.py
src/dal_obscura/adapters/providers/catalogs.py
src/dal_obscura/adapters/providers/discovery.py
src/dal_obscura/adapters/providers/published.py
src/dal_obscura/adapters/providers/schema.py
src/dal_obscura/adapters/providers/task_codec.py
src/dal_obscura/adapters/sql/__init__.py
src/dal_obscura/adapters/sql/assets.py
src/dal_obscura/adapters/sql/audit.py
src/dal_obscura/adapters/sql/catalogs.py
src/dal_obscura/adapters/sql/database/__init__.py
src/dal_obscura/adapters/sql/database/cli.py
src/dal_obscura/adapters/sql/database/db.py
src/dal_obscura/adapters/sql/database/migrations/__init__.py
src/dal_obscura/adapters/sql/database/migrations/env.py
src/dal_obscura/adapters/sql/database/migrations/script.py.mako
src/dal_obscura/adapters/sql/database/migrations/versions/20261011_0001_current_schema.py
src/dal_obscura/adapters/sql/database/migrations/versions/__init__.py
src/dal_obscura/adapters/sql/database/orm.py
src/dal_obscura/adapters/sql/encoding.py
src/dal_obscura/adapters/sql/policies.py
src/dal_obscura/adapters/sql/snapshots.py
src/dal_obscura/adapters/sql/tickets.py
src/dal_obscura/adapters/sql/unit_of_work.py
src/dal_obscura/adapters/sql/workspace.py
src/dal_obscura/adapters/tickets/__init__.py
src/dal_obscura/adapters/tickets/signing.py
src/dal_obscura/application/__init__.py
src/dal_obscura/application/governance/__init__.py
src/dal_obscura/application/governance/errors.py
src/dal_obscura/application/governance/ports.py
src/dal_obscura/application/governance/values.py
src/dal_obscura/application/reads/__init__.py
src/dal_obscura/application/reads/admission.py
src/dal_obscura/application/reads/contracts.py
src/dal_obscura/application/reads/execution.py
src/dal_obscura/application/reads/ports.py
src/dal_obscura/application/reads/service.py
src/dal_obscura/application/reads/sources.py
src/dal_obscura/application/reads/tickets/__init__.py
src/dal_obscura/application/reads/tickets/contracts.py
src/dal_obscura/application/reads/tickets/payload.py
src/dal_obscura/control/access.py
src/dal_obscura/control/assets/access.py
src/dal_obscura/control/assets/schema.py
src/dal_obscura/control/assets/service.py
src/dal_obscura/control/audit_service.py
src/dal_obscura/control/catalogs/discovery.py
src/dal_obscura/control/catalogs/service.py
src/dal_obscura/control/context.py
src/dal_obscura/control/errors.py
src/dal_obscura/control/plugins.py
src/dal_obscura/control/policies/compiler.py
src/dal_obscura/control/policies/evaluation.py
src/dal_obscura/control/policies/service.py
src/dal_obscura/control/workspace/attributes.py
src/dal_obscura/control/workspace/auth_providers.py
src/dal_obscura/control/workspace/service.py
src/dal_obscura/identity/claims.py
src/dal_obscura/identity/contracts.py
src/dal_obscura/identity/http_session.py
src/dal_obscura/identity/names.py
src/dal_obscura/identity/oidc.py
src/dal_obscura/identity/sessions.py
src/dal_obscura/interfaces/cli/control.py
src/dal_obscura/interfaces/cli/maintenance.py
src/dal_obscura/interfaces/cli/read.py
src/dal_obscura/interfaces/flight/contracts.py
src/dal_obscura/interfaces/flight/server.py
src/dal_obscura/interfaces/flight/streaming.py
src/dal_obscura/interfaces/http/app.py
src/dal_obscura/interfaces/http/health.py
src/dal_obscura/interfaces/http/request_context.py
src/dal_obscura/interfaces/http/routes/assets.py
src/dal_obscura/interfaces/http/routes/audit.py
src/dal_obscura/interfaces/http/routes/catalogs.py
src/dal_obscura/interfaces/http/routes/deps.py
src/dal_obscura/interfaces/http/routes/plugins.py
src/dal_obscura/interfaces/http/routes/policies.py
src/dal_obscura/interfaces/http/routes/session.py
src/dal_obscura/interfaces/http/routes/settings.py
src/dal_obscura/interfaces/http/routes/workspace.py
src/dal_obscura/kernel/__init__.py
src/dal_obscura/kernel/identity/__init__.py
src/dal_obscura/kernel/identity/actor.py
src/dal_obscura/kernel/identity/authentication.py
src/dal_obscura/kernel/identity/names.py
src/dal_obscura/kernel/identity/principal.py
src/dal_obscura/kernel/plans/__init__.py
src/dal_obscura/kernel/plans/governed.py
src/dal_obscura/kernel/plans/projection.py
src/dal_obscura/kernel/plans/request.py
src/dal_obscura/kernel/plans/snapshot.py
src/dal_obscura/kernel/policy/__init__.py
src/dal_obscura/kernel/policy/attribute_conditions.py
src/dal_obscura/kernel/policy/authorization.py
src/dal_obscura/kernel/policy/filters.py
src/dal_obscura/kernel/policy/mask_types.py
src/dal_obscura/kernel/policy/models.py
src/dal_obscura/kernel/policy/policy_resolution.py
src/dal_obscura/policy/__init__.py
src/dal_obscura/policy/admission.py
src/dal_obscura/policy/attribute_conditions.py
src/dal_obscura/policy/authorization.py
src/dal_obscura/policy/filters.py
src/dal_obscura/policy/mask_types.py
src/dal_obscura/policy/models.py
src/dal_obscura/policy/policy_resolution.py
src/dal_obscura/policy/projection.py
src/dal_obscura/read/__init__.py
src/dal_obscura/read/contracts.py
src/dal_obscura/read/execution/__init__.py
src/dal_obscura/read/execution/connection.py
src/dal_obscura/read/execution/contracts.py
src/dal_obscura/read/execution/limits.py
src/dal_obscura/read/execution/stream.py
src/dal_obscura/read/execution/transform.py
src/dal_obscura/read/request.py
src/dal_obscura/read/service.py
src/dal_obscura/read/tickets/__init__.py
src/dal_obscura/read/tickets/contracts.py
src/dal_obscura/read/tickets/envelope.py
src/dal_obscura/read/tickets/payload.py
src/dal_obscura/read/tickets/signing.py
src/dal_obscura/runtime/configuration.py
src/dal_obscura/sources/__init__.py
src/dal_obscura/sources/access.py
src/dal_obscura/sources/adapters.py
src/dal_obscura/sources/catalogs.py
src/dal_obscura/sources/contracts.py
src/dal_obscura/sources/discovery.py
src/dal_obscura/sources/planning.py
src/dal_obscura/sources/published.py
src/dal_obscura/sources/schema.py
src/dal_obscura/storage/__init__.py
src/dal_obscura/storage/assets.py
src/dal_obscura/storage/audit.py
src/dal_obscura/storage/catalogs.py
src/dal_obscura/storage/database/__init__.py
src/dal_obscura/storage/database/cli.py
src/dal_obscura/storage/database/db.py
src/dal_obscura/storage/database/migrations/__init__.py
src/dal_obscura/storage/database/migrations/env.py
src/dal_obscura/storage/database/migrations/script.py.mako
src/dal_obscura/storage/database/migrations/versions/20261011_0001_current_schema.py
src/dal_obscura/storage/database/migrations/versions/__init__.py
src/dal_obscura/storage/database/orm.py
src/dal_obscura/storage/encoding.py
src/dal_obscura/storage/policies.py
src/dal_obscura/storage/snapshots.py
src/dal_obscura/storage/tickets.py
src/dal_obscura/storage/workspace.py
tests/acceptance/test_ticket_encoding.py
tests/benchmarks/test_masking_row_filter_benchmarks.py
tests/benchmarks/test_ticket_to_response_benchmark.py
tests/conftest.py
tests/consumers/test_governed_reads.py
tests/control/assets/test_asset_grant_authorization.py
tests/control/assets/test_asset_mutation_locks.py
tests/control/assets/test_asset_service_plugins.py
tests/control/assets/test_schema_evolution.py
tests/control/assets/test_schema_service.py
tests/control/catalogs/test_catalog_discovery.py
tests/control/catalogs/test_catalog_option_validation.py
tests/control/policies/test_evaluation_service.py
tests/control/test_access_identity.py
tests/control/test_audit_repository.py
tests/control/test_browser_sessions.py
tests/control/test_live_config_admission.py
tests/control/test_structured_conflicts.py
tests/control/workspace/test_auth_provider_validation.py
tests/examples/test_demo_initialization.py
tests/identity/test_identity_oidc_jwks.py
tests/identity/test_identity_request.py
tests/integration/test_io_boundary.py
tests/integration/test_live_snapshot.py
tests/integration/test_recovery_upgrade.py
tests/interfaces/cli/control/test_control_plane_cli.py
tests/interfaces/cli/maintenance/test_maintenance_cli.py
tests/interfaces/cli/read/test_runtime_identity.py
tests/interfaces/flight/test_authentication_request_context.py
tests/interfaces/flight/test_data_plane_health.py
tests/interfaces/flight/test_integration_flight_backends.py
tests/interfaces/flight/test_service_streaming.py
tests/interfaces/http/assets/test_schema_api.py
tests/interfaces/http/catalogs/test_catalogs_api.py
tests/interfaces/http/contract/test_api_inventory_reads.py
tests/interfaces/http/identity/test_actor_auth.py
tests/interfaces/http/identity/test_oidc_login.py
tests/plugins/test_delta_format_plugin.py
tests/plugins/test_public_catalog_adapter.py
tests/plugins/test_public_format_adapter.py
tests/policy/test_access_models.py
tests/policy/test_null_default_policy.py
tests/policy/test_policy_resolution.py
tests/policy/test_row_filters.py
tests/read/execution/test_duckdb_filters.py
tests/read/execution/test_duckdb_masks.py
tests/read/execution/test_duckdb_streaming.py
tests/read/execution/test_stream_ownership.py
tests/read/planning/test_planning_admission.py
tests/read/planning/test_planning_filters.py
tests/read/planning/test_planning_projection.py
tests/read/test_fetch.py
tests/read/test_live_request_context.py
tests/read/test_query_models.py
tests/read/tickets/fixtures/scan_envelope_v1.json
tests/read/tickets/fixtures/signed_reference.txt
tests/read/tickets/test_scan_envelopes.py
tests/read/tickets/test_ticket_hmac.py
tests/read/tickets/test_ticket_payload_hash.py
tests/read/tickets/test_ticket_payload_validation.py
tests/read/tickets/test_tickets.py
tests/sources/test_catalog_registry.py
tests/sources/test_source_admission.py
tests/storage/test_db_driver_extras.py
tests/storage/test_governance_unit_of_work.py
tests/storage/test_live_config_store.py
tests/storage/test_migration_cli.py
tests/storage/test_schema_admission.py
tests/storage/test_schema_migrations.py
tests/storage/test_ticket_store_sqlalchemy.py
tests/support/control/control_plane.py
tests/support/control/seed.py
tests/support/flight/flight.py
tests/support/flight/iceberg_flight.py
tests/support/identity.py
tests/support/performance/stream_benchmark.py
tests/support/plugins/public_plugins.py
tests/support/qualification/build_connector_fixture.py
tests/support/qualification/consumer_backends.py
tests/support/read/read_scenarios.py
tests/support/read/reads.py
tests/support/read/tickets.py
tests/support/read/use_cases.py
tests/support/sources/live_config.py
tests/support/storage/postgres.py
tests/support/storage/storage_queries.py
tests/test_e2e_smoke.py
tests/test_support_build_connector_fixture.py
```
