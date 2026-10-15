# Architecture implementation progress

Implementation authorized on 2026-10-15. Work proceeds serially, with no delegation.
The user subsequently authorized committing progress and pushing the current
branch before continuing. Merge and deployment remain unauthorized.

## Authorized checkpoint

Checkpoint includes T00–T04 and the independently verified T05 implementation.
T05 remains blocked on the recorded acceptance-scope conflict. The unrelated
root AGENTS.md rewrite remains unstaged. The branch is
`codex/policy-authoring-hardening`, tracking the same branch on `origin`.
`git diff --check` passed before staging. Existing verification results below
describe the code in this checkpoint; no qualification gate is waived.
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

Status: acceptance blocked pending clarification. T04 acceptance verified before
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

Status: pending. Requires T05 acceptance.

## T07: migrate remaining governance

Status: pending. Requires T06 acceptance.

## T08: centralize startup and sessions

Status: pending. Requires T07 acceptance.

## T09: supervise execution lifecycle

Status: pending. Requires T08 acceptance.

## T10: own UI session lifecycle

Status: pending. Requires T09 acceptance.

## T11: own asset and policy workflows

Status: pending. Requires T10 acceptance.

## T12: own management queries

Status: pending. Requires T11 acceptance.

## T13: enforce boundaries and remove obsolete paths

Status: pending. Requires T12 acceptance.

## T14: qualify and deliver

Status: pending. Requires T13 acceptance and every required qualification lane.

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
