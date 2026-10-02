# Testing guide

Tests are organized around observable behaviors and the boundary that owns each
contract. Use named parameter cases when inputs vary but the behavior and assertion
remain the same. Keep separate tests when failures mean different things.

This follows Google's Testing on the Toilet guidance on
[testing behaviors](https://testing.googleblog.com/2014/04/testing-on-toilet-test-behaviors-not.html),
[focused tests](https://testing.googleblog.com/2018/06/testing-on-toilet-keep-tests-focused.html),
[public interfaces](https://testing.googleblog.com/2015/01/testing-on-toilet-prefer-testing-public.html),
and [independent expectations](https://testing.googleblog.com/2014/07/testing-on-toilet-dont-put-logic-in.html).
Reduce incidental setup duplication while leaving the important inputs and expected
results visible in the test. Excessive abstraction makes tests harder to understand;
see [Tests Too DRY? Make Them DAMP!](https://testing.googleblog.com/2019/12/testing-on-toilet-tests-too-dry-make.html).

## Ownership

- `tests/common` and `tests/domain` own pure model, policy resolution, field-path,
  identity mapping, and ticket payload behavior. Transport fixtures do not belong here.
- `tests/application` owns planning admission, authorized projection, filters,
  fetch-time reauthorization, ticket issuance, and orchestration. Planning scenarios
  are split into admission, projection, and filtering modules.
- `tests/infrastructure` owns persistence, HMAC encoding, catalog adapters,
  masking, row filtering, and streaming resource management. DuckDB masking,
  filtering, and streaming have separate modules.
- `tests/interfaces` owns HTTP and Flight contracts. Descriptor parsing runs without
  a socket; only actual socket binding requires the `socket` marker.
- `tests/plugin_platform` owns admitted public catalog and format routing. The
  independent SDK and conformance packages retain their own tests under
  `packages/*/tests`; they must also work against installed wheels.
- `tests/consumers` qualifies one shared nested masking contract independently
  against memory, SQL Iceberg, manifest Parquet, and REST Iceberg. It verifies exact
  values through both the Python SDK and DuckDB, rather than accepting only row counts.
- `tests/integration` owns real database publication, revision races, and recovery.
  Recovery script argument validation runs in the fast lane; PostgreSQL recovery
  remains in the service lane.
- `apps/governance-ui/tests` owns pure UI helpers. Storybook interaction tests own
  component behavior and keyboard mechanics. Playwright `e2e` files own complete
  user journeys, grouped into authentication, workspace, management, lifecycle,
  asynchronous state, policy authoring, and performance scenarios.
- `connectors/jvm/*/src/test` owns Java client, Spark options, pushdown/residual
  translation, fixture tooling, and real Spark/Flight integration contracts.
- `tests/benchmarks` owns bounded throughput and large streaming/RSS scenarios.
  The large probe retains a separate subprocess to measure process RSS.

Input validation at separate trust boundaries is intentional. For example, domain
SQL validation, HTTP admission, signed ticket decoding, and public plugin schema
admission each protect a different entry point. A unit test for one boundary does
not replace evidence that another boundary calls it and rejects unsafe input.

## Fixtures and expectations

`tests/conftest.py` supplies a fresh migrated SQLite engine per test, managed database
sessions, and an HTTP client factory that closes every client. App-specific identity,
catalog, and session options remain explicit at the call site. Multiple clients in
one test can share that test's database; unrelated tests cannot share mutable rows.

Reusable fakes and builders live under `tests/support`. Test modules never import
another test module. Ticket defaults, seeded configuration, schema catalogs,
discovery payloads, public plugin fakes, and Flight contexts have one owner.
Fixtures provide data and resources; assertions stay in the behavioral tests.

Browser fixtures compose session, asset, and management API boundaries. Each
installation gets fresh mutable state, including a deep copy of initial policy
rules. Deferred responses expose explicit start/release signals for race tests.
Synthetic routes demonstrate UI behavior; they do not qualify live SSO.

The JVM integration fixture manages its server and Spark session, restores previous
token properties, and closes the server when Spark initialization fails. The
authorization-header journey supplies an actual `Authorization` header option.
Known fixture row counts are independent constants rather than loops that repeat
the policy algorithm under test.

## Consolidation and removal ledger

- Removed the inventory suite's second end-to-end resource provisioning scenario
  and its setup helper. Asset detail, schema, owner/policy writes, catalogs, runtime
  settings, and authentication providers remain covered by their owning HTTP suites.
  Inventory pagination, contract shape, authorization, health, and body-limit cases
  remain distinct.
- Replaced 28 repeated ticket constructors with a small payload builder. Raw payload
  rejection now belongs to the domain suite, expressed as named malformed-input cases.
- Unified duplicated database creation, schema catalog fakes, discovery failures,
  configuration seeding, public plugin fakes, and benchmark schema/probe setup.
- Combined equivalent field-path, SQL rejection, REST configuration, credential,
  filter translation, recovery status, and navigation path cases into named matrices.
  Cases keep their own failure names and assertions; unrelated behaviors were not
  folded into a single large test.
- Replaced four separately implemented backend consumer scenarios with one
  parameterized contract. Coverage grew from simple scalar/count assertions to exact
  nested values on every backend and both consumers.
- Removed the duplicate browser column-shortcut scenario: the pure shortcut suite
  owns the transformations and the governance journey owns nested integration.
  Removed the repeated keyboard-picker browser scenario: the Storybook component
  interaction owns its search, focus, selection, and callback behavior. Mobile
  popovers, schema expansion/scroll stability, save conflicts, and accessibility
  remain distinct browser coverage.
- Extracted screenshot capture from correctness tests. Review captures and live SSO
  use explicit Playwright configurations, so the default suite has no opt-in skips.
- Corrected the stale PostgreSQL CI test path and cleared pytest's configured source
  paths in installed-wheel lanes. Those lanes must import installed packages.

Other suites were retained where they establish distinct contracts: migrations,
revocation, identity enforcement, plugin admission, request limits, source path
admission, parallel scan tasks, resource cleanup, and streaming limits. Removing
their scenarios solely to reduce a test count would remove useful evidence.

## Running the lanes

Install Python and UI dependencies using the repository's locked environments:

```bash
uv sync --dev --extra server --extra sqlite
pnpm --dir apps/governance-ui install --frozen-lockfile
```

The pre-commit lane needs no external service or socket permissions:

```bash
uv run pytest -m 'not heavy and not integration and not socket'
uv run ruff check .
uv run ruff format --check .
uv run ty check
```

Local functional coverage includes socket and heavy functional scenarios while
keeping external services and benchmarks separate:

```bash
uv run pytest -m 'not integration' --ignore=tests/benchmarks
```

For complete non-benchmark Python coverage, start a disposable PostgreSQL database,
set `DAL_OBSCURA_POSTGRES_TEST_URL` to its SQLAlchemy `postgresql+psycopg` URL,
and enable the real consumer lane. PostgreSQL fixtures create and drop isolated
schemas. Use the no-skips wrapper for mandatory qualification:

```bash
DAL_OBSCURA_RUN_CONSUMER_TESTS=1 uv run python scripts/require_no_skips.py \
  --junitxml /tmp/dal-python.xml -- \
  uv run pytest --ignore=tests/benchmarks
```

UI correctness and build checks:

```bash
pnpm --dir apps/governance-ui test
pnpm --dir apps/governance-ui test:stories
pnpm --dir apps/governance-ui test:e2e
pnpm --dir apps/governance-ui check
pnpm --dir apps/governance-ui check:api-types
pnpm --dir apps/governance-ui build
```

`check` type-checks production code, browser tests, review tools, and Playwright
configuration. The regular browser suite uses synthetic API boundaries. For live
SSO, use the Keycloak demo runner described in
[`examples/demo/keycloak/README.md`](../examples/demo/keycloak/README.md); its
browser command selects `playwright.live.config.ts`. Running `test:e2e:live`
directly requires the running demo and `DAL_OBSCURA_E2E_LIVE_OIDC=1`.

Screenshot captures are explicit review tools:

```bash
pnpm --dir apps/governance-ui capture:review identity-review.spec.ts
UX_CAPTURE_PHASE=after pnpm --dir apps/governance-ui capture:review governance-review.spec.ts
```

The identity comparison needs the previous UI preview on port 4174. Review output
and fixtures are documented in `docs/ui-review`; captures are not correctness tests.

JVM and benchmark lanes:

```bash
mvn -f connectors/jvm/pom.xml verify
uv run pytest tests/benchmarks --benchmark-only
```

The complete benchmark command includes the 10-million/35-million-row RSS probe.
Use explicit file or test selections for bounded local checks. Performance claims
require comparable same-host baselines; see the
[capacity runbook](../evaluation/capacity/README.md).

Installed-wheel CI uses a fresh virtual environment and `pytest -o pythonpath=`.
The root pytest configuration otherwise inserts local `src` paths, which can
silently invalidate wheel qualification. Keep test-support imports separate from
package-under-test imports.

## Refactor verification, 2026-10-02

The same fast-lane coverage command before and after the refactor passed 851 and
894 named cases respectively. More granular parameter reporting and corrected
markers explain the increased case count. Python test/support code across the core
and public packages decreased from 25,272 to 24,973 lines and from 797 to 787 test
functions; modules increased from 111 to 130 as large files were split by ownership.

Fast-lane statement coverage increased from 83.68% to 84.87%. Comparing executed
line sets found no loss in unchanged source modules. The changed native Iceberg
module has separate schema, lazy-buffer reuse, failure-cleanup, and real signed-task
round-trip regressions. Coverage is evidence of retention, not proof of correctness.

The full non-benchmark Python lane passed 960 cases with zero skips, including real
PostgreSQL and all four consumer backends. The Node helper suite passed 55 cases,
Storybook passed 58 interactions, the synthetic browser suite passed 89 journeys,
and Maven verification passed all default-profile modules. Production/test
TypeScript, generated API types, lint, format, type checks, and bundle budgets passed.
Installed-wheel qualification passed 76 package cases and 33 core routing cases
with zero skips, and all six bounded benchmarks passed.

The stronger REST/nested consumer contract exposed a native Iceberg defect: its
declared large-offset Arrow schema differed from emitted small-offset batches.
The adapter now normalizes each batch lazily to its declared schema, reuses matching
batches, and closes the reader on early exit or conversion failure. Public plugin
schema validation remains strict.

The large RSS benchmark, live Keycloak browser journey, and Spark 4 profile were
retained but not rerun during this refactor. Bounded benchmarks and a small real
streaming subprocess probe exercised the reorganized benchmark harness.
