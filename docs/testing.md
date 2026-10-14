# Testing and qualification

## Ownership and evidence

Core tests follow the owning service folder. Package tests qualify the public SDK
and provider behavior independently. Shared real-provider writers and worker
probes belong in the conformance kit. Tests must not import another test module.
Fixtures own setup and cleanup; assertions belong in tests.
Warnings fail Python qualification. Close engines, catalogs, HTTP responses and
child-process pipes explicitly, including failure paths. Annotate generators
decorated with `contextmanager` as `Generator[T]` on Python 3.14.

For new behavior, observe an owning test fail, then implement the smallest change.
Assert exact schemas with metadata and scalar/nested values. Parameterize equivalent
cases. Keep trust-boundary checks distinct while removing duplicate validation
within one boundary. Trigger cancellation and races with explicit provider signals.
Use native create, update and delete operations for valid tables. Edit protocol
artifacts only to test malformed inputs.

## Local checks

Install actual workspace wheels so entry-point metadata and artifact admission
match packaged behavior. Reinstall affected distributions after source changes.

```sh
uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen
uv run --no-sync pytest tests packages -o pythonpath= -m 'not heavy and not integration and not socket'
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check
```

Run focused tests before broader checks. The default root suite contains core
tests; pass `packages` explicitly to run package suites. Each package owns its test
support. Native fixture writers require Java 17, Maven and pinned Spark artifacts.
The [conformance guide](../packages/plugin-conformance/README.md) explains the lanes.

## Mandatory qualification

Use a disposable PostgreSQL database and permission to bind local sockets. Keep
existing demo services untouched. Set `DAL_OBSCURA_POSTGRES_TEST_URL` to its
`postgresql+psycopg` URL and run the no-skips wrapper with real consumers enabled:

```sh
DAL_OBSCURA_RUN_CONSUMER_TESTS=1 uv run --no-sync python scripts/require_no_skips.py \
  --junitxml /tmp/dal-python.xml -- \
  uv run --no-sync pytest tests packages -o pythonpath= --ignore=tests/benchmarks
mvn -f connectors/jvm/pom.xml verify
pnpm --dir apps/governance-ui test
pnpm --dir apps/governance-ui test:stories
pnpm --dir apps/governance-ui test:e2e
pnpm --dir apps/governance-ui check
pnpm --dir apps/governance-ui check:api-types
pnpm --dir apps/governance-ui build
```

Distributed evidence requires separate S3 workers with data-file access restricted
to assignments. Shared metadata/delete reads are allowed. Thread-pool checks alone
are insufficient. Run live SSO when identity contracts change. Performance claims
need comparable same-host baselines and representative files, nested values,
deletes, streaming and RSS measurements; use `tests/benchmarks/` explicitly.
Complete dead-code auditing and relevant performance measurements during
implementation. Trace candidate callers, including registered callbacks and
plugin entry points, before deletion. Record measured limits and fixes with the
delivery evidence. A separate senior review is not a completion gate.

## Release artifacts

Build affected wheels outside Git, install them in a fresh environment, and run
qualification with `-o pythonpath=`. Rebuild the lock from those exact installed
artifacts. Verify SDK independence, locked admission, CLIs and packaged migrations.
Never hand-edit generated OpenAPI, client types or protobuf code.
