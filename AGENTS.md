# Agent Guide (dal-obscura)

## Purpose
This repo implements a governed Iceberg data access layer exposed through Arrow Flight, with policy-based masking and row filters. The runtime is organized as a hexagonal/clean architecture with explicit ports and adapters.

## Ground Rules
- Keep the service stateless; do not add persistent state without explicit approval.
- Masks and row filters must be expressed as DuckDB SQL expressions.
- Avoid unnecessary data copies; prefer Arrow + DuckDB zero-copy paths where possible.
- Follow TDD for new behavior: add or extend the owning behavioral test, implement the smallest change, then run focused and broad checks.
- TableFormat task planning must create parallelizable scan tasks whenever the backend exposes splittable work, such as files, fragments, partitions, or row groups. If a backend cannot be parallelized, document the reason and performance drawback in the format implementation and user-facing docs.

## How to Run
```bash
uv sync --dev --extra server --extra sqlite
uv run dal-obscura --help
uv run dal-obscura-control-plane --help
pnpm --dir apps/governance-ui install --frozen-lockfile
pnpm --dir apps/governance-ui dev
```

The data plane reads `DAL_OBSCURA_*` environment variables from an already
migrated and published database. Start the authenticated governance UI with
the control-plane command; use the local bootstrap token only for disposable
development profiles. Run `uv run dal-obscura-migrate upgrade` before either
service.

## Tests
```bash
uv sync --dev
uv run pytest
uv run ruff check .
uv run ruff format .
uv run ty check
```

### Focused Checks
```bash
uv run pytest tests/domain/access_control/test_row_filters.py tests/interfaces/flight/test_service_streaming.py::test_parse_descriptor_rejects_unsafe_row_filter_sql tests/infrastructure/adapters/test_duckdb_transform.py -q
uv run pytest tests/test_e2e_smoke.py
```

### Benchmarks
```bash
uv run pytest tests/benchmarks --benchmark-only
uv run pytest tests/benchmarks/test_masking_row_filter_benchmarks.py --benchmark-only --benchmark-json .benchmarks/row-filter-mask.json
uv run pytest tests/benchmarks/test_iceberg_multifile_benchmark.py --benchmark-only --benchmark-json .benchmarks/iceberg-multifile.json
uv run pytest tests/benchmarks/test_ticket_to_response_benchmark.py --benchmark-only
```

### Pre-commit
```bash
uv run pre-commit install
uv run pre-commit run --all-files
```

### JVM Connectors
```bash
mvn -f connectors/jvm/pom.xml verify
```

## Architecture (Current)
- **Interfaces (transport adapters)**
  - `data_plane/interfaces/flight/`: Arrow Flight server, header middleware, request parsing, streaming.
  - `data_plane/interfaces/cli/`: data-plane composition root and startup command.
  - `control_plane/interfaces/`: authenticated HTTP routes, UI shell, admin and maintenance CLIs.
- **Application (use cases + ports)**
  - `data_plane/application/use_cases/plan_access.py`: authenticate, authorize, plan, mint tickets.
  - `data_plane/application/use_cases/fetch_stream.py`: verify ticket, re-auth, execute, stream.
  - `data_plane/application/ports/`: identity, authorization, ticket, masking, row transforms.
  - `control_plane/application/`: catalog, asset, policy, publication, session and audit use cases.
- **Domain (pure models + policies)**
  - `common/access_control/`: policy models + resolution rules.
  - `common/query_planning/`: plan request/read spec.
  - `common/ticket_delivery/`: ticket payload value object.
  - `common/catalog/`, `common/table_format/`: catalog and executable table format ports.
- **Infrastructure (adapters)**
  - `control_plane/infrastructure/`: catalog registry, repositories, sessions and policy storage.
  - `data_plane/infrastructure/`: published configuration, JWT identity, HMAC tickets, Iceberg and DuckDB adapters.

### Request Flow
1. `get_flight_info` -> `PlanAccessUseCase`
   - Authenticate via JWT headers.
   - Resolve catalog/target -> executable `TableFormat` -> schema + parallel scan tasks where possible.
   - Authorize columns/filters/masks via policy.
   - Mint HMAC-signed tickets with policy version and scan payload.
   - Return masked output schema + ticket endpoints.
2. `do_get` -> `FetchStreamUseCase`
   - Verify ticket signature + expiry.
   - Re-authenticate and confirm principal and policy version.
   - Execute table format scan tasks to stream Arrow batches.
   - Apply row filters + masking via DuckDB and stream results.

## Repo Map (Key Files)
- `src/dal_obscura/data_plane/interfaces/cli/`: data-plane composition root / CLI wiring
- `src/dal_obscura/data_plane/interfaces/flight/`: Flight adapter and request contracts
- `src/dal_obscura/data_plane/application/use_cases/`: planning, ticketing and streaming
- `src/dal_obscura/control_plane/interfaces/routes/`: authenticated HTTP API and UI shell
- `src/dal_obscura/common/access_control/`: policy models + resolution
- `src/dal_obscura/data_plane/infrastructure/adapters/`: published config, catalogs, masking and tickets
- `tests/`: unit tests

The public plugin SDK and conformance runner live under `packages/plugin-api/` and
`packages/plugin-conformance/`. Built catalog/format plugins live under
`packages/*-plugin/` and are admitted by the lockfile-backed registry; callers must
not import service internals or select plugins by module path.

## Common Tasks
- Add a new catalog plugin: implement the public SDK `CatalogPlugin`, declare output formats/capabilities in its static descriptor, add conformance coverage, and admit the exact wheel through the plugin lock.
- Add a new table format plugin: implement the public SDK `TableFormatPlugin`, validate nested Arrow schemas and bounded tasks, and pair it through declared handle versions/capabilities.
- Extend policy: update policy parsing/resolution and add tests.
- Add a new mask type: update `_mask_expression` and any schema adjustments, plus tests.
- Change ticket payloads: update `TicketPayload`, `HmacTicketCodecAdapter`, both use cases, and tests.
- Tune fan-out: adjust `--max-tickets` for ticket planning.
- Validate planner/masking/filter execution changes against the benchmark JSON baselines in `.benchmarks/`.

## New Rules (Self-Guidance)
- Keep domain and application layers free of transport concerns (no Flight types in use cases).
- Any new policy feature must update both `authorize()` and `current_policy_version()` paths.
- Ticket content is the single source of truth for `do_get`; never trust client replays of `PlanRequest`.
- Masking changes must update both the DuckDB projection logic and the masked schema logic.
- Catalog resolution must stay deterministic; never mutate shared config or registry state during requests.
- Catalog implementations resolve governed targets into executable table formats directly.
- Avoid pickling arbitrary user input; only serialize trusted, internal task payloads.

## Style
- Prefer small, composable functions.
- Keep public APIs stable; document changes in README.
