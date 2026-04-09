# dal-obscura

Data access layer with Arrow Flight, Iceberg, masking, and row filters.

## Highlights
- Arrow Flight plan/ticket flow with HMAC-signed tickets
- Iceberg v2/v3 backend (pyiceberg)
- DuckDB-powered row filters and column masks
- YAML policy-driven authz

## Architecture
- `domain/`
  - `access_control`: principals, policy models, access resolution.
  - `query_planning`: plan/read payload models and filter primitives.
  - `ticket_delivery`: ticket payload value object.
- `application/`
  - `ports`: hexagonal contracts (`IdentityPort`, `AuthorizationPort`, backend, masking, ticket, row transform).
  - `use_cases`: `PlanAccessUseCase` and `FetchStreamUseCase`.
- `infrastructure/adapters/`
  - default identity, policy-file authorizer, Iceberg backend, DuckDB masking/row transform, HMAC ticket codec.
- `interfaces/`
  - `flight`: Arrow Flight transport adapter.
  - `cli`: composition root and runtime wiring.

## Requirements
- Python 3.10+
- `uv` for package management

## Quickstart (uv)

```bash
uv venv
uv sync
uv run dal-obscura --help
```

## Policy Example

```yaml
version: 1
datasets:
  - table: "catalog.db.table"
    rules:
      - principals: ["user1", "group:analyst"]
        columns: ["id", "email", "user.address.zip"]
        masks:
          email: { type: "redact", value: "***" }
          "user.address.zip": { type: "hash" }
        row_filter: "region = 'us'"
      - principals: ["group:analyst"]
        when:
          tenant: "acme"
          clearance: ["high", "internal"]
        columns: ["id", "email"]
      - principals: ["group:analyst"]
        when:
          clearance: "low"
        effect: deny
        columns: ["email"]
```

## Running

```bash
uv run dal-obscura \
  --app-config app.yaml
```

Clients must send JWTs as `Authorization: Bearer <token>` headers on both `get_flight_info` and `do_get`.

Clients may include an optional `row_filter` in the `get_flight_info` command payload:

```json
{
  "catalog": "analytics",
  "target": "default.users",
  "columns": ["id", "email"],
  "row_filter": "region = 'us'"
}
```

Engine `row_filter` values are validated as DuckDB SQL expressions. They may
reference columns that are not projected back to the client, but only when
those columns are plainly visible and unmasked under policy. The service
combines the engine filter with any policy filter using `AND`, pushes down the
safe subset during backend planning, and evaluates any unsupported remainder in
DuckDB during streaming.

`app.yaml` references `catalogs.yaml` and `policies.yaml`, and runtime secrets are loaded via secret references (for example, env vars):

```yaml
location: grpc://0.0.0.0:8815
catalog_file: catalogs.yaml
policy_file: policies.yaml
secret_provider:
  module: dal_obscura.infrastructure.adapters.secret_providers.EnvSecretProvider
  args: {}
ticket:
  ttl_seconds: 900
  max_tickets: 64
  secret:
    key: DAL_OBSCURA_TICKET_SECRET
auth:
  jwt_secret:
    key: DAL_OBSCURA_JWT_SECRET
  jwt_issuer: null
  jwt_audience: null
logging:
  level: INFO
  json: true
```

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff check .
uv run ruff format .
uv run ty check
```

Benchmark baselines:

```bash
uv run pytest tests/benchmarks --benchmark-only
uv run pytest tests/benchmarks/test_masking_row_filter_benchmarks.py --benchmark-only --benchmark-json .benchmarks/row-filter-mask.json
uv run pytest tests/benchmarks/test_iceberg_multifile_benchmark.py --benchmark-only --benchmark-json .benchmarks/iceberg-multifile.json
```

Use the JSON output as the before/after artifact for any planner, filter, masking, or Iceberg execution change. Compare:
- `row-filter-mask`: `filter-only`, `mask-only`, `filter-plus-mask`, and `nested-field-mask`
- `iceberg-multifile`: large multi-file execution baseline

## Test Matrix
- `tests/application/use_cases/test_access_flow_use_cases.py`: access-flow planning and ticket/fetch guardrails, including wildcard expansion, nested requests, and pending internal-dependency regressions.
- `tests/domain/access_control/test_policy_resolution.py`: policy resolution, rule union semantics, row-filter composition, and policy parsing validation.
- `tests/infrastructure/adapters/test_duckdb_transform.py`: masked schema derivation, nested struct masking, and DuckDB projection behavior.
- `tests/infrastructure/adapters/test_iceberg_phase0_regressions.py`: current Iceberg planning baseline plus pending predicate-pushdown regression coverage.
- `tests/interfaces/flight/test_service_streaming.py`: end-to-end Flight behavior for authorization, filtering, masking, and streaming.
- `tests/benchmarks/test_masking_row_filter_benchmarks.py`: row-filter and masking throughput baselines.
- `tests/benchmarks/test_iceberg_multifile_benchmark.py`: large multi-file Iceberg execution baseline.

## Pre-commit hooks

After `uv sync --dev`, install the hooks with `uv run pre-commit install`. The configured hooks run `uv run ruff format` and `uv run ruff check` (each passed the staged python files), plus `uv run ty check` and `uv run pytest --maxfail=1 --disable-warnings` on every commit to guard formatting, linting, typing, and a quick smoke test. Re-run them manually with `uv run pre-commit run --all-files` if needed.

## Notes
- Mask expressions are executed in DuckDB SQL.
- Supported mask types include `null`, `redact`, `hash`, `default`, `email`, and `keep_last`.
- `hash`, `redact`, `email`, and `keep_last` expose masked values as Arrow `string`; `default` exposes the Arrow type DuckDB infers for the configured literal, while `null` preserves the original field type.
- Row filters are validated against the Arrow schema before planning and currently support comparisons, `AND`/`OR`, `IN`, and `IS NULL`/`IS NOT NULL`.
- Iceberg planning pushes down a safe subset of validated row filters for top-level fields; unsupported or nested predicates remain residual DuckDB filters for exact correctness.
- Nested field masks use DuckDB `struct_update`, and list-of-struct masks use `list_transform` plus `struct_update`.

## Current Limitations
- Iceberg pushdown is currently conservative: mixed `AND` predicates split, but nested-field predicates and unsupported shapes remain residual-only.
- ABAC conditions currently support exact principal-attribute matches and membership in an explicit allowed-value list.
- Collection masking currently targets list-of-struct paths and does not yet support arbitrary scalar lists or deeper heterogeneous container rewrites.
- Tickets still serialize Python scan tasks directly, which keeps the transport format tied to Python internals.

## Logging
- JSON logs by default.
- Configure level and JSON output in `app.yaml` under `logging`.
