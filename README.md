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
  - JWT/OIDC/API-key/mTLS/trusted-header identity providers, policy-file authorizer, Iceberg backend, DuckDB masking/row transform, HMAC ticket codec.
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

## Container Image

The repo root [Dockerfile](/Users/nithish/.codex/worktrees/601a/dal-obscura/Dockerfile)
builds a reusable `dal-obscura` server image. It contains only the server
runtime and the locked Python dependencies needed to start the Flight service.

Build it locally with:

```bash
docker build -t dal-obscura:local .
```

The runnable auth examples under
[examples/auth](/Users/nithish/.codex/worktrees/601a/dal-obscura/examples/auth/README.md)
reuse that image and keep provider-specific infrastructure in each example
directory.

## Connectors

Connector implementations live under `connectors/`.

- `connectors/jvm/dal-obscura-client-java`: engine-agnostic Java Flight client
- `connectors/jvm/spark3-datasource`: Spark 3.x DataSource V2 reader
- `connectors/jvm/integration-tests-jvm`: end-to-end Spark connector coverage

Build and verify the JVM connector workspace with:

```bash
mvn -f connectors/jvm/pom.xml verify
```

See `connectors/README.md` for the Spark datasource contract and option names.

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

Clients authenticate through whichever provider chain is configured. Header-based
providers inspect Flight headers on both `get_flight_info` and `do_get`, while
mTLS providers can authenticate from the verified Flight peer identity alone.

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
  module: dal_obscura.infrastructure.adapters.identity_default.DefaultIdentityAdapter
  args:
    jwt_secret:
      key: DAL_OBSCURA_JWT_SECRET
    jwt_issuer: null
    jwt_audience: null
logging:
  level: INFO
  json: true
```

### Authentication Providers

`auth.module` names a Python class that implements `IdentityPort`:

```python
def authenticate(self, request: AuthenticationRequest) -> Principal:
    ...
```

`AuthenticationRequest` carries normalized headers plus transport metadata such
as `peer_identity`, `peer`, and the current Flight method. That lets one
deployment authenticate with bearer tokens, API keys, mTLS identities, trusted
gateway headers, or a chain of multiple providers without changing the use
cases.

The service ships with these built-in provider adapters:

- `identity_default.DefaultIdentityAdapter`: shared-secret bearer JWT validation.
- `identity_oidc_jwks.OidcJwksIdentityProvider`: OIDC issuer/audience validation with discovered or static JWKS.
- `identity_api_key.ApiKeyIdentityProvider`: static API keys from any configured header.
- `identity_mtls.MtlsIdentityProvider`: authenticated peer identity from Flight/mTLS.
- `identity_trusted_headers.TrustedHeaderIdentityProvider`: identities asserted by a trusted proxy or gateway.

Single-provider config still works:

```yaml
auth:
  module: dal_obscura.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider
  args:
    issuer: https://keycloak.example.com/realms/acme
    audience: dal-obscura
    subject_claim: sub
    group_claims:
      - groups
      - realm_access.roles
      - resource_access.dal-obscura.roles
    attribute_claims:
      tenant: tenant
      clearance: clearance
```

The OIDC/JWKS provider validates access tokens locally. It can discover the
issuer JWKS automatically, or you can pin static keys with `jwks` or
`jwks_file` for offline and on-prem deployments.

Multi-provider chains let one deployment accept several credential families:

```yaml
auth:
  providers:
    - module: dal_obscura.infrastructure.adapters.identity_mtls.MtlsIdentityProvider
      args:
        identities:
          - peer_identity: spiffe://cluster.local/ns/analytics/sa/spark
            id: spark-job
            groups: ["spark"]
    - module: dal_obscura.infrastructure.adapters.identity_api_key.ApiKeyIdentityProvider
      args:
        header: x-api-key
        keys:
          - id: batch-job
            secret:
              key: BATCH_API_KEY
            groups: ["batch"]
            attributes:
              tenant: acme
    - module: dal_obscura.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider
      args:
        issuer: https://keycloak.example.com/realms/acme
        audience: dal-obscura
```

Providers run in order. Missing credentials fall through to the next provider;
invalid credentials stop the chain and reject the request.

### Transport TLS

Flight server TLS is configured separately from authentication so you can run
plain bearer-token, mTLS, or mixed deployments:

```yaml
transport:
  tls:
    cert:
      key: SERVER_CERT
    key:
      key: SERVER_KEY
    client_ca:
      key: CLIENT_CA
    verify_client: true
```

Set `verify_client: true` and `client_ca` when you want client certificates
available to `MtlsIdentityProvider`.

### Runnable Auth Examples

`examples/auth/` contains Docker Compose examples for every built-in
authentication approach:

- shared JWT
- Keycloak OIDC
- API key
- local mTLS
- SPIFFE/SPIRE mTLS
- trusted headers through a real Flight gateway
- composite provider chains

Each example is self-contained, starts the real auth backend for that flow, and
leaves a client container running so you can issue more reads with
`docker compose exec`. See
[examples/auth/README.md](/Users/nithish/.codex/worktrees/601a/dal-obscura/examples/auth/README.md)
for the shared run pattern and per-provider caveats.

Each example starts a real `dal-obscura` server, authenticates a real client, and
executes a real Flight read without external services. See
`examples/auth/README.md` for commands and caveats.

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

Security-focused checks:

```bash
uv run pytest tests/domain/access_control/test_row_filters.py tests/interfaces/flight/test_service_streaming.py::test_parse_descriptor_rejects_unsafe_row_filter_sql tests/infrastructure/adapters/test_duckdb_transform.py -q
```

## Pre-commit hooks

After `uv sync --dev`, install the hooks with `uv run pre-commit install`. The configured hooks run `uv run ruff format` and `uv run ruff check` (each passed the staged python files), plus `uv run ty check` and `uv run pytest --maxfail=1 --disable-warnings` on every commit to guard formatting, linting, typing, and a quick smoke test. Re-run them manually with `uv run pre-commit run --all-files` if needed.

## Notes
- Mask expressions are executed in DuckDB SQL.
- Supported mask types include `null`, `redact`, `hash`, `default`, `email`, and `keep_last`.
- `hash`, `redact`, `email`, and `keep_last` expose masked values as Arrow `string`; `default` exposes the Arrow type DuckDB infers for the configured literal, while `null` preserves the original field type.
- Row filters are parsed with SQLGlot using the DuckDB dialect, must contain exactly one expression, and are validated against a small allowlist before execution.
- Supported row-filter shapes are boolean columns, comparisons, `AND`/`OR`/`NOT`, scalar arithmetic inside comparisons, `IN` with scalar literal lists, `IS NULL`/`IS NOT NULL`, `LOWER(...)`, `COALESCE(...)`, and `CAST(...)`.
- Row filters reject SQL statements, multi-statement input, subqueries, table functions such as `read_csv(...)`, extension commands, `COPY`, `ATTACH`, DDL, and DML.
- DuckDB row-transform execution runs over Arrow batches with external access and extension auto-loading disabled.
- Iceberg planning pushes down a safe subset of validated row filters for top-level fields, but DuckDB re-applies the full effective filter during fetch so backend pushdown remains an optimization rather than the final enforcement point.
- Nested field masks use DuckDB `struct_update`, and list-of-struct masks use `list_transform` plus `struct_update`.

## Current Limitations
- Iceberg pushdown is currently conservative: mixed `AND` predicates split, but nested-field predicates and unsupported shapes remain residual-only.
- ABAC conditions currently support exact principal-attribute matches and membership in an explicit allowed-value list.
- Collection masking currently targets list-of-struct paths and does not yet support arbitrary scalar lists or deeper heterogeneous container rewrites.
- Tickets still serialize Python scan tasks directly, which keeps the transport format tied to Python internals.

## Logging
- JSON logs by default.
- Configure level and JSON output in `app.yaml` under `logging`.
