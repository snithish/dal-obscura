# dal-obscura

[![CI](https://github.com/snithish/dal-obscura/actions/workflows/ci.yml/badge.svg)](https://github.com/snithish/dal-obscura/actions/workflows/ci.yml)
[![Container Image](https://img.shields.io/badge/GHCR-dal--obscura-2496ED?logo=docker&logoColor=white)](https://github.com/snithish/dal-obscura/pkgs/container/dal-obscura)
[![GitHub Release](https://img.shields.io/github/v/release/snithish/dal-obscura)](https://github.com/snithish/dal-obscura/releases)
[![GitHub Stars](https://img.shields.io/github/stars/snithish/dal-obscura?style=social)](https://github.com/snithish/dal-obscura/stargazers)
[![License](https://img.shields.io/github/license/snithish/dal-obscura)](https://github.com/snithish/dal-obscura/blob/main/LICENSE)

Data access layer with Arrow Flight, Iceberg, Delta Lake, Unity Catalog,
masking, and row filters.

## Highlights
- Arrow Flight plan/ticket flow with HMAC-signed, DB-backed tickets
- Iceberg v2/v3 backend (pyiceberg)
- Delta Lake backend (delta-rs) and file-backed Parquet/CSV/JSON/ORC/Avro/Text assets
- Unity Catalog metadata resolution for Delta and file-backed tables
- API-first FastAPI control plane for configuration provisioning
- RDBMS-backed published configuration consumed by data planes
- DuckDB-powered row filters and column masks

## Architecture
- `domain/`
  - `access_control`: principals, policy models, access resolution.
  - `query_planning`: plan/read payload models and filter primitives.
  - `ticket_delivery`: ticket payload value object.
- `application/`
  - `ports`: hexagonal contracts (`IdentityPort`, `AuthorizationPort`, backend, masking, ticket, row transform).
  - `use_cases`: `PlanAccessUseCase` and `FetchStreamUseCase`.
- `infrastructure/adapters/`
  - JWT/OIDC/API-key/mTLS/trusted-header identity providers, published-config authorizer, Iceberg/Delta/file backends, Unity Catalog resolver, DuckDB masking/row transform, HMAC ticket codec.
- `interfaces/`
  - `flight`: Arrow Flight transport adapter.
  - `cli`: composition root and runtime wiring.
  - `control_plane`: FastAPI provisioning and policy-version API.

## Requirements
- Python 3.10+
- `uv` for package management

## Quickstart (uv)

```bash
uv venv
uv sync
uv run dal-obscura --help
```

## Documentation

Start with [docs/README.md](docs/README.md) for user, asset-owner, operator,
security, connector, and contributor guides.

For the fastest hands-on path, read [docs/quickstart.md](docs/quickstart.md).

## Container Image

The repo root [Dockerfile](Dockerfile)
builds the reusable production `dal-obscura` image for both the data plane and
control plane. CI publishes it to GitHub Container Registry as
`ghcr.io/snithish/dal-obscura`. It uses a multi-stage build, installs the
locked runtime dependency set with `uv`, omits development dependencies and
build tooling from the final stage, and runs as a non-root user.

Use the published image directly:

```bash
docker pull ghcr.io/snithish/dal-obscura:latest
```

Start the data plane, which is the default image command:

```bash
docker run --rm \
  -e DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:////runtime/control-plane.db \
  -e DAL_OBSCURA_CELL_ID=00000000-0000-0000-0000-000000000001 \
  -e DAL_OBSCURA_LOCATION=grpc://0.0.0.0:8815 \
  -e DAL_OBSCURA_TICKET_SECRET=dev-ticket-secret \
  -p 8815:8815 \
  ghcr.io/snithish/dal-obscura:latest
```

Start the control plane from the same image by selecting the `control-plane`
role:

```bash
docker run --rm \
  -e DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:////runtime/control-plane.db \
  -e DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN=dev-admin \
  -p 8820:8820 \
  ghcr.io/snithish/dal-obscura:latest control-plane
```

The entrypoint also accepts direct commands for operational debugging and
one-off container tasks.

To test changes to the Dockerfile itself, build a local image with:

```bash
docker build -t dal-obscura:local .
```

CI publishes production images to GitHub Container Registry:

```bash
docker pull ghcr.io/snithish/dal-obscura:sha-<commit>
docker pull ghcr.io/snithish/dal-obscura:latest
docker pull ghcr.io/snithish/dal-obscura:v1.2.3
```

Pushes to `main` publish only a `sha-<commit>` image tag. Pushed Git tags
matching `v*` or numeric release tags publish the exact tag plus `latest`;
semver tags such as `v1.2.3` also publish `1.2.3`, `1.2`, and `1`. Published
images are multi-arch manifests for `linux/amd64` and `linux/arm64`.

The runnable auth examples under
[examples/auth](examples/auth/README.md)
reuse that image and keep provider-specific infrastructure in each example
directory.

For an optional local reference environment with Keycloak IAM, the
control-plane UI, catalog discovery, policy ownership, and authenticated Flight
reads, see
[examples/demo/keycloak](examples/demo/keycloak/README.md).

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

## Running

Start the control plane against an RDBMS URL:

```bash
export DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:///runtime/control-plane.db
export DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN=dev-admin
uv run dal-obscura-control-plane
```

Open the API docs:

```bash
open http://localhost:8820/docs
```

The operator dashboard runs as a separate React app. All state reads and writes
go through the protected `/v1` API. The admin token is not rendered into the UI
HTML.

For frontend development, run the API and Vite dev server:

```bash
uv run dal-obscura-control-plane
cd ui
pnpm install
pnpm dev
```

For a production-like local UI run, build and serve the Caddy image:

```bash
docker build -f ui/Dockerfile -t dal-obscura-control-plane-ui:local .
docker run --rm -p 127.0.0.1:8821:8080 \
  -e DAL_OBSCURA_API_BASE_URL=http://127.0.0.1:8820 \
  dal-obscura-control-plane-ui:local
```

See [docs/frontend.md](docs/frontend.md) for frontend conventions, pnpm
supply-chain settings, and dependency admission rules.

Provision workspace catalogs, assets, policy rules, owners, runtime settings,
and auth providers through the HTTP API, then submit asset-scoped policy
versions. Tenant and cell records remain internal runtime partitioning data.
Start each data plane from the same database without calling the control-plane
service:

```bash
export DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:///runtime/control-plane.db
export DAL_OBSCURA_CELL_ID=00000000-0000-0000-0000-000000000001
export DAL_OBSCURA_LOCATION=grpc://0.0.0.0:8815
export DAL_OBSCURA_TICKET_SECRET=dev-ticket-secret
uv run dal-obscura
```

Clients authenticate through whichever provider chain is configured. Header-based
providers inspect Flight headers on both `get_flight_info` and `do_get`, while
mTLS providers can authenticate from the verified Flight peer identity alone.

Clients send `get_schema` and `get_flight_info` requests with a protobuf
`dal_obscura.flight.v1.PlanRequest` in `FlightDescriptor.command`. The Flight
surface does not accept JSON command payloads.

```proto
message PlanRequest {
  uint32 protocol_version = 1;
  string catalog = 2;
  string target = 3;
  repeated string columns = 4;
  string row_filter = 5;
}
```

Engine `row_filter` values are validated as DuckDB SQL expressions. They may
reference columns that are not projected back to the client, but only when
those columns are plainly visible and unmasked under policy. The service
combines the engine filter with any policy filter using `AND`, pushes down the
safe subset during backend planning, and evaluates any unsupported remainder in
DuckDB during streaming.

Runtime secrets are resolved through the explicitly configured secret provider
module from references stored in published auth-provider arguments. The default
provider resolves references such as `{"secret": "DAL_OBSCURA_JWT_SECRET"}`
from environment variables; the control plane stores references, not secret
values.

### Table Backends

Assets can use these read-only backends:

- `iceberg`: Iceberg v2/v3 tables resolved through PyIceberg catalogs.
- `delta`: Delta Lake tables resolved from a table root path with delta-rs.
- `parquet`, `csv`, `json`, `orc`: file-backed tables read through PyArrow Dataset.
- `avro`: Avro object-container files read through fastavro.
- `text`: line-oriented text files exposed as one string column, default `value`.

Delta and file-backed targets must be resolved through a catalog. Static
catalog entries can carry `table_identifier` values that point at table roots or
file paths, and `options.storage_options` can carry object-store options. Those
options can use the same secret-reference pattern as other published runtime
configuration.

Catalogs resolve governed targets directly into executable table formats:

- A catalog implements `CatalogPlugin.resolve_table()` and returns a
  `TableFormat`. Built-in catalog types are configured with `type`, not Python
  module strings.
- A `TableFormat` owns schema extraction, task planning, and execution. Planning
  should split work into parallel scan tasks whenever the backend exposes
  splittable work such as files, fragments, partitions, or row groups. If a
  backend cannot be split safely, that limitation should be documented with the
  expected performance drawback.

This split lets path-backed providers such as Delta, Iceberg, and Parquet
coexist with future non-path providers such as relational databases, MongoDB, or
HTTP APIs. Non-path providers can use descriptor fields such as
`table_identifier`, `options`, and `properties` without inventing a fake file
location.

Catalog implementations resolve governed targets into executable table readers.
Built-in catalog config uses `type`; Python module strings are not part of the
public catalog config format.

Custom catalog types should implement `CatalogPlugin.resolve_table()` and return
an executable `TableFormat` directly. The old provider-registry extension point
is intentionally gone; do not configure `provider_modules` or make catalogs
return provider-neutral descriptors.

File-backed catalogs use typed config:

```json
{
  "name": "warehouse",
  "type": "files",
  "options": {
    "format": "parquet",
    "location": "s3://warehouse/users/",
    "storage_options": {"AWS_REGION": "us-east-1"}
  }
}
```

Unity Catalog is configured as a typed catalog:

```json
{
  "name": "main",
  "type": "unity",
  "options": {
    "base_url": "https://workspace.example.com",
    "token": {"secret": "UNITY_CATALOG_TOKEN"},
    "uc_catalog": "main",
    "credential_mode": "both",
    "storage_options": {"AWS_REGION": "us-east-1"}
  }
}
```

`credential_mode` accepts `configured`, `uc_temp`, or `both`. In `both` mode,
the resolver tries Unity Catalog temporary read credentials first and falls back
to configured storage options only when credential vending is unavailable. Views,
materialized views, streaming tables, missing storage locations, and unsupported
table formats are rejected before tickets are minted.

### Provisioning Flow

The public control-plane API is workspace-first. Tenant and cell identifiers are
internal runtime concerns, not public workspace API inputs.

Call:

- `PUT /v1/settings/runtime`
- `PUT /v1/catalogs/{name}`
- `GET /v1/catalogs/{name}/tables`
- `PUT /v1/assets/{catalog}/{target}`
- `PUT /v1/assets/{asset_id}/owners`
- `PUT /v1/assets/{asset_id}/schema-fields`
- `PUT /v1/assets/{asset_id}/policy-rules`
- `PUT /v1/settings/auth-providers`
- `POST /v1/assets/{asset_id}/policy-versions`
- `GET /v1/policy-versions`

Runtime ticket settings include `ticket_ttl_seconds`, `max_tickets`, and
`max_ticket_exchanges`. `max_ticket_exchanges` limits how many successful
`do_get` exchanges a planned ticket can reserve before it is rejected.

```json
{
  "ticket_ttl_seconds": 900,
  "max_tickets": 64,
  "max_ticket_exchanges": 2
}
```

Runtime configuration does not accept standalone storage paths. Table discovery
and physical locations must come from catalog definitions and governed assets.

Flight tickets are opaque HMAC-signed references. The executable scan payload,
including the pickled internal `ScanTask`, is stored server-side in the
runtime ticket table and hash-checked before use. Pickle is only trusted for
that DB-stored internal payload; clients never receive the pickled scan task in
the Flight ticket.

`do_get` rejects tickets whose stored policy version no longer matches the
active asset policy version. Stale tickets are invalid even if they have not
expired and still have remaining exchanges.

### Authentication Providers

Each published auth-provider `module` names a Python class that implements
`IdentityPort`:

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

Single-provider auth is represented as one published provider record. For
example, an OIDC provider uses `module:
dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider`
with args for `issuer`, `audience`, `subject_claim`, `group_claims`, and
`attribute_claims`.

The OIDC/JWKS provider validates access tokens locally. It can discover the
issuer JWKS automatically, or you can pin static keys with `jwks` or
`jwks_file` for offline and on-prem deployments.

Multi-provider chains let one deployment accept several credential families by
publishing several provider records with ascending `ordinal` values.

Providers run in order. Missing credentials fall through to the next provider;
invalid credentials stop the chain and reject the request.

### Breaking Changes

- Public tenant and cell endpoints were removed.
- Public publication endpoints were replaced by asset-scoped policy-version history.
- Catalog config now uses typed catalog entries instead of Python module strings.
- Catalogs now resolve executable table readers directly; the table provider
  registry extension point was removed.

### Transport TLS

Flight server TLS is configured with data-plane environment variables so you can
run plain bearer-token, mTLS, or mixed deployments:

```bash
export DAL_OBSCURA_LOCATION=grpc+tls://0.0.0.0:8815
export DAL_OBSCURA_TLS_CERT="$(cat server.crt)"
export DAL_OBSCURA_TLS_KEY="$(cat server.key)"
export DAL_OBSCURA_TLS_CLIENT_CA="$(cat ca.crt)"
export DAL_OBSCURA_TLS_VERIFY_CLIENT=true
```

Set `DAL_OBSCURA_TLS_VERIFY_CLIENT=true` and `DAL_OBSCURA_TLS_CLIENT_CA` when
you want client certificates available to `MtlsIdentityProvider`.

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
`docker compose exec`. You can run examples from their own directories, or use
`examples/auth/run <example> up|logs|read|down` from the repository root. See
[examples/auth/README.md](examples/auth/README.md)
for the shared run pattern and per-provider caveats.

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff check .
uv run ruff format .
uv run ty check
```

GitHub Actions runs the same Python checks, verifies the JVM connector
workspace with `mvn -f connectors/jvm/pom.xml verify`, builds the production
Docker image, scans it with Trivy for high and critical vulnerabilities, and
publishes to GHCR on `main` and release tag pushes.

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

After `uv sync --dev`, install the hooks with `uv run pre-commit install`. The configured hooks run `uv run ruff format` and `uv run ruff check` (each passed the staged python files), plus `uv run ty check` and `uv run pytest -m "not heavy" --maxfail=1 --disable-warnings` on every commit to guard formatting, linting, typing, and non-heavy tests. Heavy suites are marked with `@pytest.mark.heavy` and remain available through explicit `uv run pytest` or benchmark commands. Re-run the hooks manually with `uv run pre-commit run --all-files` if needed.

## Notes
- Mask expressions are executed in DuckDB SQL.
- Supported mask types include `null`, `redact`, `hash`, `default`, `email`, and `keep_last`.
- `hash`, `redact`, `email`, and `keep_last` expose masked values as Arrow `string`; `default` exposes the Arrow type DuckDB infers for the configured literal, while `null` preserves the original field type.
- Row filters are parsed with SQLGlot using the DuckDB dialect, must contain exactly one expression, and are validated against a small allowlist before execution.
- Supported row-filter shapes are boolean columns, comparisons, `AND`/`OR`/`NOT`, scalar arithmetic inside comparisons, `IN` with scalar literal lists, `IS NULL`/`IS NOT NULL`, `LOWER(...)`, `COALESCE(...)`, and `CAST(...)`.
- Row filters reject SQL statements, multi-statement input, subqueries, table functions such as `read_csv(...)`, extension commands, `COPY`, `ATTACH`, DDL, and DML.
- Published policy row filters are validated against the same allowlisted query shape before activation.
- DuckDB row-transform execution runs over Arrow batches with external access and extension auto-loading disabled.
- Iceberg planning pushes down a safe subset of validated row filters for top-level fields, but DuckDB re-applies the full effective filter during fetch so backend pushdown remains an optimization rather than the final enforcement point. Delta and PyArrow Dataset-backed reads translate simple validated filters into Arrow Dataset expressions for scan planning and execution. Avro and text reads can apply translated filters during task execution, but still report the filter as residual because they read whole files. Unity Catalog participates in metadata resolution; the resolved table provider determines actual pushdown behavior.
- Standalone path reads are not a supported discovery path. Catalog definitions
  resolve physical table locations before tickets are minted.
- Nested field masks use DuckDB `struct_update`, and list-of-struct masks use `list_transform` plus `struct_update`.

## Current Limitations
- Iceberg pushdown is currently conservative: mixed `AND` predicates split, but nested-field predicates and unsupported shapes remain residual-only.
- ABAC conditions currently support exact principal-attribute matches and membership in an explicit allowed-value list.
- Collection masking currently targets list-of-struct paths and does not yet support arbitrary scalar lists or deeper heterogeneous container rewrites.
- Tickets still persist Python scan tasks server-side for execution, which keeps the DB payload format tied to Python internals.

## Logging
- JSON logs by default.
- Configure level with `DAL_OBSCURA_LOG_LEVEL` and JSON output with
  `DAL_OBSCURA_JSON_LOGS`.
