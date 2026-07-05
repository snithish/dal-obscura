# dal-obscura

[![CI](https://github.com/snithish/dal-obscura/actions/workflows/ci.yml/badge.svg)](https://github.com/snithish/dal-obscura/actions/workflows/ci.yml)
[![Container Image](https://img.shields.io/badge/GHCR-dal--obscura-2496ED?logo=docker&logoColor=white)](https://github.com/snithish/dal-obscura/pkgs/container/dal-obscura)
[![GitHub Release](https://img.shields.io/github/v/release/snithish/dal-obscura)](https://github.com/snithish/dal-obscura/releases)
[![License](https://img.shields.io/github/license/snithish/dal-obscura)](https://github.com/snithish/dal-obscura/blob/main/LICENSE)

Governed analytical data access layer for Arrow Flight reads. Platform teams
register catalogs and assets, asset owners submit policy versions, and clients
receive only the rows and columns allowed by the active asset policy.

dal-obscura currently supports Iceberg, Delta Lake, Unity Catalog resolution,
file-backed assets, DuckDB row filters, column masks, and JVM/Python connector
surfaces.

## Contents

- [Why dal-obscura](#why-dal-obscura)
- [Fast start](#fast-start)
- [Documentation](#documentation)
- [Architecture](#architecture)
- [Runtime model](#runtime-model)
- [Connectors](#connectors)
- [Development](#development)
- [Status and limits](#status-and-limits)

## Why dal-obscura

- Governed reads through Arrow Flight plan/ticket flow.
- Asset-scoped policy versions for row filters, column grants, and masks.
- Control plane for catalogs, assets, owners, policies, settings, and auth.
- Stateless data-plane serving over published configuration.
- HMAC-signed, DB-backed tickets with policy-version checks on fetch.
- DuckDB execution for residual row filters and masking.
- Spark/JVM and Python connector entry points.

## Fast start

Use the local Keycloak demo when you want a complete working stack with IAM,
Postgres, control plane, UI, Iceberg, Delta, and Flight reads:

```bash
cd examples/demo/keycloak
./run up
./run smoke
```

Open:

- UI: `http://127.0.0.1:8821`
- API docs: `http://127.0.0.1:8820/docs`
- Flight data plane: `grpc://127.0.0.1:8815`

Stop the demo without deleting state:

```bash
./run down
```

For package setup and command discovery from the repo root:

```bash
uv sync --dev --extra server --extra sqlite
uv run dal-obscura --help
uv run dal-obscura-control-plane --help
uv run dal-obscura-migrate --help
```

## Documentation

Start with [docs/README.md](docs/README.md). It groups docs by user need.

| Need | Read |
| --- | --- |
| Try the service | [Quickstart](docs/quickstart.md) |
| Understand the model | [Concepts](docs/concepts.md) |
| Author policies | [Policy Authoring](docs/policy-authoring.md) |
| Run the service | [Operators](docs/operators.md) and [Operator Runbook](docs/operators-runbook.md) |
| Review risk | [Security](docs/security.md) |
| Integrate clients | [Connectors](docs/connectors.md) and [connectors/README.md](connectors/README.md) |
| Contribute | [Development](docs/development.md) and [Frontend Conventions](docs/frontend.md) |

## Architecture

```mermaid
flowchart LR
    owner["Asset owner"] --> cp["Control plane API/UI"]
    admin["Platform admin"] --> cp
    cp --> db[("Config database")]
    cp --> catalog["Catalogs"]
    reader["Client"] --> dp["Arrow Flight data plane"]
    dp --> db
    dp --> catalog
    dp --> table["Table storage"]
    dp --> duckdb["DuckDB filters/masks"]
```

The public model is workspace-first: catalogs, assets, owners, policies, policy
versions, and settings. Tenant and cell identifiers are internal runtime
partitioning details, not primary user-facing concepts.

Key paths:

| Path | Purpose |
| --- | --- |
| `src/dal_obscura/control_plane` | FastAPI control-plane API, repositories, and workflows. |
| `src/dal_obscura/data_plane` | Arrow Flight service, planning, fetching, auth, transforms. |
| `src/dal_obscura/common` | Shared policy, catalog, table-format, ticket, and config-store code. |
| `ui` | React/Vite control-plane UI served separately from the API. |
| `connectors` | JVM client, Spark datasource, testkit, and contract fixtures. |
| `examples` | Auth examples, local demo, and sample manifests. |
| `tests` | Unit, integration, smoke, and benchmark tests. |

## Runtime model

Run schema migrations explicitly before starting services:

```bash
uv run dal-obscura-migrate upgrade
uv run dal-obscura-migrate check
```

Start the control plane against the config database:

```bash
export DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:///runtime/control-plane.db
export DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN=dev-admin
uv run dal-obscura-control-plane
```

Start each data plane from the same published configuration:

```bash
export DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:///runtime/control-plane.db
export DAL_OBSCURA_CELL_ID=00000000-0000-0000-0000-000000000001
export DAL_OBSCURA_LOCATION=grpc://127.0.0.1:8815
export DAL_OBSCURA_TICKET_SECRET=replace-with-a-secret
uv run dal-obscura
```

The control-plane API exposes `/docs`, `/redoc`, and `/openapi.json`. The React
UI runs as a separate Caddy/Vite build and reads runtime config from
`/config.json`; see [docs/frontend.md](docs/frontend.md).

The published container image is `ghcr.io/snithish/dal-obscura`. The same image
can run migration, control-plane, and data-plane commands. See
[docs/operators.md](docs/operators.md) for production-oriented setup.

## Connectors

Connector implementations live under `connectors/`.

- `jvm/dal-obscura-client-java`: engine-agnostic Java Flight client.
- `jvm/spark3-datasource`: Spark 3.x DataSource V2 reader.
- `jvm/connector-testkit-jvm`: shared JVM integration helpers.
- `contract-fixtures`: language-neutral protocol fixtures.

Build and verify the JVM connector workspace:

```bash
mvn -f connectors/jvm/pom.xml verify
```

## Development

```bash
uv sync --dev --extra server --extra sqlite
uv run ruff check .
uv run ruff format .
uv run ty check
uv run pytest
```

JVM connector checks:

```bash
mvn -f connectors/jvm/pom.xml verify
```

Frontend checks:

```bash
pnpm --filter @dal-obscura/control-plane-ui test
pnpm --filter @dal-obscura/control-plane-ui build
```

Benchmark suites are available under `tests/benchmarks`; use JSON output as the
before/after artifact for planner, masking, filtering, and table-format changes.

## Status and limits

- Row filters and masks are DuckDB SQL expressions.
- Catalogs resolve governed targets into executable table formats.
- Standalone path reads are not a public discovery path.
- Iceberg pushdown is conservative; residual filtering is reapplied in DuckDB.
- ABAC conditions currently support exact principal-attribute matches and
  explicit allowed-value lists.
- Tickets persist trusted internal Python scan tasks server-side, so the DB
  ticket payload format is tied to Python internals.
