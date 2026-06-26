# Quickstart

This guide gets you from a clean checkout to a working dal-obscura service. It
uses local defaults where possible, but the shape matches a real deployment:
control plane, data plane, IAM, catalog, and persistent configuration.

## What You Are Starting

```mermaid
flowchart LR
    user["User or client"] --> cp["Control plane API and UI"]
    client["Flight client"] --> dp["Data plane"]
    cp --> db[("Config database")]
    dp --> db
    cp --> iam["IAM provider"]
    dp --> iam
    cp --> catalog["Catalog"]
    dp --> catalog
    dp --> storage["Table storage"]
```

## Prerequisites

- Python 3.10+.
- `uv`.
- A SQL database for persistent configuration. Use Postgres for shared or
  restart-stable environments.
- An IAM provider such as OIDC/JWKS, API keys, mTLS, trusted headers, or a
  composite provider.
- A configured catalog and table storage.

## Install

```bash
uv sync --dev
uv run dal-obscura --help
```

## Start A Control Plane

Set a persistent database URL and bootstrap admin token:

```bash
export DAL_OBSCURA_DATABASE_URL=postgresql+psycopg://dal_obscura:dal_obscura@127.0.0.1:5432/dal_obscura
export DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN=dev-admin
export DAL_OBSCURA_CONTROL_PLANE_HOST=127.0.0.1
export DAL_OBSCURA_CONTROL_PLANE_PORT=8820
uv run dal-obscura-control-plane
```

Open the UI:

```text
http://127.0.0.1:8821
```

Swagger docs are available from the API process:

```text
http://127.0.0.1:8820/docs
```

For short-lived local development, SQLite also works:

```bash
export DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:///runtime/control-plane.db
```

Use Postgres when other people will use the environment or when state must
survive restarts reliably.

## Configure The Service

```mermaid
sequenceDiagram
    participant Admin
    participant CP as "Control plane"
    participant DB as "Config database"
    participant Catalog

    Admin->>CP: Configure IAM
    Admin->>CP: Configure catalog
    CP->>Catalog: Discover tables
    Admin->>CP: Promote table to asset
    Admin->>CP: Assign owners
    Admin->>CP: Publish policy version
    CP->>DB: Store active configuration
```

At minimum, configure through the workspace API or UI:

1. IAM provider.
2. Catalog connection.
3. Catalog discovery.
4. Governed asset.
5. Asset owners.
6. Active policy version.

## Start A Data Plane

```bash
export DAL_OBSCURA_DATABASE_URL=postgresql+psycopg://dal_obscura:dal_obscura@127.0.0.1:5432/dal_obscura
export DAL_OBSCURA_CELL_ID=00000000-0000-0000-0000-000000000001
export DAL_OBSCURA_LOCATION=grpc://127.0.0.1:8815
export DAL_OBSCURA_TICKET_SECRET=replace-with-a-secret
uv run dal-obscura
```

The data plane reads published configuration from the config database, verifies
identity, mints opaque tickets during planning, and applies the active policy
version during streaming.

## Verify The Read Path

```mermaid
flowchart LR
    identity["Valid identity"] --> info["get_flight_info"]
    info --> ticket["Opaque ticket"]
    ticket --> get["do_get"]
    get --> rows["Filtered and masked rows"]
```

Use one allowed principal and one denied principal. A useful first check is:

- The allowed principal receives only authorized columns.
- Row filters remove rows outside that principal's scope.
- Masks apply to sensitive columns.
- The denied principal receives an authorization failure.

## Optional Local Reference

The repository includes a complete Keycloak/Postgres/Iceberg example for local
evaluation:

```bash
cd examples/demo/keycloak
./run up
```

Treat it as one reference deployment, not the required way to run the service.
See [`examples/demo/keycloak/README.md`](../examples/demo/keycloak/README.md).
