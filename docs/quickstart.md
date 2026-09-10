# Quickstart

This guide gets you to a working dal-obscura environment first, then shows the
manual service shape used in real deployments.

## Contents

- [Run The Local Demo](#run-the-local-demo)
- [What Starts](#what-starts)
- [Verify The Environment](#verify-the-environment)
- [Use The UI](#use-the-ui)
- [Stop Or Reset](#stop-or-reset)
- [Manual Service Shape](#manual-service-shape)
- [Next Reads](#next-reads)

## Run The Local Demo

Prerequisites:

- Docker with Compose v2.
- Python 3 for the local `./run` helper.

Start the complete local environment:

```bash
cd examples/demo/keycloak
./run up
```

The demo builds from the current checkout unless `DAL_OBSCURA_IMAGE` points at a
prebuilt image.

## What Starts

```mermaid
flowchart LR
    browser["Browser"] --> ui["UI :8821"]
    ui --> api["Control plane API :8820"]
    api --> db[("Postgres")]
    api --> keycloak["Keycloak :8080"]
    api --> catalog["Iceberg catalog"]
    client["Flight client"] --> dp["Data plane :8815"]
    dp --> db
    dp --> keycloak
    dp --> storage["Demo table files"]
```

The demo provisions:

1. Keycloak realm and demo users.
2. Postgres-backed config store.
3. Operator CLI and published configuration store.
4. Iceberg demo table.
5. Catalog discovery and governed assets.
6. Asset owners, policies, masks, row filters, and active policy versions.
7. Arrow Flight data plane.

Open:

- Keycloak: `http://127.0.0.1:8080`
- Flight data plane: `grpc://127.0.0.1:8815`

## Verify The Environment

Run the smoke checks:

```bash
./run smoke
```

Run individual read checks:

```bash
./run read --as us-analyst
./run read --as eu-analyst
./run read --as data-steward
./run read --as blocked-user
```

Expected behavior:

- `us-analyst` reads US rows with masked email values.
- `eu-analyst` reads EU rows with masked email values.
- `data-steward` reads all rows with clear email values.
- `blocked-user` is denied by policy.

## Use The UI

Print demo credentials:

```bash
./run credentials
```

Open `http://127.0.0.1:8821`.

Useful personas:

- `demo-admin`: platform administration.
- `asset-owner`: owner workflow for editing policy and submitting policy
  versions.
- `us-analyst`, `eu-analyst`, `data-steward`: read-path policy behavior.
- `blocked-user`: denied principal.

Suggested UI path:

1. Sign in as `demo-admin`.
2. Open Catalogs and confirm both demo catalogs are discovered.
3. Open Assets and inspect `retail.customer_revenue`.
4. Sign in as `asset-owner`.
5. Edit a row filter or mask and submit a new policy version.
6. Sign in as an analyst and confirm policy controls are not editable.

## Stop Or Reset

Stop containers but keep generated files and Postgres volume:

```bash
./run down
```

Delete containers, generated files, and demo database state:

```bash
./run reset
```

## Manual Service Shape

Use this section when you want to understand the production-shaped runtime
instead of the all-in-one demo.

### Install

```bash
uv sync --dev --extra server --extra postgres
uv run dal-obscura --help
uv run dal-obscura-admin --help
uv run dal-obscura-migrate --help
```

SQLite works for short-lived local development:

```bash
uv sync --dev --extra server --extra sqlite
export DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:///runtime/control-plane.db
```

Use Postgres for shared environments or state that must survive restarts
reliably.

### Initialize A Publication

```bash
export DAL_OBSCURA_DATABASE_URL=postgresql+psycopg://dal_obscura:dal_obscura@127.0.0.1:5432/dal_obscura
uv run dal-obscura-migrate upgrade
uv run dal-obscura-migrate check
uv run dal-obscura-admin status
```

### Configure The Service

Configure at least:

1. IAM provider.
2. Catalog connection.
3. Catalog discovery.
4. Governed asset.
5. Asset owners.
6. Active policy version.

Use the operator CLI manifest. It defines catalogs, assets, policies, policy versions, and
settings.

### Start A Data Plane

```bash
export DAL_OBSCURA_DATABASE_URL=postgresql+psycopg://dal_obscura:dal_obscura@127.0.0.1:5432/dal_obscura
uv run dal-obscura-migrate check
export DAL_OBSCURA_CELL_ID=00000000-0000-0000-0000-000000000001
export DAL_OBSCURA_LOCATION=grpc://127.0.0.1:8815
export DAL_OBSCURA_TICKET_SECRET=replace-with-a-secret
uv run dal-obscura
```

The data plane reads published configuration from the config database,
authenticates each request, mints opaque tickets during planning, and verifies
the active policy version again during streaming.

## Next Reads

- [Concepts](concepts.md): understand assets, policies, tickets, and read flow.
- [Policy Authoring](policy-authoring.md): define grants, filters, and masks.
- [Operators](operators.md): prepare a shared or persistent environment.
- [Security](security.md): review identity providers, tickets, and secret
  handling.
