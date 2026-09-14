# Quickstart

This guide gets you to a working dal-obscura environment first, then shows the
manual service shape used in real deployments.

## Contents

- [Run The Local Demo](#run-the-local-demo)
- [What Starts](#what-starts)
- [Verify The Environment](#verify-the-environment)
- [Publish With The Operator CLI](#publish-with-the-operator-cli)
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
    cli["Operator CLI"] --> db[("Postgres")]
    cli --> catalog["Iceberg catalog"]
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
5. Published Iceberg asset, policies, masks, and row filters.
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

## Publish With The Operator CLI

Use a versioned manifest from an administrative host. The CLI has no
reader-facing authoring endpoint:

```bash
uv run dal-obscura-admin validate examples/manifests/iceberg-gateway.json
uv run dal-obscura-admin preview examples/manifests/iceberg-gateway.json \
  --personas examples/manifests/personas.json
uv run dal-obscura-admin publish examples/manifests/iceberg-gateway.json \
  --database-url "$DAL_OBSCURA_DATABASE_URL" --expected-generation none
uv run dal-obscura-admin status --database-url "$DAL_OBSCURA_DATABASE_URL"
```

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

1. OIDC/JWKS reader identity.
2. Iceberg catalog connection.
3. Governed asset and policy.
4. Runtime ticket settings.

Use the operator CLI manifest. It defines the qualified catalog/table-format and OIDC runtime,
catalogs, assets, policies, and settings.

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
