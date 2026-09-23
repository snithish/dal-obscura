# Quickstart

Start with the authenticated local demo, then use the manual service steps for a
deployment you configure yourself.

## Run the local demo

Prerequisites: Docker with Compose v2 and Python 3.

```bash
cd examples/demo/keycloak
./run up
./run credentials
```

The demo starts Keycloak, PostgreSQL, the authenticated control-plane UI, a
seeded Iceberg catalog and governed assets, and the Arrow Flight data plane.
Open the UI URL printed by `./run credentials`, choose **Sign in with SSO**, and
sign in as `demo-admin` with the generated password. The `asset-owner` account
can edit policy for the demo asset. Reader personas are
`us-analyst`, `eu-analyst`, `data-steward`, and `blocked-user`.

Run the end-to-end seeded read checks:

```bash
./run smoke
./run read --as us-analyst
./run read --as eu-analyst
./run read --as data-steward
./run read --as blocked-user
```

The local demo preserves its PostgreSQL volume and generated files across
restarts. If you already have state from an earlier checkout, reset the
disposable demo before its first start with the new schema:

```bash
./run reset
./run up
```

`reset` deletes the local demo's generated files and database volume.

## Start services manually

The supported config-store migration history is one clean baseline for live
catalogs, assets, policy, tickets, sessions, and audit records. This is a
breaking schema reset: databases stamped with the previous migration history
are unsupported. For a disposable environment, create a new empty database.

Install the server, SQLite driver, and UI dependencies:

```bash
uv sync --dev --extra server --extra sqlite
pnpm --dir apps/governance-ui install --frozen-lockfile
```

For a local authoring-only run, configure a fresh SQLite database and apply the
baseline explicitly:

```bash
mkdir -p runtime
export DAL_OBSCURA_DATABASE_URL='sqlite+pysqlite:///runtime/control-plane.db'
uv run dal-obscura-migrate upgrade
uv run dal-obscura-migrate check
```

In the first terminal, start the authenticated local control plane:

```bash
export DAL_OBSCURA_CONTROL_PLANE_PORT=8821
export DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN="$(openssl rand -hex 32)"
printf 'Bootstrap token: %s\n' "$DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN"
uv run dal-obscura-control-plane
```

In a second terminal, start the UI:

```bash
pnpm --dir apps/governance-ui dev
```

Open `http://localhost:5173` and use the local bootstrap token printed from the
first terminal to sign in. Register a catalog, discover and govern an asset,
and save its live policy in the UI. This local profile exercises authenticated
sessions; use the Keycloak demo above when you want to exercise the full SSO and
Flight read path.

Asset owners can choose to revoke all existing asset tickets during a policy
save or use the separate revoke action. Without revocation, tickets retain
their captured permissions until expiry. For a service with actual governed
reads, PostgreSQL, OIDC, TLS, and production network controls, follow the
[production reference](../deployment/production/README.md).

The UI development server proxies API and authentication requests to the local
control plane on port `8821`.

## Stop the local demo

Stop containers while preserving demo data:

```bash
./run down
```

Delete demo containers, generated files, and database state:

```bash
./run reset
```

## Next reads

- [Concepts](concepts.md): assets, policies, tickets, and request flow.
- [Policy authoring](policy-authoring.md): nested grants, filters, and masks.
- [Operators](operators.md): service setup and operational requirements.
- [Security](security.md): identity, ticket, and secret boundaries.
