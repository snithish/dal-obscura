# Quickstart

Start with the authenticated local demo, then use the manual service steps for a
deployment you configure yourself.

## Run the local demo

Prerequisites: uv and Docker/Podman with Compose v2. Start Docker Desktop or
`podman machine start` first. Node 24/pnpm are needed for browser verification.

```bash
cd examples/demo/keycloak
./demo init
./demo credentials
./demo check
```

Open `http://localhost:28821`, choose **Sign in with SSO**, and sign in as
`demo-admin` with the generated password. The `asset-owner` can edit the seeded
policy. Regional analysts see filtered rows and masked email; stewards and owners
see complete rows. The blocked user and administrative-only user cannot read data.

`./demo check` verifies actual reads and Chromium SSO, reload, and logout. Use
`./demo check --reads-only` without browser tools. Read checks expect the seeded
policy; intentional policy changes can alter those expected results.

The new example has its own project, PostgreSQL databases, and Iceberg warehouse.
It preserves secrets and edits across `./demo down` and `./demo up`. Init can resume
an interrupted setup and rebuild images after code changes. Normal up does not
seed or provision. See the [example README](../examples/demo/keycloak/README.md)
for port overrides and troubleshooting.

The [secure local demo](../deployment/local-secure/README.md) uses the same fixture
and lifecycle commands with HTTPS Keycloak, browser HTTPS, Flight mTLS, and
separate migration/control/data database roles. Start it with `./demo init` from
`deployment/local-secure`; no external identity provider is required.

## Start services manually

The supported config-store migration history starts from a clean baseline for
live catalogs, assets, policy, tickets, sessions, and audit records, followed by
the nested schema type storage upgrade. This is a
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
./demo down
```

Delete demo containers, generated files, and database state:

```bash
./demo reset
```

## Next reads

- [Concepts](concepts.md): assets, policies, tickets, and request flow.
- [Policy authoring](policy-authoring.md): nested grants, filters, and masks.
- [Operators](operators.md): service setup and operational requirements.
- [Security](security.md): identity, ticket, and secret boundaries.
