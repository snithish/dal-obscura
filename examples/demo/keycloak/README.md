# Local Keycloak Demo

This demo runs dal-obscura locally with Keycloak IAM, Postgres-backed
control-plane state, a seeded Iceberg table, and a Flight data plane. It is
built for sales walkthroughs and hands-on trials on a laptop.

Generated secrets and table files stay under this directory in `.runtime/`.
Control-plane state is stored in the Compose `postgres-data` volume so policy
changes survive container restarts.

## Requirements

- Docker with Compose v2
- Python 3 for the local `./run` helper

## Start

```bash
cd examples/demo/keycloak
./run up
```

What happens:

1. `scripts/prepare_demo.py` creates `.runtime/`, generates local secrets, writes
   per-service env files, and renders the Keycloak realm from
   `keycloak/realm.template.json`.
2. `./run up` builds a local `dal-obscura-demo:local` image from this checkout,
   unless `DAL_OBSCURA_IMAGE` is set to a prebuilt image.
3. Docker Compose starts Postgres on `127.0.0.1:5432` and Keycloak on
   `127.0.0.1:8080`.
4. The `migrate` service runs `dal-obscura-migrate upgrade` against Postgres.
5. The control plane starts on `127.0.0.1:8820` with Postgres config storage,
   Keycloak token validation, and public UI authentication configuration.
   The governance UI is built from `apps/governance-ui`, served on
   `127.0.0.1:8821` by default, and proxies `/v1` to the control plane on the
   same origin. Set `DAL_OBSCURA_DEMO_UI_PORT` to choose another loopback port.
6. The `setup` service creates the Iceberg table metadata and data files from `fixtures/demo_fixture.json`.
7. The setup service waits for the control plane and provisions it through the
   HTTP API. It configures one Iceberg SQL catalog,
   calls catalog discovery, confirms the demo table are discovered, then
   promotes them to governed assets.
8. The setup service assigns `group:asset-owners`, saves the demo policies
   directly to each live asset, and configures OIDC/JWKS auth for the data plane.
9. The Flight data plane starts on `127.0.0.1:8815`.

## Credentials

```bash
./run credentials
```

This prints generated passwords for the disposable local Keycloak users. Keep
them on the local machine and use them only with this demo realm.

The control-plane API remains on `http://127.0.0.1:8820`, with
Swagger docs at `http://127.0.0.1:8820/docs`. Useful demo users:

- `demo-admin`: platform admin access.
- `asset-owner`: can edit policies, filters, and masks for the demo asset.
- `us-analyst`, `eu-analyst`, `data-steward`: read-path personas for policy
  behavior.
- `blocked-user`: denied by policy.

Open the UI URL printed by `./run credentials` and select **Sign in with SSO**.
Keycloak handles the normal Authorization Code + PKCE flow. Sign in as
`demo-admin` with the generated password from `./run credentials` to manage the
workspace. `./run token --as <user>` prints a CLI access token for debugging
scripted reads; it does not create a browser session.

If port 8821 is already in use, start the demo on another loopback port. The
runner writes that origin into its OIDC callback and Keycloak realm settings:

```bash
DAL_OBSCURA_DEMO_UI_PORT=8822 ./run up
```

`./run credentials` prints the configured UI URL, and `./run ui-smoke` uses it.
Set `DAL_OBSCURA_DEMO_PROJECT_NAME` to target another Compose project and its
separate database volume. The default project name stays stable across runs.

## Demo Flow

Use `./run token --as <user>` and the read checks below to exercise the governed Flight path.

Verify the browser shell, session setup, authenticated inventory, CSRF-protected
logout, and session revocation after `./run up`:

```bash
./run ui-smoke
```

This smoke uses the explicitly enabled local bootstrap endpoint. It does not
verify OIDC or replace the normal **Sign in with SSO** browser flow.

For the full browser flow, open the UI and:

1. Select **Sign in with SSO** and confirm Keycloak shows the `dal-obscura-demo`
   realm.
2. Sign in as `demo-admin` using the generated password.
3. Confirm the account menu shows `demo-admin` and the Assets view lists seeded
   assets.
4. Select **Sign out** and confirm the UI returns to the signed-out workspace.

The full flow is also covered by an opt-in live browser test. With the demo
running and UI dependencies installed, run:

```bash
DAL_OBSCURA_E2E_LIVE_OIDC=1 \
DAL_OBSCURA_E2E_BASE_URL=http://127.0.0.1:8822 \
pnpm --dir ../../../apps/governance-ui test:e2e -- e2e/live-oidc-demo.spec.ts
```

Adjust the base URL when using a different UI port. The test reads the generated
`demo-admin` password from `.runtime/client.env` and reports only pass/fail.

See [LOCAL_VALIDATION.md](LOCAL_VALIDATION.md) for the latest recorded run and
any acceptance checks that remain pending.

## Read Checks

Run these from `examples/demo/keycloak`:

```bash
./run smoke
./run read --as us-analyst
./run read --as us-analyst --catalog retail_demo --target retail.customer_revenue
./run read --as eu-analyst
./run read --as data-steward
./run read --as blocked-user
```

Expected behavior:

- `./run smoke` runs the expected read checks for the demo personas against
  the Iceberg table.
- `us-analyst` reads two US rows with masked email values.
- `eu-analyst` reads two EU rows with masked email values.
- `data-steward` reads all rows with clear email values.
- `blocked-user` is denied.

## Stop Or Reset

Stop containers but keep generated demo state, including the Postgres volume:

```bash
./run down
```

Delete containers, generated files, and the Postgres volume:

```bash
./run reset
```

After a reset, `./run up` recreates `.runtime/`, starts Postgres, and runs the
explicit migration service before the control plane starts.

## Security Notes

This is a disposable local demo, not a production deployment. Ports are
loopback-bound, secrets are generated locally, and runtime files are gitignored.
Keycloak runs in development mode so the demo can start unattended. Use
production-grade identity, database, TLS, secret-management, and authorization
configuration before serving customer workloads.
