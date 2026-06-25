# Local Keycloak Demo

This demo runs dal-obscura locally with Keycloak IAM, Postgres-backed
control-plane state, seeded Iceberg and Delta tables, the control-plane UI, and
a Flight data plane. It is built for sales walkthroughs and hands-on trials on a
laptop.

Generated secrets and table files stay under this directory in `.runtime/`.
Control-plane state is stored in the Compose `postgres-data` volume so policy
changes and publications survive container restarts.

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
4. The control plane starts on `127.0.0.1:8820` with Postgres config storage,
   Keycloak token validation, and public browser-login configuration for the UI.
5. The `setup` service creates the Iceberg table metadata, Delta table log, and
   data files from `fixtures/demo_fixture.json`.
6. The setup service waits for the control plane and provisions it through the
   HTTP API. It configures one Iceberg SQL catalog and one static Delta catalog,
   calls catalog discovery, confirms both demo tables are discovered, then
   promotes them to governed assets.
7. The setup service assigns `group:asset-owners`, installs the demo policies,
   configures OIDC/JWKS auth for the data plane, and publishes the first active
   policy version.
8. The Flight data plane starts on `127.0.0.1:8815`.

## Credentials

```bash
./run credentials
```

Open `http://127.0.0.1:8821`, choose **Sign in**, and log in through
Keycloak. The control-plane API remains on `http://127.0.0.1:8820`, with
Swagger docs at `http://127.0.0.1:8820/docs`. Useful demo users:

- `demo-admin`: platform admin access.
- `asset-owner`: can edit owners, policies, filters, masks, and publish policy
  versions for the demo asset.
- `us-analyst`, `eu-analyst`, `data-steward`: read-path personas for policy
  behavior.
- `blocked-user`: denied by policy.

`./run token --as <user>` still prints a CLI access token for debugging scripted
reads, but the UI uses Keycloak authorization-code + PKCE login.

The UI also shows two demo shortcut buttons, **Platform owner** and
**Data asset owner**. In this local demo they use a demo-only server-side token
exchange, so clicking one drops you into the UI as that persona without typing a
password. Other users still use Keycloak with the generated passwords.

## Demo Flow

1. Open `http://127.0.0.1:8821`.
2. Sign in as `demo-admin` using the password from `./run credentials`.
3. Open Catalogs and run discovery for `retail_demo` and `retail_delta`; the
   tables should appear as governed.
4. Open Assets and inspect owners for `retail.customer_revenue`.
5. Sign out, sign in as `asset-owner`, then open Policies.
6. Edit a row filter or mask and publish a policy version.
7. Sign out, sign in as `us-analyst`, and confirm policy controls are not
   editable.

## Read Checks

Run these from `examples/demo/keycloak`:

```bash
./run smoke
./run read --as us-analyst
./run read --as us-analyst --catalog retail_demo --target retail.customer_revenue
./run read --as us-analyst --catalog retail_delta --target retail.customer_revenue_delta
./run read --as eu-analyst
./run read --as data-steward
./run read --as blocked-user
```

Expected behavior:

- `./run smoke` runs the expected read checks for the demo personas against
  both Iceberg and Delta.
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

## Security Notes

This is a secure local demo, not a production deployment manifest. Ports are
bound to `127.0.0.1`, secrets are generated locally, and runtime files are
gitignored. The browser UI uses a public Keycloak client with PKCE; the
confidential client secret is only used by the scripted CLI reads. Keycloak runs
in development mode so the demo can start unattended.
