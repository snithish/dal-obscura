# Authentication Examples

These examples run `dal-obscura` with real authentication mechanisms inside
Docker Compose. Each directory has its own `compose.yaml`, builds the reusable
root server image, and can run without any external identity service,
certificate authority, gateway, or data service.

## Examples

- `shared-jwt`: shared-secret HS256 bearer JWT.
- `keycloak-oidc`: real Keycloak OIDC issuer and JWKS validation.
- `api-key`: static service API key.
- `mtls`: local certificate-authority mTLS.
- `mtls-spiffe`: real SPIRE server and agent issuing SPIFFE X.509-SVIDs.
- `trusted-headers`: real Flight gateway injecting trusted identity headers.
- `composite-provider`: one server accepting API key and shared JWT credentials.

## Run Pattern

Run from an example directory:

```bash
docker compose up --build -d --wait
docker compose logs client
docker compose exec client dal-obscura-example-read --target default.users
```

Clean up from that directory:

```bash
docker compose down --volumes
```

The startup read happens automatically inside the `client` container. That
container stays running so you can issue more reads with `docker compose exec`
instead of restarting the whole stack.

## What Each Example Does

Every example starts from the same shape:

1. `setup` creates a SQLite-backed Iceberg catalog, a small table, a policy file,
   and an auth-specific `app.yaml` in a named Docker volume.
2. `dal-obscura` starts the real Flight service from that generated config.
3. Optional infrastructure services start when the mechanism needs them, such as
   Keycloak, SPIRE, or a trusted Flight gateway.
4. `client` obtains or presents the selected credential, performs a startup read,
   marks itself healthy, and then stays available for interactive reads.

The sample policy grants `example-user` access to `default.users`, with a row
filter that returns two rows. A successful run prints:

```text
<example-name>: authenticated as example-user and read 2 rows
```

## Customizing Data

Each example directory owns its runtime inputs:

- `fixture/fixture.yaml`: catalog, tables, rows, and policy rules
- `config/auth.yaml`: auth provider wiring
- `config/transport.yaml`: transport TLS settings when that example needs them

To add your own table or rows, edit `fixture/fixture.yaml` and recreate the
stack:

```bash
docker compose down --volumes
docker compose up --build -d --wait
```

The fixture format supports multiple tables, primitive scalar fields, inline
rows, and optional CSV-backed rows. The shared setup container rebuilds the
runtime catalog from those files on every fresh start.

## Caveats

These examples are local developer fixtures. They use fixed demo secrets,
development-mode Keycloak, short-lived generated data, and disposable Docker
volumes. Treat them as runnable integration references, not production
deployment manifests.
