# Authentication Examples

These examples run `dal-obscura` with real authentication mechanisms inside Docker
Compose. Each directory has its own `compose.yaml` and can be run independently.
No external identity service, certificate authority, gateway, or data service is
required.

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
docker compose up --build --abort-on-container-exit --exit-code-from client
```

Clean up from that directory:

```bash
docker compose down --volumes
```

## What Each Example Does

Every example starts from the same shape:

1. `setup` creates a SQLite-backed Iceberg catalog, a small table, a policy file,
   and an auth-specific `app.yaml` in a named Docker volume.
2. `dal-obscura` starts the real Flight service from that generated config.
3. Optional infrastructure services start when the mechanism needs them, such as
   Keycloak, SPIRE, or a trusted Flight gateway.
4. `client` obtains or presents the selected credential, calls `get_flight_info`,
   calls `do_get`, validates the returned schema and rows, then exits `0`.

The sample policy grants `example-user` access to `default.users`, with a row
filter that returns two rows. A successful run prints:

```text
<example-name>: authenticated as example-user and read 2 rows
```

## Caveats

These examples are local developer fixtures. They use fixed demo secrets,
development-mode Keycloak, short-lived generated data, and disposable Docker
volumes. Treat them as runnable integration references, not production
deployment manifests.
