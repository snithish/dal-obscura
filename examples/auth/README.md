# Authentication Examples

These examples run `dal-obscura` with real authentication mechanisms inside
Docker Compose. They are teaching fixtures first: each directory keeps the
auth-specific wiring visible, while shared setup/client mechanics live in
`_shared/` and `compose.common.yaml`.

The `dal-obscura` service uses the published image
`ghcr.io/snithish/dal-obscura:latest` by default. Set `DAL_OBSCURA_IMAGE` when
you want to run the examples against a release tag, SHA tag, or local image.

## Choose an Example

| Example | Start here when you want to see |
| --- | --- |
| `api-key` | the smallest service credential flow |
| `shared-jwt` | HS256 bearer JWT validation with a shared secret |
| `keycloak-oidc` | OIDC/JWKS validation against a real Keycloak issuer |
| `trusted-headers` | a trusted Flight gateway injecting identity headers |
| `mtls` | local client-certificate authentication |
| `mtls-spiffe` | SPIRE-issued SPIFFE X.509-SVID authentication |
| `composite-provider` | one server accepting multiple credential families |

For a first read-through, start with `api-key` or `shared-jwt`. The mTLS and
SPIFFE examples include more infrastructure because they demonstrate certificate
issuance, trust roots, and workload identities.

## Run From an Example Directory

Every example can still be run from its own directory:

```bash
cd examples/auth/shared-jwt
docker compose up -d --wait
docker compose logs client
docker compose exec client dal-obscura-example-read --target default.users
docker compose down --volumes
```

The startup read happens automatically inside the `client` container. That
container stays running so you can issue more reads with `docker compose exec`
instead of restarting the stack.

## Optional Root Helper

From the repository root, `examples/auth/run` wraps the same Docker Compose
commands without changing into each directory:

```bash
examples/auth/run list
examples/auth/run shared-jwt up
examples/auth/run shared-jwt logs
examples/auth/run shared-jwt read --target default.users
examples/auth/run shared-jwt down
```

The helper prints the Docker Compose command it runs, so users can copy the
plain command once they understand the pattern.

## Common Shape

Most examples have the same three service roles:

1. `setup` creates a SQLite-backed Iceberg catalog, provisions the control-plane
   API into `control-plane.db`, publishes the config snapshot, and writes
   data-plane environment settings into a Docker volume.
2. `dal-obscura` starts the real Flight service from the published GHCR image
   and reads that published state.
3. `client` obtains or presents the selected credential, performs a startup
   read, marks itself healthy, and then stays available for interactive reads.

Some examples add provider infrastructure:

- `keycloak-oidc` adds `keycloak`.
- `trusted-headers` adds `gateway`.
- `mtls` generates local demo certificates during setup.
- `mtls-spiffe` includes `compose.spire.yaml`, which contains the SPIRE server,
  agent, bootstrap, and SVID helper services.

The sample policy grants `example-user` access to `default.users`, with a row
filter that returns two rows. A successful run prints:

```text
<example-name>: authenticated as example-user and read 2 rows
```

## Customizing Data

The shared setup script contains the small sample dataset, policy, and auth
provider provisioning payloads used by these examples. To change the sample
table, edit `examples/auth/_shared/scripts/build_runtime.py` and recreate the
stack:

```bash
docker compose down --volumes
docker compose up -d --wait
```

The setup container rebuilds the local catalog and republishes control-plane
state on every fresh start.

If you change helper image dependencies or Dockerfile instructions, rebuild the
helper image explicitly with `docker compose build` or
`examples/auth/run <example> build`. Changes to the mounted shared scripts and
per-example files are picked up on the next fresh start.

## Caveats

These examples are local developer environments. They use fixed demo secrets,
development-mode Keycloak, short-lived generated data, and disposable Docker
volumes. Treat them as runnable integration references, not production
deployment manifests.
