# Auth Compose Container Layout Design

## Goal

Refactor the authentication Docker Compose examples so the reusable
`dal-obscura` server container is clearly separated from local example harness
code and provider-specific infrastructure. The examples should remain fully
self-contained, but the repository should also expose a sensible Docker image
that users can copy, build, and run outside the examples.

The redesign also keeps a client container running after the initial successful
authentication read, so users can experiment with additional reads, tables, and
data without restarting the Compose stack.

## Current Problem

`examples/auth/_shared/` currently mixes several responsibilities:

- the image used to run `dal-obscura`
- the image used to run example clients and setup scripts
- generic Iceberg fixture setup
- provider-specific Keycloak realm data
- provider-specific SPIRE configuration
- provider-specific mTLS certificate generation
- trusted-header gateway code

This makes it hard to tell which parts are reusable deployment material and
which parts are local demonstration scaffolding. It also makes every example
look dependent on all authentication backends, even when an example only needs
one mechanism.

## Design Summary

Use a three-layer layout:

1. **Reusable server image**
   - Add a root-level `Dockerfile`.
   - Build only the `dal-obscura` runtime and server dependencies.
   - Do not include Keycloak, SPIRE, fixture builders, client scripts, generated
     certificates, or example-only data.
   - Use concise Dockerfile comments where they clarify runtime decisions, such
     as dependency locking, non-root execution, and config mount expectations.

2. **Shared example harness**
   - Keep only provider-neutral helpers under `examples/auth/_shared/`.
   - The shared harness can create Iceberg catalogs, load fixture data, render
     generic app config, wait for Flight readiness, and run a parameterized
     Flight read.
   - It must not contain Keycloak realm files, SPIRE configs, or backend-specific
     auth config branches.

3. **Provider-owned example directories**
   - Move provider-specific assets next to the compose file that uses them.
   - Each auth example owns its auth provider config, any supporting service
     config, and its README.
   - Keycloak assets live under `keycloak-oidc/`.
   - SPIRE assets live under `mtls-spiffe/`.
   - Local certificate generation lives under `mtls/`.
   - Trusted gateway code lives under `trusted-headers/`.

## Proposed File Layout

```text
Dockerfile
examples/auth/
  compose.common.yaml
  README.md
  _shared/
    Dockerfile
    scripts/
      build_runtime.py
      flight_client.py
      wait_for_flight.py
  shared-jwt/
    compose.yaml
    README.md
    config/auth.yaml
    fixture/fixture.yaml
  api-key/
    compose.yaml
    README.md
    config/auth.yaml
    fixture/fixture.yaml
  composite-provider/
    compose.yaml
    README.md
    config/auth.yaml
    fixture/fixture.yaml
  keycloak-oidc/
    compose.yaml
    README.md
    config/auth.yaml
    fixture/fixture.yaml
    keycloak/realm.json
  mtls/
    compose.yaml
    README.md
    config/auth.yaml
    config/transport.yaml
    fixture/fixture.yaml
    scripts/generate_certs.py
  mtls-spiffe/
    compose.yaml
    README.md
    config/auth.yaml
    config/transport.yaml
    fixture/fixture.yaml
    spire/agent.conf
    spire/server.conf
    scripts/fetch_spiffe_svid.sh
    scripts/register_spiffe_entries.sh
    Dockerfile.tools
  trusted-headers/
    compose.yaml
    README.md
    config/auth.yaml
    fixture/fixture.yaml
    gateway/trusted_gateway.py
```

`examples/auth/_shared/Dockerfile` remains example-only. It installs helper
scripts and the Python dependencies needed to build fixtures and run a real
Flight client. It should not be presented as the deployment image.

## Reusable Server Image

The root `Dockerfile` should build a `dal-obscura` server image that works
outside the examples:

- copy `pyproject.toml`, `uv.lock`, `README.md`, `LICENSE`, and `src/`
- install runtime dependencies with the `server` extra and without dev
  dependencies
- expose the Flight port used by examples, while allowing users to override it
  through their app config
- run as a non-root user
- use `dal-obscura` as the entrypoint
- expect users to mount or bake an app config and run with
  `--app-config /path/to/app.yaml`

The Dockerfile comments should be limited to decisions a reader would otherwise
have to infer. Useful comments include:

- why the lockfile is copied before source files
- why logs are unbuffered
- why the image creates a non-root runtime user
- where runtime config is expected to be mounted

Comments should not narrate obvious Dockerfile syntax.

## Compose Reuse

Add `examples/auth/compose.common.yaml` and use Docker Compose `extends` from
each example when it reduces duplication.

Common service definitions should cover:

- `dal-obscura`: builds the root `Dockerfile`, mounts the generated runtime
  volume, and starts the server from `/workspace/runtime/app.yaml`
- `setup`: builds the example harness image and creates runtime config/data
- `client`: builds the example harness image, waits for the server, validates
  one authenticated read, then stays alive for interactive use

Per-example compose files should own:

- auth-specific environment variables and secrets
- provider support services, such as Keycloak, SPIRE, or the trusted gateway
- service dependencies that are unique to that provider
- TLS, SPIFFE, or gateway-specific volume mounts
- top-level volumes, since `extends` does not make top-level resource ownership
  clear enough on its own

The common file is a reuse mechanism, not a hiding place for provider-specific
logic. If a service only exists for one provider, keep it in that provider's
compose file.

## Runtime Config Generation

Replace the mode-switching fixture builder with a provider-neutral runtime
builder.

The shared `build_runtime.py` should:

- create a SQLite-backed Iceberg catalog and warehouse in the runtime volume
- load table definitions from a fixture YAML file supplied by the example
- write `catalogs.yaml`
- write `policies.yaml`
- assemble `app.yaml` from generic server settings plus the example's local
  `config/auth.yaml`
- optionally merge `config/transport.yaml` when the example needs TLS

The shared builder should not contain branches like `if mode == "keycloak"` or
hard-code provider module choices. Each example expresses its auth provider in
its local YAML files.

## User Data And Tables

Each example should include a `fixture/fixture.yaml` that defines the default
`default.users` table and policy grant for `example-user`.

The fixture format should support at least:

- catalog name
- one or more table targets
- table schema
- inline rows for simple examples
- optional CSV file input for larger local data
- policy rules for each table

Users can add their own table by editing the example's fixture file or mounting
an alternate fixture file into the setup container. The README should show both
the default table and a minimal custom-table example.

The runtime volume owns generated SQLite catalog state, Iceberg warehouse files,
and rendered server config. Users can reset generated state with:

```bash
docker compose down --volumes
```

## Long-Running Client Container

The `client` service should no longer be a short-lived one-shot container.

At startup the client should:

1. wait for the relevant endpoint
2. obtain or present the correct credential
3. run one read against `default.users`
4. validate the expected columns and non-empty result
5. print a success message
6. mark itself ready
7. stay running

The client should expose a simple command users can run with `docker compose
exec`, for example:

```bash
docker compose exec client dal-obscura-example-read --target default.users
```

The same command should support alternate targets and columns so users can read
tables they added through the fixture file.

For verification, the preferred run pattern becomes:

```bash
docker compose up --build -d --wait
docker compose logs client
docker compose exec client dal-obscura-example-read --target default.users
```

This keeps the interactive container available while still proving the auth
flow succeeds during startup.

## Provider-Specific Ownership

### Shared JWT

`shared-jwt/` owns a simple auth YAML using
`DefaultIdentityAdapter`. The client signs a local HS256 token with the shared
secret from the compose environment.

### API Key

`api-key/` owns a static API key auth YAML using `ApiKeyIdentityProvider`. The
client sends the configured key in the configured header.

### Composite Provider

`composite-provider/` owns an auth YAML with `auth.providers`, using multiple
real providers in order. The startup validation should prove both configured
credential families work, then keep the client running.

### Keycloak OIDC

`keycloak-oidc/` owns:

- the Keycloak service definition
- `keycloak/realm.json`
- the OIDC auth YAML
- any Keycloak-specific README caveats

The shared harness only knows how to request a credential when the example asks
for the Keycloak flow. It should not store the realm file or OIDC provider
config in `_shared`.

### Local mTLS

`mtls/` owns certificate generation and TLS transport config. Generated
certificates remain in the runtime volume. The shared harness can run the
example's script, but the certificate authority policy and SAN values are
owned by the mTLS example.

### SPIFFE mTLS

`mtls-spiffe/` owns:

- SPIRE server and agent configuration
- SPIRE registration scripts
- SVID fetch scripts
- any SPIRE tools image needed because official SPIRE images are minimal

The `dal-obscura` server still runs from the root server image. Any SPIRE helper
container is example infrastructure, not part of the reusable server image.

### Trusted Headers

`trusted-headers/` owns the gateway implementation and gateway service. The
backend server uses `TrustedHeaderIdentityProvider`, but direct access to the
backend should be documented as unsafe in real deployments unless blocked by
network policy.

## Documentation

Update documentation at three levels:

- root `README.md`: describe the reusable Docker image and point to auth
  examples
- `examples/auth/README.md`: explain the common layout, run pattern, client
  container, and fixture customization model
- each example README: document provider-specific services, credentials, custom
  data flow, interactive client commands, cleanup, and caveats

Each README should make it clear which files are production-relevant concepts
and which files are local example scaffolding.

## Verification

Per earlier direction, do not add automated tests for these examples.

Verification should include:

- `uv run ruff check` for edited Python helper scripts and any touched Python
  source
- `docker compose up --build -d --wait` for each example
- `docker compose exec client dal-obscura-example-read --target default.users`
  for each example
- `docker compose down --volumes` after each example
- a final check that no `dal-obscura-auth-*` containers are still running

If Compose `extends` behaves differently across Docker Compose versions, keep
the final structure compatible with the Docker Compose plugin available in this
environment and document that version-specific caveat.

## Non-Goals

- Do not add Kubernetes, Helm, Terraform, or cloud-specific deployment assets.
- Do not introduce external services outside Docker Compose.
- Do not mock Keycloak, SPIRE, mTLS, or the trusted gateway.
- Do not add automated tests for these examples unless explicitly requested.
- Do not turn the example harness image into a supported deployment image.

## Risks And Caveats

- Compose `extends` merges service definitions but does not make top-level
  resources obvious. Keep volumes and provider services explicit in each
  example.
- A long-running client changes the run workflow from one-shot
  `--exit-code-from client` to a detached or `--wait` workflow.
- User-provided fixtures need clear errors when schema, CSV paths, or policy
  rules are invalid.
- Demo secrets and generated certificates remain local-only examples and must
  not be described as production defaults.
- The reusable server image is useful only when paired with real app, catalog,
  policy, secret, and TLS configuration supplied by the operator.
