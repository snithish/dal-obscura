# Auth Compose Container Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the auth example suite so `dal-obscura` has a reusable root Docker image, provider-specific assets live with their example, shared helpers are provider-neutral, and each example leaves an interactive client container running after a verified startup read.

**Architecture:** Add a root server `Dockerfile`, introduce `examples/auth/compose.common.yaml` for reusable Compose service definitions, replace the mode-switching fixture builder with a provider-neutral runtime builder driven by per-example YAML files, and move Keycloak, SPIFFE, mTLS, and trusted-gateway assets into their own example directories. Keep examples self-contained, but stop mixing reusable server runtime concerns with example-only infrastructure.

**Tech Stack:** Docker, Docker Compose `extends`, Python 3.12, `uv`, PyArrow Flight, PyIceberg SQL catalog, SQLite, Keycloak, SPIRE/SPIFFE, PyJWT, `cryptography`, YAML.

---

## Scope Check

This work stays within one subsystem: the runnable auth example suite and its
container layout. It does not change the server authentication model itself.
The existing core auth tests remain in place. Per user direction, do not add
new automated tests for the examples; verify them by linting helper code and
running each Compose flow.

## File Structure

- Create: `Dockerfile`
- Create: `examples/auth/compose.common.yaml`
- Create: `examples/auth/_shared/scripts/build_runtime.py`
- Create: `examples/auth/_shared/scripts/client_session.py`
- Modify: `examples/auth/_shared/Dockerfile`
- Modify: `examples/auth/_shared/scripts/flight_client.py`
- Modify: `examples/auth/_shared/scripts/wait_for_flight.py`
- Delete: `examples/auth/_shared/scripts/build_fixture.py`
- Delete: `examples/auth/_shared/keycloak/realm.json`
- Delete: `examples/auth/_shared/scripts/generate_certs.py`
- Delete: `examples/auth/_shared/scripts/fetch_spiffe_svid.sh`
- Delete: `examples/auth/_shared/scripts/register_spiffe_entries.sh`
- Delete: `examples/auth/_shared/scripts/trusted_gateway.py`
- Delete: `examples/auth/_shared/spire/agent.conf`
- Delete: `examples/auth/_shared/spire/server.conf`
- Create: `examples/auth/shared-jwt/config/auth.yaml`
- Create: `examples/auth/shared-jwt/fixture/fixture.yaml`
- Modify: `examples/auth/shared-jwt/compose.yaml`
- Modify: `examples/auth/shared-jwt/README.md`
- Create: `examples/auth/api-key/config/auth.yaml`
- Create: `examples/auth/api-key/fixture/fixture.yaml`
- Modify: `examples/auth/api-key/compose.yaml`
- Modify: `examples/auth/api-key/README.md`
- Create: `examples/auth/composite-provider/config/auth.yaml`
- Create: `examples/auth/composite-provider/fixture/fixture.yaml`
- Modify: `examples/auth/composite-provider/compose.yaml`
- Modify: `examples/auth/composite-provider/README.md`
- Create: `examples/auth/keycloak-oidc/config/auth.yaml`
- Create: `examples/auth/keycloak-oidc/fixture/fixture.yaml`
- Create: `examples/auth/keycloak-oidc/keycloak/realm.json`
- Modify: `examples/auth/keycloak-oidc/compose.yaml`
- Modify: `examples/auth/keycloak-oidc/README.md`
- Create: `examples/auth/mtls/config/auth.yaml`
- Create: `examples/auth/mtls/config/transport.yaml`
- Create: `examples/auth/mtls/fixture/fixture.yaml`
- Create: `examples/auth/mtls/scripts/generate_certs.py`
- Modify: `examples/auth/mtls/compose.yaml`
- Modify: `examples/auth/mtls/README.md`
- Create: `examples/auth/mtls-spiffe/config/auth.yaml`
- Create: `examples/auth/mtls-spiffe/config/transport.yaml`
- Create: `examples/auth/mtls-spiffe/fixture/fixture.yaml`
- Create: `examples/auth/mtls-spiffe/spire/agent.conf`
- Create: `examples/auth/mtls-spiffe/spire/server.conf`
- Create: `examples/auth/mtls-spiffe/scripts/fetch_spiffe_svid.sh`
- Create: `examples/auth/mtls-spiffe/scripts/register_spiffe_entries.sh`
- Create: `examples/auth/mtls-spiffe/Dockerfile.tools`
- Modify: `examples/auth/mtls-spiffe/compose.yaml`
- Modify: `examples/auth/mtls-spiffe/README.md`
- Create: `examples/auth/trusted-headers/config/auth.yaml`
- Create: `examples/auth/trusted-headers/fixture/fixture.yaml`
- Create: `examples/auth/trusted-headers/gateway/trusted_gateway.py`
- Modify: `examples/auth/trusted-headers/compose.yaml`
- Modify: `examples/auth/trusted-headers/README.md`
- Modify: `examples/auth/README.md`
- Modify: `README.md`

## Task 1: Add The Reusable Server Image

**Files:**
- Create: `Dockerfile`
- Modify: `.dockerignore`
- Modify: `README.md`

- [ ] **Step 1: Add the root Dockerfile**

Use a small runtime image, install only locked runtime dependencies, create a
non-root user, and leave clear comments only where the reasoning is not
obvious.

```dockerfile
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /workspace

# Keep Python output unbuffered so container logs are visible immediately.
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
ENV UV_NO_DEV=1

# Create a non-root runtime user for the Flight server process.
RUN groupadd --gid 10001 dalobscura \
    && useradd --uid 10001 --gid 10001 --home-dir /home/dalobscura --create-home dalobscura

# Copy lockfile inputs first so dependency resolution stays cached until inputs change.
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --extra server --frozen

COPY src ./src

USER 10001:10001
EXPOSE 8815

# Mount or bake an app config, then run: dal-obscura --app-config /path/to/app.yaml
ENTRYPOINT ["/workspace/.venv/bin/dal-obscura"]
```

- [ ] **Step 2: Keep the Docker build context clean**

Ensure `.dockerignore` continues to exclude local caches and virtualenvs. Add
any example-local generated directories only if the refactor introduces them.

- [ ] **Step 3: Document the reusable image in the root README**

Add a short section explaining that the root `Dockerfile` is the reusable server
artifact and the auth examples build on top of it.

- [ ] **Step 4: Verify the image builds**

Run: `docker build -f Dockerfile .`
Expected: exit `0`

- [ ] **Step 5: Commit**

Run:

```bash
git add Dockerfile .dockerignore README.md
git commit -m "build(docker): add reusable dal-obscura image"
```

## Task 2: Create A Provider-Neutral Shared Harness

**Files:**
- Create: `examples/auth/compose.common.yaml`
- Modify: `examples/auth/_shared/Dockerfile`
- Create: `examples/auth/_shared/scripts/build_runtime.py`
- Create: `examples/auth/_shared/scripts/client_session.py`
- Modify: `examples/auth/_shared/scripts/flight_client.py`
- Modify: `examples/auth/_shared/scripts/wait_for_flight.py`
- Delete: `examples/auth/_shared/scripts/build_fixture.py`

- [ ] **Step 1: Slim the shared image to example-only helpers**

Keep the shared image focused on fixture generation and client tooling. Do not
copy provider-specific Keycloak or SPIFFE assets into it.

```dockerfile
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /workspace

ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
ENV UV_NO_DEV=1

COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --extra server --frozen

COPY src ./src
COPY examples/auth/_shared ./examples/auth/_shared

COPY examples/auth/_shared/scripts/client_session.py /usr/local/bin/dal-obscura-example-client
```

- [ ] **Step 2: Replace the mode-switching runtime builder**

Create `build_runtime.py` that reads:

- `EXAMPLE_ROOT`
- `FIXTURE_FILE`
- `AUTH_CONFIG_FILE`
- optional `TRANSPORT_CONFIG_FILE`

It should create the SQLite Iceberg catalog, build tables from fixture YAML,
write `catalogs.yaml`, write `policies.yaml`, and render `app.yaml` by merging a
fixed server base with auth and optional transport YAML.

- [ ] **Step 3: Support editable fixture data**

Implement fixture parsing for:

- catalog name
- one or more tables
- primitive schema fields
- inline rows
- optional `csv` source relative to the example directory
- policy rules

Keep the first version intentionally narrow: primitive scalar types only, with
clear errors for unsupported field types.

- [ ] **Step 4: Add a long-running client session wrapper**

Create `client_session.py` that:

1. waits for Flight readiness
2. performs one or more startup reads
3. writes a readiness marker
4. sleeps indefinitely

It should use `flight_client.py` for real reads instead of duplicating the
Flight logic.

- [ ] **Step 5: Make the Flight client reusable from `docker compose exec`**

Refactor `flight_client.py` to accept CLI arguments such as:

```bash
python examples/auth/_shared/scripts/flight_client.py \
  --target default.users \
  --columns id,email,region \
  --credential api-key
```

Keep the existing auth-flow support, but drive it from environment variables and
CLI flags rather than hard-coded startup behavior.

- [ ] **Step 6: Add common Compose service definitions**

Create `examples/auth/compose.common.yaml` with reusable `setup`,
`dal-obscura`, and `client` services. The common services should use the shared
and root Dockerfiles, runtime volume mounts, standard commands, and healthcheck
markers. Leave auth-specific services and environment in each example compose
file.

- [ ] **Step 7: Verify helper code quality**

Run:

```bash
uv run ruff check examples/auth/_shared/scripts
```

Expected: `All checks passed!`

- [ ] **Step 8: Commit**

Run:

```bash
git add examples/auth/compose.common.yaml examples/auth/_shared
git commit -m "feat(examples): add shared compose harness"
```

## Task 3: Move Local Auth Examples To Per-Example Config

**Files:**
- Create: `examples/auth/shared-jwt/config/auth.yaml`
- Create: `examples/auth/shared-jwt/fixture/fixture.yaml`
- Modify: `examples/auth/shared-jwt/compose.yaml`
- Modify: `examples/auth/shared-jwt/README.md`
- Create: `examples/auth/api-key/config/auth.yaml`
- Create: `examples/auth/api-key/fixture/fixture.yaml`
- Modify: `examples/auth/api-key/compose.yaml`
- Modify: `examples/auth/api-key/README.md`
- Create: `examples/auth/composite-provider/config/auth.yaml`
- Create: `examples/auth/composite-provider/fixture/fixture.yaml`
- Modify: `examples/auth/composite-provider/compose.yaml`
- Modify: `examples/auth/composite-provider/README.md`

- [ ] **Step 1: Add per-example auth YAML**

Move the auth provider choice out of the shared builder and into each example's
`config/auth.yaml`.

Example:

```yaml
auth:
  module: dal_obscura.data_plane.infrastructure.adapters.identity_api_key.ApiKeyIdentityProvider
  args:
    keys:
      - id: example-user
        secret:
          key: DAL_OBSCURA_API_KEY
        groups: ["compose-example"]
```

- [ ] **Step 2: Add per-example fixture YAML**

Define the default `default.users` table and the example-user policy grant in
each example directory so users can edit the fixture locally without touching
shared code.

- [ ] **Step 3: Rebuild each compose file on top of `extends`**

Each local auth compose file should extend:

- `setup` from `../compose.common.yaml`
- `dal-obscura` from `../compose.common.yaml`
- `client` from `../compose.common.yaml`

Override only the example-specific environment, volumes, and startup read
configuration.

- [ ] **Step 4: Keep the client container alive**

Update each example so the client starts, verifies the flow, becomes healthy,
and then stays running for later `docker compose exec` reads.

- [ ] **Step 5: Document custom tables and interactive reads**

Update each README with:

- run commands using `docker compose up --build -d --wait`
- how to inspect client logs
- how to run `docker compose exec client ...`
- how to edit `fixture/fixture.yaml`
- how to reset with `docker compose down --volumes`

- [ ] **Step 6: Verify the three local examples**

Run:

```bash
docker compose -f examples/auth/shared-jwt/compose.yaml up --build -d --wait
docker compose -f examples/auth/shared-jwt/compose.yaml exec client /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py --target default.users
docker compose -f examples/auth/shared-jwt/compose.yaml down --volumes

docker compose -f examples/auth/api-key/compose.yaml up --build -d --wait
docker compose -f examples/auth/api-key/compose.yaml exec client /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py --target default.users
docker compose -f examples/auth/api-key/compose.yaml down --volumes

docker compose -f examples/auth/composite-provider/compose.yaml up --build -d --wait
docker compose -f examples/auth/composite-provider/compose.yaml exec client /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py --target default.users --credential api-key
docker compose -f examples/auth/composite-provider/compose.yaml exec client /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py --target default.users --credential jwt
docker compose -f examples/auth/composite-provider/compose.yaml down --volumes
```

Expected: each example reports a successful authenticated read and tears down
cleanly.

- [ ] **Step 7: Commit**

Run:

```bash
git add examples/auth/shared-jwt examples/auth/api-key examples/auth/composite-provider
git commit -m "refactor(examples): split local auth example config"
```

## Task 4: Move Keycloak, mTLS, And Trusted Gateway Assets To Their Examples

**Files:**
- Create: `examples/auth/keycloak-oidc/config/auth.yaml`
- Create: `examples/auth/keycloak-oidc/fixture/fixture.yaml`
- Create: `examples/auth/keycloak-oidc/keycloak/realm.json`
- Modify: `examples/auth/keycloak-oidc/compose.yaml`
- Modify: `examples/auth/keycloak-oidc/README.md`
- Create: `examples/auth/mtls/config/auth.yaml`
- Create: `examples/auth/mtls/config/transport.yaml`
- Create: `examples/auth/mtls/fixture/fixture.yaml`
- Create: `examples/auth/mtls/scripts/generate_certs.py`
- Modify: `examples/auth/mtls/compose.yaml`
- Modify: `examples/auth/mtls/README.md`
- Create: `examples/auth/trusted-headers/config/auth.yaml`
- Create: `examples/auth/trusted-headers/fixture/fixture.yaml`
- Create: `examples/auth/trusted-headers/gateway/trusted_gateway.py`
- Modify: `examples/auth/trusted-headers/compose.yaml`
- Modify: `examples/auth/trusted-headers/README.md`

- [ ] **Step 1: Move the Keycloak realm under `keycloak-oidc/`**

Update the compose file to mount `./keycloak/realm.json` and keep all issuer,
audience, and JWKS configuration in `config/auth.yaml`.

- [ ] **Step 2: Move local mTLS assets under `mtls/`**

Place certificate generation under `mtls/scripts/generate_certs.py`, move peer
mapping into `mtls/config/auth.yaml`, and move the TLS listener config into
`mtls/config/transport.yaml`.

- [ ] **Step 3: Move the trusted Flight gateway under `trusted-headers/`**

Place the gateway implementation in `trusted-headers/gateway/` and update the
compose file to mount or invoke it from there instead of `_shared`.

- [ ] **Step 4: Align compose files with the common base**

Extend the shared `setup`, `dal-obscura`, and `client` definitions. Keep only
provider-specific env, volumes, service dependencies, and support services in
the example compose files.

- [ ] **Step 5: Verify the three examples**

Run:

```bash
docker compose -f examples/auth/keycloak-oidc/compose.yaml up --build -d --wait
docker compose -f examples/auth/keycloak-oidc/compose.yaml exec client /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py --target default.users
docker compose -f examples/auth/keycloak-oidc/compose.yaml down --volumes

docker compose -f examples/auth/mtls/compose.yaml up --build -d --wait
docker compose -f examples/auth/mtls/compose.yaml exec client /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py --target default.users
docker compose -f examples/auth/mtls/compose.yaml down --volumes

docker compose -f examples/auth/trusted-headers/compose.yaml up --build -d --wait
docker compose -f examples/auth/trusted-headers/compose.yaml exec client /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py --target default.users
docker compose -f examples/auth/trusted-headers/compose.yaml down --volumes
```

Expected: Keycloak issues a real token, mTLS authenticates with the example
certificates, and the gateway-backed trusted-header flow succeeds through the
gateway container.

- [ ] **Step 6: Commit**

Run:

```bash
git add examples/auth/keycloak-oidc examples/auth/mtls examples/auth/trusted-headers
git commit -m "refactor(examples): isolate provider-specific auth assets"
```

## Task 5: Refactor The SPIFFE Example Around Provider-Owned Tools

**Files:**
- Create: `examples/auth/mtls-spiffe/config/auth.yaml`
- Create: `examples/auth/mtls-spiffe/config/transport.yaml`
- Create: `examples/auth/mtls-spiffe/fixture/fixture.yaml`
- Create: `examples/auth/mtls-spiffe/spire/agent.conf`
- Create: `examples/auth/mtls-spiffe/spire/server.conf`
- Create: `examples/auth/mtls-spiffe/scripts/fetch_spiffe_svid.sh`
- Create: `examples/auth/mtls-spiffe/scripts/register_spiffe_entries.sh`
- Create: `examples/auth/mtls-spiffe/Dockerfile.tools`
- Modify: `examples/auth/mtls-spiffe/compose.yaml`
- Modify: `examples/auth/mtls-spiffe/README.md`

- [ ] **Step 1: Build a SPIFFE-specific tools image**

Create `Dockerfile.tools` for shell-capable SPIRE helper services. Keep SPIRE
assets out of the root server image and the generic shared harness image.

- [ ] **Step 2: Move SPIRE config and scripts under `mtls-spiffe/`**

Place the server and agent config plus the registration and SVID fetch scripts
next to the example compose file.

- [ ] **Step 3: Fetch SVIDs through helper services**

Use helper services to fetch server and client SVID material into named volumes
before the root `dal-obscura` container and shared client container start. The
server container should read certs from volumes, not from embedded SPIRE tools.

- [ ] **Step 4: Keep the interactive client pattern**

Once the client SVID volume is ready, the shared client container should perform
the verified startup read, become healthy, and stay alive for manual reads.

- [ ] **Step 5: Verify the SPIFFE example**

Run:

```bash
docker compose -f examples/auth/mtls-spiffe/compose.yaml up --build -d --wait
docker compose -f examples/auth/mtls-spiffe/compose.yaml exec client /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py --target default.users
docker compose -f examples/auth/mtls-spiffe/compose.yaml down --volumes
```

Expected: the server and client both receive real X.509-SVIDs from SPIRE and
the authenticated read succeeds through mTLS with the SPIFFE identity mapping.

- [ ] **Step 6: Commit**

Run:

```bash
git add examples/auth/mtls-spiffe
git commit -m "refactor(examples): isolate spiffe example tooling"
```

## Task 6: Update Documentation And Run The Full Verification Sweep

**Files:**
- Modify: `examples/auth/README.md`
- Modify: `README.md`
- Modify: all example `README.md` files touched above

- [ ] **Step 1: Document the new run pattern**

Update docs to use:

```bash
docker compose up --build -d --wait
docker compose logs client
docker compose exec client /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py --target default.users
docker compose down --volumes
```

- [ ] **Step 2: Document fixture customization**

Each README should show where `fixture/fixture.yaml` lives, how to add a second
table, and how to swap inline rows for CSV-backed rows when useful.

- [ ] **Step 3: Run helper lint**

Run:

```bash
uv run ruff check examples/auth/_shared/scripts examples/auth/mtls/scripts examples/auth/trusted-headers/gateway
```

Expected: `All checks passed!`

- [ ] **Step 4: Run the full Compose sweep**

Run every example:

```bash
docker compose -f examples/auth/shared-jwt/compose.yaml up --build -d --wait
docker compose -f examples/auth/shared-jwt/compose.yaml down --volumes
docker compose -f examples/auth/api-key/compose.yaml up --build -d --wait
docker compose -f examples/auth/api-key/compose.yaml down --volumes
docker compose -f examples/auth/composite-provider/compose.yaml up --build -d --wait
docker compose -f examples/auth/composite-provider/compose.yaml down --volumes
docker compose -f examples/auth/keycloak-oidc/compose.yaml up --build -d --wait
docker compose -f examples/auth/keycloak-oidc/compose.yaml down --volumes
docker compose -f examples/auth/mtls/compose.yaml up --build -d --wait
docker compose -f examples/auth/mtls/compose.yaml down --volumes
docker compose -f examples/auth/mtls-spiffe/compose.yaml up --build -d --wait
docker compose -f examples/auth/mtls-spiffe/compose.yaml down --volumes
docker compose -f examples/auth/trusted-headers/compose.yaml up --build -d --wait
docker compose -f examples/auth/trusted-headers/compose.yaml down --volumes
```

Expected: all stacks reach ready state and can be cleaned up without left-over
containers.

- [ ] **Step 5: Confirm cleanup**

Run:

```bash
docker ps --format '{{.Names}}' | rg 'dal-obscura-auth-' || true
git status --short
```

Expected: no running example containers remain, and only intended repo changes
are present.

- [ ] **Step 6: Commit**

Run:

```bash
git add README.md examples/auth
git commit -m "docs(examples): document interactive auth compose layout"
```

## Self-Review

- Spec coverage: the plan covers the reusable root image, provider-neutral
  shared harness, per-example provider assets, long-running clients, editable
  fixtures, Compose `extends`, docs, and per-example verification.
- Placeholder scan: no `TBD`, `TODO`, or deferred "implement later" text
  remains.
- Type consistency: the plan consistently uses `build_runtime.py`,
  `client_session.py`, per-example `config/auth.yaml`, optional
  `config/transport.yaml`, and `fixture/fixture.yaml`.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-28-auth-compose-container-layout.md`.

Execution choice for this session: proceed with **Inline Execution** using
`superpowers:executing-plans`, because the user already asked to implement the
approved design and subagent delegation is not appropriate here.
