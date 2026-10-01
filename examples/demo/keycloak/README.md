# Local Keycloak example

A disposable, loopback-only environment for real SSO and governed Iceberg reads.
It uses the product's service and UI images, built from this checkout.

## Requirements

- Docker Desktop with Compose v2, or a running Podman machine using Compose v2.
- `uv`; the launcher uses an isolated Python 3.12 interpreter with no host package installation.
- At least 4 GiB available container memory and enough disk for image builds.
- Node 24 and pnpm for the browser check. Startup itself does not need host Node.
- Free TCP ports 20080 (Keycloak), 28821 (UI), and 28115 (Flight).

With Podman, start the machine with `podman machine start`. The launcher reports
an unavailable engine, missing Compose, or occupied ports before building.

## First start

```bash
cd examples/demo/keycloak
./demo init
./demo credentials
./demo check
```

Open **http://localhost:28821**, choose **Sign in with SSO**, and sign in as
`demo-admin` with the generated password. Always use `localhost` for browser
access so the configured origin, callback, and cookie behavior agree.

`./demo init` generates one private `.env`, builds both images from the current
checkout, starts the infrastructure, migrates the application database, seeds
Iceberg, provisions configuration through the API, and starts Flight and the UI.
Every phase has a deadline and propagates failures. Setup jobs exit; there is no
setup marker, sleeping helper container, or forced recreation during normal start.
An interrupted initialization can be retried with `./demo init`.

Five services remain running: PostgreSQL, Keycloak, control plane, data plane,
and UI/proxy. PostgreSQL has separate databases and users for Keycloak,
configuration/tickets, and the Iceberg SQL catalog. One named warehouse volume
is mounted at `/warehouse`; services read it, and the seed job writes it.
The fixture contains nested structs/lists/maps and two files for parallel reads.

The Keycloak realm uses a single public issuer at
`http://localhost:20080/realms/dal-obscura-demo`. Servers exchange tokens and fetch
JWKS through the internal `keycloak:8080` address while validating that public
issuer. Browser login uses authorization code with PKCE; bootstrap browser login
is disabled. The CLI fixture client permits password grants solely for local read
checks. Administrative authority alone does not permit reading governed rows.

**Sign out** now opens Keycloak's logout confirmation after revoking the app
session. Confirm **Logout** to end SSO as well; signing in again should require
credentials. New realm imports register the UI origin as a valid post-logout
redirect. For an existing realm, add the exact UI origin (for example,
`http://localhost:28821`) to **Valid Post Logout Redirect URIs** on the
`dal-obscura-ui` client in Keycloak. Restarting Keycloak preserves its existing
realm and does not reapply the import file.

## Everyday commands

```bash
./demo up                   # Start existing images and check schema; preserve edits
./demo check                # Verify reads, then real Chromium SSO/login/reload/logout
./demo check --reads-only    # Verify reads without Node or a browser
./demo credentials          # Explicitly display local example passwords
./demo logs control-plane   # Redacted recent logs for a service
./demo down                 # Stop containers; retain databases, warehouse, and secrets
```

`check` installs the locked UI test dependencies if absent and ensures Chromium
is available. It validates all read tickets, row counts, regional restrictions,
email masking, nested structs/lists/maps, NULL masking for readers without grants,
and invalid-token rejection. Unexpected transport or backend failures fail the
check. Read expectations are for the seeded policies; intentional edits can
change those expected results.

Rerun `./demo init` to rebuild after changing service or UI code. Existing tables,
policies (including saved deny-all), owners, and runtime settings are preserved.
`up` does not seed or provision and does not rebuild images. Fresh initialization
needs network access for images and locked build dependencies; subsequent startup
uses local images.

## Ports and isolation

Set port overrides before the first initialization:

```bash
UI_PORT=28822 KEYCLOAK_PORT=20081 FLIGHT_PORT=28116 ./demo init
```

The values persist in `.env`. Changing them through shell overrides after
initialization is rejected rather than silently changing the registered callback.
Reset explicitly before selecting a different port configuration.

Each checkout gets its own Compose project and named volumes. Other projects
are not removed. Commands take a local lock to prevent concurrent initialization
or reset. Credentials stay in ignored, mode-0600 `.env`; normal logs redact them.
Keep that file with the volumes: replacing it would invalidate database and user
passwords. Keycloak imports the realm only when it does not already exist.

## Troubleshooting and reset

Use `./demo logs` or specify `postgres`, `keycloak`, `control-plane`, `data-plane`,
or `ui`. If ports are occupied by the previous example, stop those containers
before initializing this one; the new launcher does not silently stop another project.
If a build cannot fetch an artifact, restore registry/network access and rerun init.

**Reset deletes this example's databases, warehouse, user sessions, policies,
and generated credentials.** It leaves other projects alone.

```bash
./demo reset
./demo init
./demo check
```

The old `./run` launcher and multi-file `.runtime` layout are retired. Old example
volumes are not migrated into this new project. For TLS and production-oriented
controls, use the separate [secure local profile](../../../deployment/local-secure/README.md).
