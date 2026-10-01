# Secure local demo

A self-contained HTTPS/OIDC and Flight mTLS example, using product images built
from this checkout and the same Iceberg fixture as the basic Keycloak demo.
Both application services enforce their production profiles.

## First start

Requirements: `uv`, OpenSSL 3, Docker Desktop with Compose v2 or a running Podman
machine with Compose v2, at least 4 GiB available container memory, and disk space
for image builds. Node 24 and pnpm are needed only for the browser check.

```bash
cd deployment/local-secure
./demo init
./demo credentials
./demo check
```

Open **https://localhost:28443**, choose **Sign in with SSO**, and use `demo-admin`
with its generated password. For manual browsing, trust the public local CA at
`.tls/ca.crt` in your browser or user trust store. The launcher never changes OS
trust. The automated check verifies this CA and pins the two browser endpoint
public keys only for its own test process.
Always use `localhost`, matching the issuer, callback, certificates, and cookies.

No external IdP, image digest lookup, copied environment template, or manual
secret replacement is required. Initial setup builds the service, UI, and edge images,
generates one private `.env` and a complete local certificate set, starts
PostgreSQL and HTTPS Keycloak, migrates and grants database access, seeds Iceberg,
provisions configuration through authenticated API calls, and starts the apps.
Every phase has a deadline and failed jobs stop startup. Rerun `./demo init` after
an interruption; existing credentials, CA, tables, and authored state survive.

Six services remain running: PostgreSQL, Keycloak, control plane, data plane,
UI/proxy, and Caddy HTTPS edge. Setup jobs exit; no sleeping helper or marker files
control readiness. The backend network is internal. Only these ports are exposed,
all bound to loopback:

- `28443`: browser HTTPS.
- `24443`: HTTPS Keycloak, issuer `https://localhost:24443/realms/dal-obscura-demo`.
- `28815`: Flight mTLS, `grpc+tls://localhost:28815`.

Published services also join an ingress network for host return routing, including
on Podman VMs. PostgreSQL, control plane, and UI remain on the internal backend.

The basic demo uses different ports and a different Compose project, so both can
run together. This profile is standalone; it does not layer over production
Compose or silently stop another project's services.
Use separate browser profiles for concurrent sessions: cookies are scoped by
hostname, so the two `localhost` demos share cookie names despite different ports.

## Security exercised by this example

Browser SSO uses authorization code with PKCE, HTTPS secure cookies, and CSRF
checks. Bootstrap login is disabled. Internal OIDC token/JWKS calls also use
HTTPS and validate the local CA and `keycloak` hostname. Keycloak runs in
production mode; its HTTP health endpoint is confined to container loopback.
The fixture's confidential CLI client permits password grants for automated
local checks; administrative authority does not grant data access.

Flight requires a trusted client certificate and a valid OIDC token. Separate
keys serve the browser edge, Keycloak, Flight, and the example client. The CA key
stays on the host; each service receives only its own TLS material, copied into
named volumes with the correct container UID and owner-only key permissions.
Application containers use read-only roots, temporary writable directories,
dropped capabilities, and no-new-privileges.

PostgreSQL has separate Keycloak and Iceberg databases. Configuration uses a
migration owner, control-plane writer, and data-plane reader. The reader can
write ticket exchange/revocation state; it cannot author policies or create
schema objects. Catalog credentials remain scoped environment secret references.
The named Iceberg warehouse is writable only by the finite seed job and mounted
read-only in both application services.

## Everyday commands

```bash
./demo up                   # Start existing images; check schema; preserve edits
./demo check                # Verify TLS, permissions, governed reads, and browser SSO
./demo check --reads-only    # Same server checks, without host Node or Chromium
./demo doctor               # Verify HTTPS chains/hostnames and six healthy services
./demo config               # Validate Compose without printing secrets
./demo credentials          # Explicitly show local passwords
./demo logs control-plane   # Recent redacted logs; omit service for all logs
./demo down                 # Preserve databases, warehouse, credentials, and CA
```

`check` proves positive governed reads, all file tickets, regional row filters,
email masks, nested structs/lists/maps, NULL masking for no-grant readers, and
invalid-token rejection. It then requires TLS rejection for missing or untrusted
client certificates and an untrusted server CA, and verifies database role
permissions. Unexpected network/backend errors fail the check. Browser checks
cover real HTTPS SSO, secure session cookies, CSRF rejection, reload, and logout.
Read expectations assume the fixture policies; intentional edits may change them.

`up` does not build, migrate, seed, or provision. Rerun `init` after source changes
or when applying migrations; saved policies (including deny-all), owners,
settings, and table edits remain intact. `./run` is a compatibility alias for
`./demo`.

## State, ports, and recovery

Generated `.env` is mode 0600; `.tls` is mode 0700 and private keys are mode 0600.
Both are ignored by Git. Keep them together with their named volumes. Commands
use a local lock, preserve passwords across starts, redact secrets from logs, and
reject changed port/image overrides after initialization.

Choose ports before first initialization if defaults are occupied:

```bash
UI_PORT=28444 KEYCLOAK_PORT=24444 FLIGHT_PORT=28816 ./demo init
```

Expired, incomplete, or mismatched TLS material fails validation rather than
being replaced behind existing clients. Certificates last one year. Restore a
complete saved identity set or explicitly reset this disposable profile.

**Reset deletes this profile's databases, warehouse, policies, sessions,
credentials, and local CA. Other projects remain intact.**

```bash
./demo reset
./demo init
./demo check
```

After reset, manual browsers must trust the newly generated public CA. The old
manual `.env.example`, external-IdP setup, and production Compose layering are
retired. Old `dal-obscura-local-secure` volumes are preserved and are not migrated
into the new per-checkout project. Use the separate
[production reference](../production/README.md) for deployment configuration.

Keycloak TLS and management settings follow its official
[TLS guide](https://www.keycloak.org/server/enabletls) and
[management interface guide](https://www.keycloak.org/server/management-interface).
