# Local HTTPS and SSO

Owner decision, 2026-09-22: remove Cloudflare Tunnel and Access integration.
The supported local deployment is `deployment/local-secure`, layering the same
production services behind loopback HTTPS. No tunnel connector, DNS provisioning,
edge account or external exposure is part of this project.

## Topology and authority

Browser → https://localhost:8443 → local TLS gateway → built UI/control plane.
The configured OIDC issuer authenticates users through exact callback/PKCE flows;
the application owns sessions, CSRF, role authorization and publication permissions.
Bootstrap is disabled. A reachable local IdP is required for local SSO; an external
issuer can be used when configured, but offline login must not be claimed for it.

Flight clients use grpc+tls://localhost:8815, with trusted certificates, required
client verification and application JWT/policy authorization. Browser cookies are
not Flight credentials. Database, backend and development ports remain private.

## Operator workflow

Follow [the secure-local runbook](../../deployment/local-secure/README.md).
Set immutable image digests and owner-only secrets, initialize the disposable CA,
trust that CA in the browser/client, configure a real issuer and exact callback,
then run `./run config`, `./run up` and `./run doctor`.
No source edits or disabled certificate verification are permitted. Diagnostics
must redact secrets and distinguish configuration checks from verified login.
`./run down` preserves data; destructive reset remains an explicit operator action.

## Remaining qualification

R02 owns canonical origins, trusted proxy attribution, cookies and session rules.
R03 owns secure-local lifecycle, secret/TLS validation and private bindings.
R09/E07 owns real allowed/denied users, role denial, logout, expiry, revocation,
provider outage, callback rejection and publication reconciliation. Record exact
candidate, commands and durable redacted evidence. Synthetic tests do not close
these gates. No external tunnel qualification remains a release prerequisite.
