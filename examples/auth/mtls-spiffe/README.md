# SPIFFE mTLS Authentication

This example starts real SPIRE server and agent services, issues SPIFFE
X.509-SVIDs, and uses `MtlsIdentityProvider` to map the verified client SPIFFE
URI to `example-user`.

## Services

The main `compose.yaml` keeps the dal-obscura runtime visible and includes
`compose.spire.yaml` for the SPIRE-only infrastructure services.

- `spire-server`: real SPIRE server with a local self-signed authority.
- `spire-bootstrap`: generates a join token and registers workload entries.
- `spire-agent`: real SPIRE agent exposing the Workload API.
- `setup`: creates the table, provisions the mTLS identity mapping and policy
  through the control-plane API, then publishes the data-plane snapshot.
- `dal-obscura`: fetches its server X.509-SVID and starts with TLS client
  verification enabled.
- `client`: fetches its client X.509-SVID, performs the startup read over
  Flight TLS, and then stays running for interactive reads.

## Run

```bash
docker compose up -d --wait
docker compose logs client
docker compose exec client dal-obscura-example-read --target default.users
docker compose down --volumes
```

Expected success:

```text
mtls-spiffe: authenticated as example-user and read 2 rows
```

## Docker Caveat

The server and client share the SPIRE agent PID namespace in this Compose file.
That is required here because SPIRE's Unix workload attestor needs to resolve
the calling process for Workload API requests. In Kubernetes this is usually
handled by the SPIRE agent DaemonSet and platform-specific selectors instead of
this Compose-specific PID namespace arrangement.

## Security Caveats

Production SPIFFE deployments need trust-domain governance, registration-entry
lifecycle, agent hardening, SVID rotation, and clear mapping from SPIFFE IDs to
application principals.
