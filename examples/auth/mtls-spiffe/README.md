# SPIFFE mTLS Authentication

This example starts real SPIRE server and agent services, issues SPIFFE
X.509-SVIDs, and uses `MtlsIdentityProvider` to map the verified client SPIFFE
URI to `example-user`.

## Services

- `spire-server`: real SPIRE server with a local self-signed authority.
- `spire-bootstrap`: generates a join token and registers workload entries.
- `spire-agent`: real SPIRE agent exposing the Workload API.
- `setup`: creates the table, policy, and mTLS app config.
- `dal-obscura`: fetches its server X.509-SVID and starts with TLS client
  verification enabled.
- `client`: fetches its client X.509-SVID, presents it over Flight TLS, and
  reads the table.

## Run

```bash
docker compose up --build --abort-on-container-exit --exit-code-from client
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
