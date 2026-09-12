# Production reference topology

This directory is a concrete single-customer deployment reference. It keeps
the control plane, stateless Flight workers, UI, and PostgreSQL on a private
network. Put a managed TLS/HTTP2 ingress in front of the UI and Flight service;
the Compose file binds only the UI listener to loopback for an operator-managed
ingress. Replace every image tag with a verified immutable digest.

## Install and start

1. Copy `.env.example` to `.env` and load every secret from the deployment's
   secret manager. Place the referenced certificate, private key, and client-CA
   files at the three `*_SOURCE` paths with mode `0400`; Compose mounts them as
   read-only secrets only into the data-plane workers. Do not put provider
   passwords, ticket keys, database credentials, or TLS private keys in Git.
2. Set the external governance hostname and Flight HTTP/2 hostname in the IdP,
   ingress, and redirect configuration. The control-plane production profile
   rejects non-TLS OIDC and browser redirect settings, weak bootstrap tokens,
   missing audiences, missing catalog egress policy, and demo login shortcuts.
   Set `DAL_OBSCURA_CELL_ID` to the UUID selected for the published data-plane
   cell and keep `DAL_OBSCURA_DATA_PLANE_PROFILE=production`; the data plane
   rejects an insecure Flight location, weak ticket secret, or non-PostgreSQL
   control-plane store.
3. Verify the image signatures/digests and the PostgreSQL backup policy, then
   run `docker compose --env-file .env config` and review the rendered topology.
4. Run `docker compose --env-file .env up migrate` and require exit code 0.
   Migrations are explicit and separate from routine service startup.
5. Start the service processes with
   `docker compose --env-file .env up -d postgres control-plane data-plane ui`.
   The application processes intentionally do not depend on the migration
   container; if the schema is missing or stale they fail closed and report the
   explicit migration command instead of migrating during startup.
6. Verify `GET /healthz` and `GET /readyz` through the private control-plane
   network, complete the real OIDC login, and run an authenticated synthetic
   Flight read through the TLS ingress before opening customer traffic.

Routine restart is `docker compose --env-file .env restart`; it does not seed,
reset, republish, or migrate state. Back up PostgreSQL and the referenced IdP,
secret, key, and certificate configuration before upgrades. A failed migration
or readiness check keeps ingress closed until the operator resolves it.

## Restore and emergency access invalidation

Restore PostgreSQL into an isolated environment, run the packaged migrations and
verify the schema before exposing any listener. Reconcile the active publication,
cell UUID, IdP configuration, secret references, and key versions. Before opening
ingress, invalidate credentials and durable replay artifacts in the restored
database:

```bash
dal-obscura-maintenance invalidate-access \
  --database-url "$DAL_OBSCURA_DATABASE_URL" \
  --cell-id "$DAL_OBSCURA_CELL_ID"
```

The command revokes every browser session and consumes pending OIDC login
transactions. With `--cell-id` it also deletes every stored Flight ticket for
that cell; omit the flag only when intentionally invalidating tickets for every
cell. Verify denied and allowed synthetic reads after invalidation, then open
the TLS ingress. Keep the restored environment closed if any reconciliation,
readiness, or synthetic read check fails.

## Boundary and operating requirements

- PostgreSQL is internal-only. Give migration jobs schema-change rights, the
  control plane normal application rights, and Flight workers only the narrow
  ticket-store rights they require. Do not allow customer clients to write
  trusted ticket or publication rows.
- Compose injects only the variables each role needs: PostgreSQL receives its
  database bootstrap values, migrations receive only the database URL, the
  control plane receives browser/admin OIDC settings, and Flight workers
  receive ticket/TLS/runtime settings. The admin token is never present in the
  PostgreSQL or data-plane process environment.
- The UI container is unprivileged, read-only, and has no source-data access.
  The UI never receives database, catalog, IdP client-secret, or ticket-secret
  values. Catalog credentials belong to the secret manager; configure
  `DAL_OBSCURA_CONTROL_PLANE_CATALOG_EGRESS_ALLOWLIST` with exact approved
  catalog/object-store hostnames. Private endpoints require explicit entries.
- Configure ingress rate limits, request/body limits, trusted proxy handling,
  HSTS, CSP, and HTTP/2 Flight forwarding. Keep PostgreSQL, migration, and
  health ports off the public interface.
- Keep the control-plane login limiter enabled with bounded attempts, window,
  and block settings. It hashes direct client keys, applies across API
  processes through PostgreSQL, and returns generic retryable failures. Only
  configured UI/CORS origins may mutate a cookie-authenticated session; do not
  use a forged `Host` or forwarded header as an origin trust signal.
- Establish a named operator for certificate/key rotation, backups, incident
  response, and emergency bootstrap-token retirement. Platform-admin MFA must
  be enforced by the IdP.

This reference is an implementation and configuration contract; it is not a
claim that a production image, TLS ingress, backup restore, multi-process race,
or customer release has already been executed in this checkout.
