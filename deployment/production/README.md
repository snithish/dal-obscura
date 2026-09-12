# Production reference topology

This directory is a concrete single-customer deployment reference. It keeps
the control plane, stateless Flight workers, UI, and PostgreSQL on a private
network. Put a managed TLS/HTTP2 ingress in front of the UI and Flight service;
the Compose file binds only the UI listener to loopback for an operator-managed
ingress. Replace every image tag with a verified immutable digest.

## Install and start

1. Copy `.env.example` to `.env` and load every secret from the deployment's
   secret manager. Do not put provider passwords, ticket keys, database
   credentials, or TLS private keys in Git.
2. Set the external governance hostname and Flight HTTP/2 hostname in the IdP,
   ingress, and redirect configuration. The control-plane production profile
   rejects non-TLS OIDC and browser redirect settings, weak bootstrap tokens,
   missing audiences, and demo login shortcuts.
3. Verify the image signatures/digests and the PostgreSQL backup policy, then
   run `docker compose --env-file .env config` and review the rendered topology.
4. Run `docker compose --env-file .env up migrate` and require exit code 0.
   Migrations are explicit and separate from routine service startup.
5. Start the service processes with
   `docker compose --env-file .env up -d postgres control-plane data-plane ui`.
6. Verify `GET /healthz` and `GET /readyz` through the private control-plane
   network, complete the real OIDC login, and run an authenticated synthetic
   Flight read through the TLS ingress before opening customer traffic.

Routine restart is `docker compose --env-file .env restart`; it does not seed,
reset, republish, or migrate state. Back up PostgreSQL and the referenced IdP,
secret, key, and certificate configuration before upgrades. A failed migration
or readiness check keeps ingress closed until the operator resolves it.

## Boundary and operating requirements

- PostgreSQL is internal-only. Give migration jobs schema-change rights, the
  control plane normal application rights, and Flight workers only the narrow
  ticket-store rights they require. Do not allow customer clients to write
  trusted ticket or publication rows.
- The UI container is unprivileged, read-only, and has no source-data access.
  The UI never receives database, catalog, IdP client-secret, or ticket-secret
  values. Catalog credentials and egress policy belong to the secret manager.
- Configure ingress rate limits, request/body limits, trusted proxy handling,
  HSTS, CSP, and HTTP/2 Flight forwarding. Keep PostgreSQL, migration, and
  health ports off the public interface.
- Establish a named operator for certificate/key rotation, backups, incident
  response, and emergency bootstrap-token retirement. Platform-admin MFA must
  be enforced by the IdP.

This reference is an implementation and configuration contract; it is not a
claim that a production image, TLS ingress, backup restore, multi-process race,
or customer release has already been executed in this checkout.
