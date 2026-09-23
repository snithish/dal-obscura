# Local demo validation

Validation run: 2026-09-22

Endpoint update: 2026-09-23. The local demo now uses loopback ports in the
20000 range and no longer has a Caddy gateway. Runtime evidence below predates
this endpoint update; repeat the live OIDC and stack-health checks for the new
ports before treating them as validated.

Start the demo with `./run up`. The management UI is served at
`http://127.0.0.1:28821` by default (or the configured
`DAL_OBSCURA_DEMO_UI_PORT`). Sign-in uses the configured Keycloak realm and the
normal OIDC authorization-code flow with PKCE.

## Passed

- `ui-smoke`: browser session creation, authorization, and logout passed.
- Live Playwright OIDC acceptance: the UI redirected to Keycloak, completed
  the PKCE callback for `demo-admin`, exposed the seeded platform-admin
  capability and asset inventory, then signed out and returned to the
  unauthenticated state.
- Focused control-plane, demo fixture, schema-admission, and local-demo
  architecture tests passed.
- `tests/test_e2e_smoke.py` passed.
- The fresh Compose stack became healthy, including the control plane, UI,
  Keycloak, and Flight server.

## Pending Flight demo check

The role-by-role demo smoke did not complete. Its first `us-analyst` read was
rejected during planning. Diagnostics showed that Iceberg's `string` schema
type was compared literally with Arrow's equivalent `large_string` type. The
code now normalizes those equivalent type names, including inside nested
struct types, and the schema-admission regression test passes.

The running demo container still has the image built before this fix. Rebuilding
that standard runtime image failed because the Podman VM ran out of storage
(about 1 GB free in a 28 GB VM). No existing image or volume was removed. The
Flight demo smoke, including all personas and the denied-user case, must be run
after the updated image can be built. Do not treat this record as full demo
acceptance until that check passes.

The demo fixture provisioner now reads the live schema and admits each nested
field with its stable provider ID and typed path. Its tests cover nested struct
and list paths. These changes are covered by tests but still need a successful
Flight run through the rebuilt demo image.
