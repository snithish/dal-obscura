# Local demo validation

Validation run: 2026-09-22 (HTTP gateway before Caddy migration)

The prior live validation below used the HTTP loopback gateway. The current
demo uses Caddy HTTPS at `https://governance.localhost` and
`https://keycloak.localhost`; rerun the live checks after trusting the local
Caddy root certificate before treating the HTTPS migration as validated.

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
