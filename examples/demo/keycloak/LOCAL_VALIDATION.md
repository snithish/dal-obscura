# Local demo validation

Validation run: 2026-09-26, Podman Compose.

Start the demo with `./run up`. The management UI is served at
`http://localhost:28821` by default (or the configured
`DAL_OBSCURA_DEMO_UI_PORT`). Sign-in uses the configured Keycloak realm and the
normal OIDC authorization-code flow with PKCE.

## Passed

- `./run up` succeeded twice consecutively. Postgres, Keycloak, migrations,
  control plane, provisioning, UI, and Flight reached their expected healthy
  or successful completion states on both runs.
- `./run ui-smoke` passed in Chromium. It exercised Keycloak Authorization
  Code + PKCE login, session cookie acceptance, platform-admin access, seeded
  asset inventory, CSRF-protected logout, and session revocation.
- `./run smoke` passed for all five seeded personas: US and EU row filters and
  email masking, unrestricted steward and owner reads, and blocked-user denial.
- `pnpm build` and its JavaScript, CSS, and font size budgets passed during the
  Compose image build.
- `uv run pytest` passed: 861 passed, 12 skipped.
- `ruff check .`, `ruff format --check .`, and `ty check` all passed.

The UI origin is `http://localhost:28821` by default. The application retains
its `Secure` and `__Host-` session cookie settings; browsers treat localhost as
a secure context. API, database, Keycloak, and Flight ports remain bound to
loopback in the 20000 range.
