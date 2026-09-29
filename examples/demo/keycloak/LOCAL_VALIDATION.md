# Local demo validation

Earlier validation run: 2026-09-26, Podman Compose.

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

## Startup regression fixed and rechecked (2026-09-27)

The first fresh-workspace start exposed a provisioning bug: the control plane
returns JSON `null` for `GET /v1/settings/runtime` until the workspace has a
runtime settings record. Provisioning incorrectly treated that valid empty
state as an invalid response. It now creates settings at revision zero, with a
regression test in `tests/examples/test_demo_initialization.py`.

After the fix, `./run up` succeeded twice consecutively without resetting the
demo or its Postgres volume. `./run smoke` passed all five personas, the UI
returned HTTP 200, the control plane `/readyz` returned `ok`, and `./run
ui-smoke` passed the Chromium OIDC sign-in, governed inventory, and sign-out
flow.

The UI origin is `http://localhost:28821` by default. The application retains
its `Secure` and `__Host-` session cookie settings; browsers treat localhost as
a secure context. API, database, Keycloak, and Flight ports remain bound to
loopback in the 20000 range.
