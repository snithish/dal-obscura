# Governance UI

New policy authoring and management application for dal-obscura. It is a
separate administrative surface; it does not grant governed data-read access.

## Current slice

The first working slice implements an asset workspace with:

- bounded, searchable asset inventory loading from `GET /v1/assets/page`;
- revisioned draft loading and saving through `/v1/assets/{id}/draft`;
- schema-field selection, row restriction editing, and all six supported masks;
- synthetic persona policy evaluation through `/v1/assets/{id}/policy-evaluate`;
- editable synthetic principal, group, and claims inputs for server-side review;
- per-asset consumer handoff snippets for Python/DuckDB, Spark, and raw Arrow;
- bounded catalog connection diagnostics with redacted provider failures;
- authenticated Settings editing for the OIDC provider chain, with staged
  activation guidance and redacted secret preservation;
- local draft status, explicit save, and stale-test messaging.

Changes are saved as personal drafts and require a current server review before
publication. This app does not call the gateway data plane or render source rows.

## Development

```bash
cd apps/governance-ui
pnpm install
pnpm run dev
```

Vite proxies `/v1` and `/auth` to a local control plane at
`http://127.0.0.1:8821`. The normal route starts signed out. In the local
profile, enter the configured `DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN` in the
labelled local sign-in form; the server exchanges it for the same HttpOnly
session and CSRF cookie used by OIDC. Production disables this bootstrap route
and uses the SSO button, which starts the OIDC authorization-code/PKCE flow.
Failed authentication never loads policy data. Local development uses the same
authenticated browser session and CSRF flow as production; the only local
bootstrap is the explicitly configured control-plane token form. Production
disables that bootstrap route and uses the SSO button.

```bash
pnpm run check
pnpm run build
```

The production asset-serving and authenticated session integration are defined
in [U02–U03](../../docs/ui-v2/IMPLEMENTATION.md). Do not ship this development
proxy as a production authorization boundary.
