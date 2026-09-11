# Governance UI

New policy authoring and management application for dal-obscura. It is a
separate administrative surface; it does not grant governed data-read access.

## Current slice

The first working slice implements an asset workspace with:

- asset inventory loading from `GET /v1/assets`, with clearly labelled demo
  content when no control plane is available;
- existing-rule loading and saving through `/v1/assets/{id}/policy-rules`;
- schema-field selection, row restriction editing, and all six supported masks;
- synthetic persona policy evaluation through `/policy-preview`;
- local draft status, explicit save, and stale-test messaging.

Changes remain drafts until the upcoming reviewed-publication slice. This app
does not call the gateway data plane or render source rows.

## Development

```bash
cd apps/governance-ui
pnpm install
pnpm run dev
```

Vite proxies `/v1` to a local control plane at `http://127.0.0.1:8821`. Use a
browser session authorized for that API. Without it, the application renders
the labelled synthetic demo workspace so interaction work can proceed without
mistaking a mock result for a live policy decision.

```bash
pnpm run check
pnpm run build
```

The production asset-serving and authenticated session integration are defined
in [U02–U03](../../docs/ui-v2/IMPLEMENTATION.md). Do not ship this development
proxy as a production authorization boundary.
