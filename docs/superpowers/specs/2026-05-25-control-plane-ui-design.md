# Control Plane UI Redesign

## Summary

dal-obscura will replace the current raw HTMX control-plane UI with a React
asset-governance console. The visible product model becomes a single workspace
with catalogs, assets, owners, policies, publishing, and settings. Tenant and
cell concepts are removed from the UI and simplified API surface; any runtime
identity needed by the data plane remains an internal backend concern.

The first screen is an asset search/workspace, not a setup wizard or marketing
page. Admins and owners should be able to find a governed table, understand its
policy state, edit access rules through guided controls, preview the result, and
publish changes with confidence.

## Goals

- Provide a polished operational console for an open-source data-governance
  project.
- Make asset search the default workflow.
- Remove visible tenant and cell concepts from product copy, navigation, and
  new API endpoints.
- Replace raw JSON/module editing with opinionated forms and advanced escape
  hatches.
- Provide a guided policy builder for owners with schema-aware controls and
  preview.
- Keep frontend choices grounded for contributors who are stronger in Python
  than TypeScript.
- Use pnpm with strict supply-chain mitigations before adding JavaScript
  dependencies.
- Remove HTMX and server-rendered partials entirely.

## Non-Goals

- Build a custom authentication or user lifecycle system.
- Implement full RBAC enforcement in the first UI migration.
- Implement SCIM, directory sync, or IdP user search.
- Build policy-as-code editing as the default v1 owner experience.
- Add multi-workspace or multi-tenant product concepts to the UI.

## Product Model

The visible model is:

- Workspace
- Catalogs
- Assets
- Owners
- Policies
- Publish
- Settings

The backend may temporarily map this model to the existing tenant/cell schema
using a default workspace/runtime context. New UI-facing endpoints should not
require tenant or cell identifiers.

## Navigation

Primary navigation:

- Assets: home screen for search, filtering, policy status, ownership status,
  and opening asset workspaces.
- Catalogs: connect catalogs, inspect sync health, discover tables, and promote
  discovered tables into governed assets.
- Policies: cross-asset policy queue and shortcuts into asset workspaces.
- Publish: validate draft changes, review diffs, create publications, and
  activate releases.
- Settings: runtime settings now; auth and claim mapping later.

## User Responsibilities

The UI should be designed around these eventual responsibilities:

- Platform Admin: manages catalogs, owners, settings, publishing, and all
  policies.
- Asset Owner: manages policy builder and preview for assigned assets.
- Auditor: read-only review role in a later phase.

Auth is parked for this design. The first React migration can run as an
admin-capable UI while preserving clear seams for future authorization checks.

## Core Screens

### Assets

The Assets page is the home screen.

Required controls:

- Search input for asset name, catalog, schema, and table identifier.
- Catalog filter.
- Owner filter.
- Policy status filter.
- Summary strip with total assets, unowned assets, missing policy, and draft
  changes.
- Asset table with asset name, catalog, owner, policy status, last discovered,
  and draft status.

Rows open the asset workspace. Empty state points to adding a catalog.

### Catalogs

Catalogs is both setup and operations.

Required behavior:

- Catalog cards show connection status, last sync, discovered table count, and
  governed asset count.
- Add catalog opens a drawer.
- Default catalog setup uses presets such as Iceberg REST, Iceberg SQL catalog,
  and local/dev.
- Advanced module/options JSON remains available behind an explicit advanced
  section.
- Discovered tables can be reviewed and promoted into governed assets.

### Asset Workspace

Tabs:

- Overview: schema, catalog, owner, status, and policy coverage.
- Policy Builder: guided allow, deny, mask, and row-filter rules.
- Preview: preview as a principal or group with masked output.
- Changes: draft vs active publication diff.

Activity/audit history is deferred until the backend has audit events.

### Policy Builder

The builder is the default owner experience.

Required rule sections:

- Who: subject and group selectors, entered manually until directory search is
  available.
- Columns: schema-aware column selection.
- Masks: per-column mask selection.
- Row filter: DuckDB SQL expression editor with validation errors.
- Effect: allow or deny, with validation that deny rules cannot define masks or
  row filters.

Policy authoring should keep preview nearby. Owners should not need to edit raw
JSON for common policies.

### Publish

Publish is a release-review workflow.

Required behavior:

- Group draft changes by catalog, asset, policy, and runtime settings.
- Validate the draft before publishing.
- Show draft vs active publication impact.
- Require confirmation for publish and activation.
- Show publication history.

## UX Rules

- Prefer drawers for create/edit flows launched from lists.
- Prefer full pages for deep asset workflows.
- Do not show UUIDs as primary labels.
- Do not expose tenant or cell in visible copy.
- Use operational labels such as Missing policy, Unowned, Draft changes, and
  Ready to publish.
- Use confirmation dialogs for destructive, publish, and activation actions.
- Keep text concise and specific to the current task.
- Avoid nested cards and marketing-style hero layouts.
- Use dense but calm layouts suited to repeated administrative work.

## Frontend Stack

Use:

- React
- TypeScript
- Vite
- pnpm
- React Router
- React Hook Form
- Zod
- Radix primitives
- Tailwind CSS
- CodeMirror later for SQL/YAML editing

Avoid in v1:

- Next.js
- Redux
- Zustand
- GraphQL
- generated API clients
- heavy enterprise component suites
- TanStack Query/Table unless the first implementation proves they are needed

The frontend should start with typed fetch helpers and plain React state. Add
state/query/table libraries only when local complexity justifies the dependency.

## Frontend Structure

Create:

```text
ui/
  package.json
  pnpm-lock.yaml
  src/
    api/
    app/
    components/
    features/
      assets/
      catalogs/
      policies/
      publish/
      settings/
    styles/
  dist/
```

Responsibilities:

- `src/api/`: typed fetch helpers, response types, error handling.
- `src/app/`: router, app shell, root providers.
- `src/components/`: shared UI primitives and layout components.
- `src/features/`: feature-owned pages, forms, and local components.
- `src/styles/`: Tailwind globals and design tokens.

## Backend Integration

Remove the HTMX interface:

- Delete vendored HTMX assets.
- Delete server-rendered shell and fragment response paths.
- Remove `HX-Request` partial behavior from `/v1` routes.
- Keep FastAPI endpoints JSON-only.

Change `control_plane/interfaces/ui.py` to serve the Vite production build:

- `/ui` returns `ui/dist/index.html`.
- `/ui/{path:path}` falls back to the React app for client-side routes when the
  path is not a static asset.
- Vite assets are served from the built assets directory.

Development flow:

```bash
uv run dal-obscura-control-plane
cd ui
pnpm install
pnpm dev
```

Vite proxies `/v1` to the FastAPI control-plane process during development.

## Simplified API Surface

Add workspace-oriented endpoints for the React UI:

- `GET /v1/workspace/summary`
- `GET /v1/catalogs`
- `PUT /v1/catalogs/{name}`
- `GET /v1/assets`
- `GET /v1/assets/{asset_id}`
- `PUT /v1/assets/{asset_id}/owners`
- `GET /v1/assets/{asset_id}/policy-rules`
- `PUT /v1/assets/{asset_id}/policy-rules`
- `GET /v1/publications/draft`
- `POST /v1/publications`
- `POST /v1/publications/{publication_id}/activate`

These endpoints should map to the current store through a default workspace
until the persistence model is simplified.

## Supply-Chain Controls

Use a root `pnpm-workspace.yaml` with explicit security defaults:

```yaml
packages:
  - ui

minimumReleaseAge: 20160
minimumReleaseAgeStrict: true
minimumReleaseAgeIgnoreMissingTime: false
trustPolicy: no-downgrade
blockExoticSubdeps: true
strictDepBuilds: true
dangerouslyAllowAllBuilds: false
verifyDepsBeforeRun: error

allowBuilds:
  esbuild: true
```

Rationale:

- `20160` minutes is a 14-day package maturity delay.
- Strict release-age enforcement avoids falling back to fresh versions.
- Missing package publish timestamps fail closed.
- Trust-policy downgrade checks reduce the risk of accepting weaker publisher
  evidence.
- Transitive exotic sources are blocked.
- Dependency build scripts require explicit approval.
- All-build-script execution remains disabled.
- `pnpm run` and `pnpm exec` fail when dependencies are out of sync.

CI requirements:

- Use `pnpm install --frozen-lockfile`.
- Run `pnpm audit --audit-level high`.
- Run `pnpm audit signatures`.
- Commit `pnpm-lock.yaml`.

Dependency admission checklist:

- Explain why the dependency is needed.
- Prefer packages with broad adoption and active maintenance.
- Reject dependencies with unnecessary install scripts.
- Review transitive dependency count and exotic sources.
- Document any `allowBuilds` approval.
- Prefer in-repo code for small utilities.

## Testing

Backend:

- Add API tests for simplified workspace endpoints.
- Preserve publication compiler and data-plane tests.
- Remove tests that assert HTMX shell or partial behavior.

Frontend:

- Start with smoke coverage for route rendering and policy-builder validation
  helpers.
- Add Playwright smoke tests once the React app has the Assets, Catalogs, Asset
  Workspace, and Publish flows.

Manual verification:

- Run FastAPI control plane.
- Run Vite dev server.
- Confirm `/ui` loads the React app in production build mode.
- Confirm tenant and cell are absent from visible UI copy.

## Documentation

Add `docs/frontend.md` with:

- Stack choices and rejected alternatives.
- Folder layout.
- API helper pattern.
- Form pattern.
- Error handling pattern.
- Styling and component rules.
- pnpm supply-chain policy.
- How to add a dependency.
- Local development commands.

Update `README.md` with React UI development and production build commands.

## Phasing

Phase 1:

- Remove HTMX UI.
- Add React/Vite/pnpm scaffold and hardened pnpm config.
- Serve the React build from FastAPI.
- Add `docs/frontend.md`.

Phase 2:

- Add simplified workspace API endpoints.
- Build app shell, Assets page, Catalogs page, and basic asset workspace.

Phase 3:

- Build guided policy builder, validation, and preview.
- Build publish review page.

Phase 4:

- Add auth/RBAC enforcement, asset-owner scoping, and auditor read-only flows.

## Approved Decisions

- React + TypeScript + Vite is the frontend stack.
- pnpm is the package manager.
- Package maturity delay is 14 days.
- HTMX is removed rather than maintained in parallel.
- The home screen is asset search.
- The default policy authoring experience is Guided Builder + Preview.
- Tenant and cell are removed from the visible UI model.
