# Control Plane UI Design

## Summary

Add a same-process operator dashboard for the existing FastAPI control plane
without deploying a frontend framework or Node build pipeline.

The UI is a read/write admin console served from the control-plane process. It
uses resource-section navigation for tenants, cells, assignments, runtime
settings, catalogs, assets, policy rules, auth providers, and publications.

The important boundary is that `/v1` remains the only state API. The UI shell
may be rendered by Jinja and static assets, but templates must not call
`ProvisioningService`, query repositories, receive privileged control-plane
state directly, or receive the admin token. All data reads and writes go
through protected FastAPI API methods under `/v1`.

## Goals

- Replace the current curl-oriented provisioning workflow with a manageable
  operator dashboard.
- Keep deployment simple: one Python control-plane service, no frontend
  framework deployment, and no Node build step.
- Preserve rich UI behavior through lightweight vendored browser helpers and
  small local JavaScript.
- Make `/v1` the single administrative state surface for both machines and the
  browser UI.
- Add JSON read endpoints for inventory, draft state, publications, and active
  publication state.
- Keep the data plane independent from the control-plane HTTP service.

## Non-Goals

- Do not add a React, Vue, Svelte, or similar frontend framework.
- Do not add a separate frontend deployment artifact.
- Do not add persistent UI session state to the service.
- Do not add user/RBAC management in the first UI. The first version uses the
  existing control-plane admin token model.
- Do not let Jinja templates or UI presentation routes become an alternate
  repository/service access path.
- Do not store or render secret values. Existing secret-reference behavior
  remains unchanged.

## Key Decisions

- Serve the UI from the existing control-plane FastAPI process under `/ui`.
- Use resource-section navigation, not a cell-centered workspace or asset-only
  workbench.
- Use the current admin-token security model.
- Keep the initial `/ui` shell unprivileged and free of embedded control-plane
  data.
- Keep the admin token only in browser-side request state, such as memory or
  `sessionStorage`; responses never echo it.
- Use JSON `/v1` endpoints plus lightweight client-side rendering for section
  contents. This preserves the selected enhanced-HTML direction while honoring
  the rule that all state access must hit API methods.
- Vendored helper libraries are acceptable for richer navigation, searchable
  tables, drawers/dialogs, JSON editing, and toast feedback.

## Architecture

The control-plane service gains a UI presentation layer beside the existing API
adapter:

```text
browser
  |
  | GET /ui                         shell only, no privileged state
  | GET /ui/static/...              CSS, local JS, vendored helpers
  | GET/POST/PUT /v1/...            all control-plane reads and writes
  v
FastAPI control-plane process
  |
  | API routes call
  v
ProvisioningService
  |
  | repository operations
  v
config-store database
```

Jinja is allowed for the static shell, shared layout, and non-sensitive
presentation scaffolding. It is not allowed to load tenants, cells, assets,
policies, publications, or auth providers directly.

The UI JavaScript owns only presentation state: active section, selected row,
open drawer, current filters, table sort, and local form state. Authoritative
state always comes from `/v1` responses.

## UI Shape

The first dashboard uses persistent resource-section navigation:

- Tenants
- Cells
- Cell assignments
- Runtime settings
- Catalogs
- Assets
- Policy rules
- Auth providers
- Publications

Each section should support the expected admin-console interactions:

- searchable, sortable tables
- create and edit drawers or side panels
- inline validation messages
- success and error toast feedback
- explicit refresh after writes
- JSON editing for options, masks, auth-provider args, and path rules

The publications section should be more deliberate than ordinary CRUD:

- compile a cell draft
- show publication id, manifest hash, asset count, catalog count, and cell id
- show active publication status
- require an explicit activation action

## API Surface

The existing write endpoints remain the source of truth for mutations. The UI
uses them rather than private UI mutation routes:

- `POST /v1/tenants`
- `POST /v1/cells`
- `PUT /v1/cells/{cell_id}/tenants/{tenant_id}`
- `PUT /v1/tenants/{tenant_id}/cells/{cell_id}/runtime-settings`
- `PUT /v1/tenants/{tenant_id}/cells/{cell_id}/catalogs/{name}`
- `PUT /v1/tenants/{tenant_id}/cells/{cell_id}/assets/{catalog}/{target}`
- `PUT /v1/assets/{asset_id}/policy-rules`
- `PUT /v1/cells/{cell_id}/auth-providers`
- `POST /v1/cells/{cell_id}/publications`
- `POST /v1/cells/{cell_id}/publications/{publication_id}/activate`

Add read endpoints needed by resource sections:

- `GET /v1/tenants`
- `GET /v1/cells`
- `GET /v1/cell-tenant-assignments`
- `GET /v1/cells/{cell_id}/runtime-settings`
- `GET /v1/cells/{cell_id}/catalogs`
- `GET /v1/cells/{cell_id}/assets`
- `GET /v1/assets/{asset_id}/policy-rules`
- `GET /v1/cells/{cell_id}/auth-providers`
- `GET /v1/cells/{cell_id}/draft`
- `GET /v1/cells/{cell_id}/publications`
- `GET /v1/cells/{cell_id}/active-publication`

All endpoints use the same admin-token dependency as existing control-plane
write routes.

`GET /v1/cells/{cell_id}/draft` is an aggregate convenience endpoint for the
dashboard. It should return the same logical state used for publication
compilation: assigned tenants, runtime settings, catalogs, assets with policy
rules, and auth providers. It is still a normal `/v1` API method and must call
application/read-model methods rather than exposing templates to repositories.

## Components

### Inventory Read Model

Add read methods to `PublicationStore` and `ProvisioningService` for:

- tenants
- cells
- cell assignments
- runtime settings
- catalogs
- assets
- policy rules
- auth providers
- publications
- active publication state
- aggregate cell draft state

The methods should return plain dict/list structures suitable for FastAPI JSON
responses. They should keep SQLAlchemy ORM objects out of interface responses
and templates.

### API Adapter

Extend `src/dal_obscura/control_plane/interfaces/api.py` with the JSON read
routes. Keep request/response validation explicit with Pydantic where useful.

The UI depends on these API routes exactly as any external client would. Only
the `/v1` API adapter calls `ProvisioningService` for state reads or writes;
private `/ui` routes must not.

### UI Adapter

Add a presentation-only UI adapter, likely
`src/dal_obscura/control_plane/interfaces/ui.py`, mounted by `create_app()`.

It owns:

- `GET /ui`
- static asset mounting
- shell/template rendering
- no privileged data loading
- no provisioning writes
- no repository reads

### Static Assets

Package UI assets with the Python project:

- templates for the shell and empty/error states
- CSS for the operator dashboard
- local JavaScript modules for API calls and rendering
- vendored helper libraries for tables, drawers/dialogs, JSON editing, and
  toast notifications

Vendored assets should be committed in readable form with source/version notes.
They should not require install-time network access or a build step.

## Authentication Boundary

The admin token is never passed into Jinja. It is never rendered into HTML.

The UI asks the operator for the admin token in the browser and attaches it as
`Authorization: Bearer ...` on `/v1` requests. The token may be kept in memory
or `sessionStorage` for the browser session. The first implementation should
avoid durable local storage unless explicitly approved later.

`GET /ui` may be public in the sense that it returns only static shell content,
but every stateful API call remains protected by the existing admin dependency.

## Error Handling

The UI should expose API failures directly and recoverably:

- `401`: show token prompt or session-expired state while keeping the shell
  loaded.
- `400`: show inline validation errors using the API `detail` response.
- `404`: show section-level empty or not-found state without breaking
  navigation.
- publication compile failures: show a blocking validation panel with cell and
  publication context.
- network errors: show a toast plus retry action.
- successful writes: refresh affected section data from `/v1`, rather than
  trusting local optimistic state.

## Testing

Follow TDD for implementation.

Required first tests:

- API inventory tests prove `GET /v1/...` endpoints are protected by the admin
  token and return tenant/cell/draft/publication/active state after writes.
- UI shell test proves `GET /ui` returns the shell without requiring,
  embedding, or echoing the admin token.
- Boundary tests prove UI templates/routes do not call `ProvisioningService`
  for privileged state or query repositories directly.
- HTML/static smoke tests prove resource navigation and browser hooks target
  `/v1` endpoints, not private UI mutation routes.
- Existing publish-flow tests stay green, with added read-after-write coverage.

Implementation should verify focused control-plane tests first, then the wider
test set used by the repo where feasible.

## Open Implementation Notes

- The first implementation can start with JSON-rendered section contents and
  progressively add richer helper-library behavior.
- If a future endpoint returns HTML fragments, it must still be a protected
  `/v1` API method backed by the same read models. It must not become a hidden
  Jinja-to-repository access path.
- README should be updated when the UI is implemented with launch instructions
  and any vendored-helper provenance.
