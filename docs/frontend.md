# Frontend Conventions

dal-obscura uses a small React application for the control-plane UI. The UI is
an operational console for catalogs, assets, policies, publishing, and settings.
It should stay understandable for contributors who primarily work in Python.

## Stack

- React
- TypeScript
- Vite
- pnpm
- React Router
- React Hook Form
- Zod
- Radix primitives
- Tailwind CSS

Do not add Redux, Zustand, GraphQL, generated API clients, Next.js, or a heavy
component suite without a design update. Start with local component state and
typed fetch helpers. Add a dependency only when repeated local code is clearly
more complex than the dependency.

## Folder Layout

```text
ui/
  src/
    api/        typed fetch helpers and API types
    app/        router, shell, providers
    components/ shared UI primitives
    features/   assets, catalogs, policies, publish, settings
    styles/     Tailwind globals and tokens
```

Feature-specific components stay inside their feature directory until another
feature actually reuses them. Shared components should be boring and stable.

## API Pattern

Keep API calls in `ui/src/api/`. Each helper should:

- accept plain values,
- return typed JSON,
- throw `ApiError` for non-2xx responses,
- avoid embedding presentation decisions.

Do not call legacy tenant/cell routes from React pages unless the migration
plan explicitly requires it. Prefer workspace endpoints such as `/v1/assets`.

## Forms

Use React Hook Form for non-trivial forms and Zod for validation schemas. Keep
simple filter inputs as local state. Show validation messages next to the field
that caused them.

## Errors

Show user-facing errors in the same surface where the action happened. For
publish, activation, and destructive actions, use a confirmation dialog and make
the final action label explicit.

## Styling

Use Tailwind utility classes plus the tokens in `tailwind.config.ts`. The UI
should be dense, calm, and suited to repeated administrative work. Avoid
marketing heroes, nested cards, decorative gradients, and UUID-first labels.

Visible copy must not use tenant or cell terminology.

## pnpm Supply-Chain Policy

The root `pnpm-workspace.yaml` enforces:

- 14-day minimum package age (`minimumReleaseAge: 20160`).
- strict release-age behavior.
- fail-closed behavior when publish time is missing.
- no trust-policy downgrade.
- blocked transitive exotic sources.
- explicit build-script approvals.
- dependency verification before `pnpm run` and `pnpm exec`.

Use:

```bash
/private/tmp/dal-obscura-pnpm/node_modules/.bin/pnpm install
/private/tmp/dal-obscura-pnpm/node_modules/.bin/pnpm --filter @dal-obscura/control-plane-ui build
```

If pnpm is installed on your PATH, `pnpm install` and
`pnpm --filter @dal-obscura/control-plane-ui build` are equivalent.

## Adding Dependencies

Before adding a dependency, document:

- why it is needed,
- whether a small in-repo helper would be simpler,
- whether it has install scripts,
- whether it pulls exotic sources,
- whether it needs `allowBuilds`,
- how hard it would be to remove later.

Commit `pnpm-lock.yaml` with any dependency change.

## Local Development

Run the backend:

```bash
export DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:///runtime/control-plane.db
export DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN=dev-admin
uv run dal-obscura-control-plane
```

Run the frontend:

```bash
cd ui
pnpm install
pnpm dev
```

Vite proxies `/v1` to `http://127.0.0.1:8820`.

Production UI is not packaged into the Python control-plane API. Build the
standalone Caddy-served UI image instead:

```bash
docker build -f ui/Dockerfile -t dal-obscura-control-plane-ui:local .
```

Set `DAL_OBSCURA_API_BASE_URL` in the UI container to the browser-visible API
origin, for example `http://127.0.0.1:8820` in local demos.
