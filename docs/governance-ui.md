# Governance UI and identity

The React UI uses the existing component system. Navigation owns URLs and leave
guards. Query hooks own session-scoped server state. Policy draft modules own
edits, baselines, revision fences and save behavior. Wire types come from the
served OpenAPI schema. Screens, feature state, stories and unit tests live together
under `apps/governance-ui/src/features/`. App navigation, server communication and
shared widgets have separate owners. Browser journeys remain grouped under `e2e/`,
with performance and live-identity qualification kept explicit.

Keep actions discoverable, labels meaningful, identifiers readable and selected
column scope explicit. Preserve focus and scroll position while browsing nested
schema. Drafts survive navigation guards and concurrent revisions. Ignore stale
responses and clear identity-specific caches when the authenticated session changes.

Identity attribute configuration maps verified source claims to canonical internal
keys with domain enforcement. Policy authoring and mapping previews use the same
mapper. Missing attributes cannot satisfy policy conditions. Discovery exposes
provider configuration, labels, descriptions and allowed values; it does not
harvest user records or observed claims. Synthetic previews do not authenticate
identities or verify tokens.

Sign-out revokes the local session and exercises provider logout where supported.
Verify federated behavior with a live provider. Synthetic browser routes cannot
prove SSO logout. The Keycloak runner under `examples/demo/keycloak/` owns the
local live-provider workflow.

Regenerate wire contracts through the existing tools:

```sh
uv run --no-sync python scripts/export_control_plane_openapi.py
pnpm --dir apps/governance-ui generate:api-types
```

For UX changes, inspect real rendered states, keyboard focus, accessibility and
responsive layouts. Keep before/after review captures in `docs/ui-review/`, separate
from correctness tests. Research established patterns before substantial redesigns;
apply only patterns that fit governed data and identity authoring.

## Review and recovery

Run `pnpm --dir apps/governance-ui format` before review. The UI check includes
Biome formatting with a line width of 100 and two-space indentation for JavaScript,
TypeScript, JSX/TSX, JSON and CSS. Lint rules and import assists are disabled so
the gate only checks formatting. Markdown/MDX, YAML and HTML are outside the
format gate; HTML support remains experimental. Generated OpenAPI and client types
keep their generator's formatting. The [Biome configuration](../apps/governance-ui/biome.json)
owns file selection and output exclusions. Adapt wire values only where editor
state needs concrete defaults; the generated schema owns the response contract.

A render failure clears private queries and invalidates pending session work before
reconnection. Recovery reloads saved server state and warns that unsaved edits may
be lost. StrictMode and browser stories exercise session cleanup and remounts.

The production budget counts gzip bytes across all JavaScript chunks, including
lazy routes. Its allowance leaves about ten percent above the reviewed 200 KiB
baseline for routine changes. It remains a build failure when exceeded. The
[budget check](../apps/governance-ui/scripts/check-budgets.mjs) owns the limits.
A budget increase is a size allowance, not evidence of faster loading.
