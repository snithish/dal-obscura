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
