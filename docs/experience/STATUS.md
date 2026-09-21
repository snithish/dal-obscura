# Execution ledger

Implementation authorized 2026-09-20. Plan: [R01–R12](ACTION_PLAN.md).
No packet is DONE until all of its acceptance criteria pass.

## R06 — ACTIVE: exact identity cache isolation

Replaced delimiter-concatenated issuer/subject cache scope with an unambiguous
serialized tuple. The existing QueryClient test now exercises a collision pair
against actual cached inventory: failed before the fix, passes afterward.
Each accepted session load now also uses its load generation in cache keys and
clears the old cache before loading inventory. A second real QueryClient test
proves same-identity reauthentication cannot reuse prior inventory, even if its
cache lifetime has not expired. Both cases failed before their fixes.
Rendered interleaving coverage remains open. No authentication or pickle changes.

Validation: all 16 existing UI unit tests and TypeScript check pass on bundled
Node 24.19.0. Initial collision-only validation also passed on Node 26.8.2.
Commit identities are available from this file's history.

## R01 — ACTIVE: build tooling ownership

Moved Vite and its React build plugin from runtime to development dependencies,
retaining exact versions/integrities. Frozen offline install passed with the
cached pnpm 12.3.4 binary and Node 24.19.0. Production build passed: JS 101.38 kB
gzip, CSS 5.28 kB gzip. Existing package-boundary, route-inventory, plugin-contract
and registry tests: 38 passed. Full support/advisory and clean-artifact evidence
remain open; these focused checks do not close E01.

## Remaining packets

R04: ACTIVE; Mantine production-CSP foundation qualified below.
R05: ACTIVE; initial executable designbook below.
R02/R03/R07–R12: OPEN. No production readiness or live Cloudflare claim.

## R04/R06 — ACTIVE: Mantine shell and CSP foundation (2026-09-21)

Pinned MIT-licensed Mantine core/hooks 9.6.1 with compatible React peers. Added
self-hosted IBM Plex Sans/Mono and a shared Mantine provider/theme. Login, shell,
mobile navigation and command search now use real library components. Removed
their replaced CSS and custom focus/theme management. Disabled navigation keeps
explicit capability guards. Authentication and pickle behavior are unchanged.

NGINX generates a per-response style nonce, injects it into the shell and CSP,
and prevents shell caching/304 reuse. Mantine and scroll-lock styles receive that
nonce. Scripts remain self-only; style attributes remain forbidden. Built Vite
preview uses the same CSP contract for browser regression checks.

Proof: 8 browser tests pass against the actual NGINX 1.27.5 image, including
wrong/missing nonce rejection, fresh nonces, keyboard focus, OS theme changes,
narrow viewport and signed-out axe checks. API responses in these tests are
synthetic: this is not SSO or authenticated workflow evidence. All 16 UI unit
tests, TypeScript and 10 focused deployment/UI-shell Python tests pass. Production
build: JS 165.60 kB gzip, CSS 39.59 kB gzip; WOFF2 fonts 85.73 kB. These meet size
budgets only; no latency/LCP claim. Frozen offline install passes.

Registry blocker resolved using a temporary loopback curl relay to the official
npm registry, with TLS and package integrity checks retained. No relay URL is
stored in the lockfile or runtime configuration.

Remaining: convert feature controls and competing legacy CSS values, measure both
themes across authenticated screens, finish workbench/query ownership, Storybook,
and live identity/provider qualification. R04/R06 are not DONE.

## R05 — ACTIVE: executable designbook foundation (2026-09-21)

Storybook 10.6.0 React/Vite, Docs, accessibility and Vitest integration are pinned
as development dependencies. Vitest/browser provider 4.1.11 satisfies the addon
peer range (the newer Vitest 5 is not supported by that range). Storybook imports
the actual AppProviders, theme, styles, LoginPanel and AppShell. Ten stories cover
semantic palette/type/icons, login unavailable/rejected/pending states, reader
and administrator navigation, and representative dark themes. Contribution MDX
documents ownership, limitations and remaining workflow coverage.

Ten browser story tests pass, including axe and actual retry/submission/capability
behavior. They first exposed two real contrast failures: dark primary-button text
and the connected badge. The shared filled-button default and semantic status
tokens now fix those failures. No accessibility rule was disabled. Static
Storybook and production application builds pass; app JS 165.63 kB gzip, CSS
39.63 kB gzip. TypeScript and frozen offline install pass. CI now builds and tests
stories using its existing Chromium install. Storybook has a separate Vite config
without production API/auth proxies, loopback binding and telemetry disabled.

Commands from apps/governance-ui: `pnpm storybook`, `pnpm build-storybook`,
`pnpm test:stories`. Story tests use Playwright Chromium; the existing
DAL_OBSCURA_E2E_EXECUTABLE_PATH override also works for a local browser install.

Still OPEN before E10/E11 can pass: network interception with fail-on-unexpected
requests (do not add networked stories before this), complete documented component/
pattern/workflow coverage, contrast matrix, reviewed responsive visual baselines,
manual keyboard/screen-reader/zoom checks and owner visual approval. Current
stories are synthetic and do not prove SSO or backend authorization. Production
Playwright retains distinct CSP, focus and OS-theme boundary checks; no existing
security tests were removed in favor of stories. R05 is not DONE.

## R05 — ACTIVE: Storybook network boundary (2026-09-21)

Added a preview-level network guard and a contribution story that proves
synthetic stories fail immediately when they attempt to call `/v1/*`, `/auth/*`,
an identity provider, or another origin. The guard wraps fetch, XHR and beacon
requests and returns Storybook's cleanup callback so one story cannot leak its
boundary into the next. The designbook now documents this rule and keeps real
API/IdP/customer-data journeys in application browser suites. The story uses
Mantine primitives for its explanation; it does not duplicate product UI.

Proof: `pnpm check`, `pnpm build`, `pnpm test`, `pnpm build-storybook`, and
`pnpm test:stories` all pass from `apps/governance-ui`; the browser suite now
reports 4 files and 11 tests passing with the pinned Playwright Chromium. No
backend, authentication, or pickle code changed.

Still OPEN before E10/E11 can pass: complete documented component/pattern/
workflow coverage, contrast matrix, reviewed responsive visual baselines,
manual keyboard/screen-reader/zoom checks, owner visual approval, and the
remaining R04 feature-CSS migration. R05 remains ACTIVE.

## R05 — ACTIVE: command palette coverage and search correctness (2026-09-21)

Added executable stories for the production `CommandPalette` component. The
stories use deterministic assets and verify both scoped search results and the
real selection callback through the Mantine modal portal. While covering this
component, fixed a functional gap: asset search now matches id, name, catalog,
and table identifier instead of showing every authorized asset for any query.

Proof: `pnpm check` and `pnpm test:stories` pass from `apps/governance-ui`;
the browser suite reports 5 files and 13 tests passing. No networked fixture,
backend, authentication, or pickle behavior was changed. R05 remains ACTIVE;
the remaining coverage, visual review, manual accessibility, and owner approval
gates are unchanged.
