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
R02/R03/R05/R07–R12: OPEN. No production readiness or live Cloudflare claim.

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
