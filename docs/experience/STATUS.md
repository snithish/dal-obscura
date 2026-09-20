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

R04: ACTIVE investigation of Mantine production-CSP slice.
R02/R03/R05/R07–R12: OPEN. No production readiness or live Cloudflare claim.

### Dependency access blocker (2026-09-21)

Official registry metadata confirms Mantine core/hooks 9.6.1 and React ^19.2.0
peer requirements. Network installation could not complete: both system pnpm
12.4.2 and bundled pnpm 11 requests failed; a direct Node 24 HTTPS request timed
out, and the exact cached pnpm 12.3.4 install also stalled. curl reaches the same
official registry. Failed/stalled task-owned processes were stopped. No Mantine
dependency or unverified lockfile changes are committed. No TLS/CSP/security
checks were bypassed. Resume with registry access restored for the pinned package
manager, then perform R04's CSP/bundle slice before broad UI migration.

R04/R05 remain open; the existing production UI is still in place. No inference
about Mantine CSP compatibility is possible from inspecting package source alone.
