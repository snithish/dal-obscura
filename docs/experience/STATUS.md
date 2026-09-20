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

## Remaining packets

R01/R04: ACTIVE investigation of supported pins and Mantine production-CSP slice.
R02/R03/R05/R07–R12: OPEN. No production readiness or live Cloudflare claim.
