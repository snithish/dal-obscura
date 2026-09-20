# Execution ledger

Implementation authorized 2026-09-20. Plan: [R01–R12](ACTION_PLAN.md).
No packet is DONE until all of its acceptance criteria pass.

## R06 — ACTIVE: exact identity cache isolation

Replaced delimiter-concatenated issuer/subject cache scope with an unambiguous
serialized tuple. The existing QueryClient test now exercises a collision pair
against actual cached inventory: failed before the fix, passes afterward.
This closes the delimiter collision only; session-generation and rendered
interleaving coverage remain open. No authentication or pickle changes.

Validation: Node 26.8.2, existing UI unit suite and TypeScript check. This local
runtime differs from the declared Node 24 release target; release qualification
still requires that target. Commit identity is available from this file's history.

## Remaining packets

R01/R04: ACTIVE investigation of supported pins and Mantine production-CSP slice.
R02/R03/R05/R07–R12: OPEN. No production readiness or live Cloudflare claim.
