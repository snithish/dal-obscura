# Implementation plan entry point

Use [EXECUTION_HANDOFF.md](EXECUTION_HANDOFF.md) for all remaining implementation.
It was rewritten against `8e23c98` to consolidate UI, backend, gateway dependencies
and production work into one sequence. The former U00–U10 execution schedule is
superseded; do not execute both schedules or repeat completed foundation work.

Use [EXECUTION_STATUS.md](EXECUTION_STATUS.md) for actual evidence. The
[production review](PRODUCTION_READINESS.md) explains current deficiencies and
backend coverage; its earlier packet elaborations are supporting context, not a
second execution order. [EXPERIENCE.md](EXPERIENCE.md) remains the product and UX
specification. [P00_CONTRACT_REVIEW.md](P00_CONTRACT_REVIEW.md) records unresolved
decisions, not approval to introduce persistent state.

## Scope mapping for older references

- U00 scope/contracts → P00.
- U01 visual/interaction direction → P04 and P12; owner/usability acceptance stays required.
- U02 UI foundation → P04; packaging → P01/P13.
- U03 identity and permissions → P02/P03.
- U04 assets/connections → P10.1 onboarding, P05 schema, P10 remaining management.
- U05 nested policy studio → P05/P06.
- U06 evaluation and explanations → P07.
- U07 review/publication/restore → P08/P09.
- U08 activity/administration/consumers → P09/P10.
- U09 quality/security/usability → P12/P15/P16.
- U10 packaging/migration/release → P01/P11/P13/P14/P16.

Gateway W02/W03/W07 supply nested and Iceberg semantics; W04/W05 supply identity,
ticket and generation consistency; W08/W09 supply consumer/resource guarantees;
W11/W12/W13 supply packaging and independent evaluation. Preserve W06 pickle
logic. UI completion cannot stand in for unverified gateway correctness.

## Remaining milestones

1. Contracts and restart-safe installation ready for implementation/verification.
2. Real login, scoped backend permissions and reliable UI lifecycle.
3. User-created asset → nested draft → evaluation → exact review → atomic publish
   → actual governed consumer reads.
4. Complete management, history, restore and local feature/security parity.
5. Verified production deployment, recovery, upgrades, capacity, operations and
   independent candidate review; then owner release acceptance.

No milestone is complete merely because its plan exists. Record estimates only
after the contract and runtime unknowns are resolved; do not promise a delivery
date or paying-customer readiness from test counts or partial UI screenshots.
