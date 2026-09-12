# Plugin platform progress ledger

Baseline reviewed: `5208eee9af35e38b5ab8294524a0485611c2b0a7`.
Implementation follow-up through `e785400`.
Review date: 2026-09-12. **Paid-production release: HOLD.**

This task began with review/planning documents and now includes incremental runtime,
UI, and plugin-contract slices. Database migrations, external plugin wheels, and
pickle serialization remain unchanged. Local probes are recorded in [the review](IMPLEMENTATION_REVIEW.md).
Earlier implementation evidence remains in [the UI ledger](../ui-v2/EXECUTION_STATUS.md).

## State meanings

- `not-started`: no implementation under the new packet has begun.
- `implementing`: code/tests exist but at least one packet criterion is incomplete.
- `implemented-unverified`: intended implementation exists; required execution
  evidence is missing. This state is not acceptance.
- `accepted`: every criterion owned by that packet passed, with evidence tied to
  the relevant commit/artifact and the covered A subcases listed. Broader A scenarios
  spanning later packets remain open until their complete evidence exists. Later
  regressions reopen the packet; packet acceptance is not product release approval.
- `blocked`: name the external dependency and remaining work; never treat it as done.

## Ordered queue

- X00 baseline and constraints: **implementing**; acceptance registry and baseline
  probes are documented, but immutable ticket fixture inventory remains open.
- X01 selected-only publication: **implemented-unverified**; initial activation now
  scopes to selected asset/catalog. Focused SQLite/API evidence passed; Flight and
  PostgreSQL evidence remain open.
- X02 immutable draft/review: **implemented-unverified**; strict review requires an
  explicit saved draft and legacy rule hashes are bound. PostgreSQL race evidence and
  full snapshot binding remain open.
- X03 publication/grant/binding transactions: **implementing**; asset-row locks now
  serialize shared-rule, draft, and restore mutations with publication. Grant,
  binding, and PostgreSQL barrier evidence remain open.
- X04 canonical evaluation: **implemented-unverified**; resolved mask values now
  flow from canonical preview and an unmatched-principal regression passes.
- X05 canonical bounded schemas: **implemented-unverified**; canonical Arrow schema
  encoding includes nested metadata/IDs and direct loader bounds. Migration and all
  entry-route/byte-budget evidence remain open.
- X06 safe schema evolution: **implementing**; typed evaluation paths now preserve
  literal dotted names. Persisted admitted field identities and evolution policy
  remain open.
- X07 configuration/secrets/IO: **implementing**; nested dynamic class-loader options
  are rejected. Typed provider configs, shared secret resolution, and IO enforcement
  remain open.
- X08 budgets and atomic reload: **implementing**; real discovery now routes through
  pre-materialization namespace/table caps and uses deque traversal. Provider page
  bounds, deadlines, cancellation, and atomic reload remain open.
- X09 UI lifecycle: **implemented-unverified**; initial-load epoch and synchronous
  logout fencing plus stale history/preview/review/publish response checks are fixed.
  Deferred browser tests and full operation-state coverage remain open.
- X10 complete authoring/management: **implemented-unverified** for the deny-all UI
  path; controls now expose save/test/review/publish actions when rules are empty.
  Full editor, activation, accessibility, and browser evidence remain open.
- X11 public SDK: **implementing**; versioned contracts are present under
  `src/dal_obscura/common/plugin_api`, but independent wheel extraction remains open.
- X12 admitted loading and Iceberg adapter: **implementing**; allowlisted entry-point
  registry tests pass, while built-in Iceberg routing and artifact-lock verification
  remain open.
- X13 plugin routing and migration: **not-started**.
- X14 plugin UI: **not-started**.
- X15 conformance kit: **not-started**.
- X16 REST Iceberg qualification: **not-started**.
- X17 independent manifest/Parquet plugin: **not-started**.
- X18 consumer qualification: **not-started**.
- X19 secure deployment and identity lifecycle: **not-started**.
- X20 recovery and upgrades: **not-started**.
- X21 performance/observability/test efficiency: **not-started**.
- X22 exact-artifact CI: **not-started**.
- X23 independent review/release decision: **not-started**.

Next implementation action: finish X00 fixture inventory, then X03 publication CAS
and X06 schema-admitted fields. Do not add new providers before Phase A's
security/correctness prerequisites are accepted.

## Evidence entry template

Copy this section for each atomic slice. Replace every placeholder; do not delete
fields to hide missing evidence.

```text
Packet/slice:
State:
Baseline and resulting commit:
Files/contracts changed:
Findings addressed (R IDs):
Acceptance cases/test node IDs (A IDs):
Failing behavior before the change:
Implementation behavior after the change:
Exact commands and exit results:
Environment/dependency and wheel/image/plugin-lock identities:
Evidence files or CI artifact links:
Pickle compatibility/unchanged-boundary check:
Migration/rollback impact:
Remaining acceptance gaps or blockers:
Next permitted packet:
```

For live evidence record hardware, timestamps, provider/IdP versions, synthetic
dataset identity, and which processes/artifacts participated. Redact secrets at
capture time. Do not store live credentials, customer rows, or executable untrusted
pickle payloads in this documentation.

## Review completion evidence

- Repository source and existing plan/ledger reviewed at the baseline above.
- Focused schema/auth/demo tests exited successfully; exact command in the review.
- Local probes reproduced unrelated initial activation, stale shared-rule review,
  nested dot-path collision, collection-ID digest collision, and admission of
  provider class-loader options. These defects remain unfixed.
- Plugin architecture, 24 implementation packets, 23 acceptance scenarios, and
  explicit release gates written. This is planning completion only.
- Documentation structure checks verified sequential R/X/A IDs, required packet
  fields, and local link targets; whitespace checks also passed before commit.
- Full suites, browser/real-IdP, PostgreSQL races, production TLS/Compose, consumers,
  recovery, performance, and independent security/UX review were not run here.

Do not move a packet to `accepted` on the strength of this review's focused tests.
