# Mask-edit coverage performance

The policy editor now builds exact typed-path coverage once per edit. Applying a
mask checks selected ancestors; removing a grant checks both ancestor and
descendant overlap, preserving masks covered by another grant and unrelated
orphan masks. Literal dotted names, collection segments, and field IDs remain
part of identity. No server policy or emitted-value semantics change.

The former implementation rebuilt an options map for each grant/target comparison.
The current implementation uses sets of selected paths and their strict ancestors.
It also traverses schema options with an explicit stack and retains option arrays
when unrelated editor state changes. Schema transfer and path-prefix work remain
bounded by existing contracts, not made free or unlimited by this change.

## Reproduce

Use the project's pinned Node/pnpm runtime. Save the old module before changing it:

```bash
git show 12428e37:apps/governance-ui/src/policy_editor.ts > /tmp/policy-editor-before.ts
pnpm --dir apps/governance-ui exec node --experimental-strip-types scripts/mask-edit-performance.mjs /tmp/policy-editor-before.ts
pnpm --dir apps/governance-ui exec node --experimental-strip-types scripts/mask-edit-performance.mjs
```

The probe uses 300 independent parent grants and 300 nested leaf masks. It measures
three sequential runs per operation, then independently checks exact mask values
and selected columns outside the measured interval. Run the baseline and current
module consecutively on the same otherwise idle host. These numbers exclude
React/browser work, IO, and native allocations; they are not an end-to-end SLA.

## Measurement

On this macOS arm64 host, Node v24.19.0, after browser checks completed:

- Apply: median 560.082 ms before, 0.278 ms after.
- Remove: median 1,145.828 ms before, 0.497 ms after.

These small microsecond-scale current measurements vary with JIT and host load.
The relevant improvement is removal of repeated inventory construction and
cross-grant comparisons, not a precise speedup guarantee.

Owning unit tests preserve parent/child coverage, literal names, collection
segments, historical IDs, exemptions, original rule immutability, and retired
fields. A synthetic 20,000-level option chain confirms JavaScript traversal avoids
call-stack recursion; it does not establish native Arrow/IPC support at that depth.
All 72 UI unit tests, 58 browser stories, 101 E2E cases, TypeScript checks, and
production bundle budgets pass. The current JS gzip total is 199.660 KiB under
the unchanged 200 KiB budget.

## Replacement typing

A separate rendered 5,000-field fixture edits an existing hash mask, types
`private-data`, then applies redaction to all exact targets without saving the
policy. Form-local keystrokes now reuse mask groups, available-field maps, and
membership sets. No backend write or visual layout change is involved.

With one browser worker and three repetitions on the same host, before/current
runs executed consecutively: p95 typing latency was 137.900–147.415 ms before
and 28.699–29.085 ms after. Each run has twelve keystrokes, so its p95 is the
largest sample; this small local regression fixture is not field INP evidence.
The existing 200 ms gate is unchanged. JSON reporter attachments retain individual
samples:

```bash
pnpm --dir apps/governance-ui exec playwright test e2e/mask-performance.spec.ts --workers=1 --repeat-each=3 --reporter=json > /tmp/mask-typing.json
```

The form still renders complete mask-summary path lists; this improvement removes
repeated computation rather than claiming bounded summary DOM. Production JS gzip
is 199.710 KiB under the unchanged 200 KiB budget.
