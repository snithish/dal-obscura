# Governance UI implementation plan

Status: **implemented and verified locally**. No commit, push, or deployment is
authorized by this document alone.

Mockup (authoritative for visual intent): `docs/mockups/governance-ui-mockup.html`
— six tabs, self-contained, no build step. Open it before starting work.

Scope: `apps/governance-ui` only. No server changes are required for any item
in this plan. Items T1–T4 need **zero** backend work.

---

## Read this first — five facts that will mislead you

**1. A parent grant does NOT disclose the whole subtree.** This is the single
most important correction in this document, and it was got wrong in the first
draft of the mockup. `resolve_access` intersects every grant with the reader's
requested path:

- `src/dal_obscura/kernel/policy/policy_resolution.py:88` — `_matching_columns`
  returns the *requested leaf* when a parent is granted. Docstring: "A request
  for a parent is pruned to each granted descendant, preventing sibling
  disclosure."
- `src/dal_obscura/kernel/policy/authorization.py:25` — `resolve_visible_columns`
  narrows from the other side (called at `application/reads/admission.py:78`).

So `identity.*` granted + `contact.phone` masked does **not** leak 1,204 fields.
Nothing leaks to siblings. Do not build UI that warns about sibling disclosure;
the kernel already prevents it. What remains is a *completeness* question — did
the operator configure masks for everything they meant to? — and it must be
presented neutrally or in amber, never in red.

**2. Mask application is sparse.** `rule.masks` only holds entries for explicitly
masked paths. An unmasked leaf returns stored values **when a reader requests
it**. That is normal and usually intended. "No mask" is not an error state.

**3. `/policy-evaluate` evaluates the SAVED policy only.** There is no
draft-preview endpoint. Any mockup showing "what a reader receives" for an
*unsaved* draft must either simulate mask semantics client-side (T2 states this
explicitly in the UI) or add a new endpoint (out of scope).

**4. `.notice` is success-green and carries error text with `role="status"`.**
Verified: `.notice { background: var(--success-surface) }` at
`src/app/styles.css:93`, used with `role="status"` for both
`"Owners updated."` and `"Only a platform administrator can change owners."`.
Errors are painted green and are not announced by screen readers. This is T1 and
it is a correctness bug, not a taste issue.

**5. Wide-schema rendering is already solved.** `ColumnMultiSelect` is
virtualized (`ROW_HEIGHT = 48`, `VIEW_HEIGHT = 288`, manual `start`/`end`
slicing with overscan). `e2e/policy/wide-selection.spec.ts` exercises **10,000
selections** at 1440/390/320px, asserting ≤5 chips, ≤12 rendered rows, p95
search <200ms, no horizontal overflow, and zero axe violations. Do not rebuild
this. Build *on top* of it.

---

## What already gives you the primitives

Do not write new traversal logic. All of it exists:

| Need | Existing function | Location |
|---|---|---|
| Subtree leaf counts | `columnLeafPaths(options): Map<string, string[]>` | `src/features/policy/policy_editor.ts:174` |
| Leaf identification | `leafColumnOptions(options)` | `policy_editor.ts:160` |
| Grouping by mask | `groupMasks(rule, columns): MaskGroup[]` | `policy_editor.ts:111` |
| Typed ancestry | `columnOptionCovers(parent, child)` | `policy_editor.ts:56` |
| Flatten schema → options | `authoritativeColumnOptions(fields, selected)` | `policy_editor.ts:14` |
| Virtualized list pattern | `ColumnMultiSelect` | `features/policy/columns/ColumnMultiSelect.tsx` |
| Existing overflow UX | `slice(0,5)` + "Review all N" | `ColumnPathSummary.tsx:19`, `ColumnMultiSelect.tsx:260` |
| Toast/confirmation | `useConfirmation()` | `src/shared/ConfirmationProvider.tsx` |
| Message tone + placement | **does not exist — T1 creates it** | — |

`SchemaNodeResponse` (`apps/governance-ui/openapi/control-plane.json:9303`) has
exactly eight fields: `field_id`, `name`, `path`, `human_path`, `type`,
`nullable`, `kind`, `children`. **There is no `leaf_count` and adding one is not
required.** Every count in this plan comes from `columnLeafPaths` client-side.

---

## Tasks, in dependency order

### T1 — Message severity as a type, not a colour class

**Why first.** It is a live correctness bug and it touches four files that later
tasks also edit. Doing it first avoids conflicting edits.

Create `src/shared/Message.tsx`:

```tsx
type Tone = "success" | "error" | "info";
// success → .msg-success  + role="status"
// error   → .msg-error    + role="alert"
// info    → .msg-info     + role="status"
export function Message({ tone, children }: { tone: Tone; children: ReactNode })
```

Reuse existing tokens only: `--success-surface`/`--success`,
`--danger-surface`/`--danger`, `--selection`/`--selection-text`. Do not
introduce new colours.

Migrate these four call sites, and **place each message inside the card that owns
the action** rather than at the bottom of the view:

| File | Line | Current | Problem |
|---|---|---|---|
| `features/connections/ConnectionsView.tsx` | 916 | `<p className="notice">` | no `role` at all; carries both success and failure |
| `features/settings/SettingsView.tsx` | 530 | `role="status"` | identity-provider errors shown as success |
| `features/assets/AssetWorkspace.tsx` | 223 | `role="status"` | policy notice; usually fine, verify tone |
| `features/assets/AssetWorkspace.tsx` | 1228 | `role="status"` | "Only a platform administrator…" rendered under *Delegated capabilities* |

Callers hold a single `message: string` state. You must split it into
`{ tone, text }` at each call site or you cannot know the severity. This is the
main mechanical cost of the task — budget for it.

**Acceptance:** no `.notice` usage remains for error text. Every error is
`role="alert"`. `e2e/shell/design-system.spec.ts` axe assertions still pass.

---

### T2 — Collapsed rule summary, three states

Mockup: **Tab 2**, section "Collapsed summary at scale".

Current: `AssetWorkspace.tsx` renders `{rule.columns.length} columns` plus
`{Object.keys(rule.masks).length} masks` — arithmetically correct,
informationally empty at scale.

Three tiers, based on distinct column count:

| Tier | Condition | Render |
|---|---|---|
| Full list | ≤ 6 | one `sem-chip` per column with its mask badge |
| Clamped | 7–20 | chip row clamped to `max-height: 52px`, fade overlay, `Review all N columns` button |
| Aggregated | > 20 | mask-state tallies, **ordered by exposure** |

Aggregation tier renders one chip per `groupMasks` group:

```
1,204 no mask   88 keep_last   312 hash   46 redact   1,197 null
```

**Ordering is load-bearing.** `no mask` must come first — it is the only state
that returns stored values. Sort by exposure, never alphabetically.

**Tones:** `no mask` → danger; `keep_last` → warning; `hash`/`null`/`redact` →
neutral. This is the one place red is justified, because it is a state tally,
not a per-column alarm. See gotcha G3.

The clamp must be **visual only**: the full chip list stays in the DOM so
assistive technology is not truncated, and the button opens the existing
`ColumnPathSummary` review.

**Acceptance:** header height stays bounded at 50+ columns. Existing
`wide-selection.spec.ts` still passes unchanged.

---

### T3 — Value lens for wide/deep schemas

Mockup: **Tab 5**.

Point at a subtree, resolve its rows on demand. **Parent nodes render aggregate
mask state (a swatch); only leaves render values.** This is why a 7-level path
never mounts 7 rows of raw data.

Deep paths use the canonical syntax your `FieldPath` already speaks —
`.`, `$element`, `$value` — e.g.
`identity.contact.address.history[].previous.line1`. Never split a display
string on dots; `pathSegments()` (`policy_editor.ts:41`) exists for this.

**Simulated, not authoritative.** The preview renders mask semantics computed
client-side from the draft, and the UI must say so — mirroring the existing
"Unsaved edits are not included." copy in `AssetWorkspace.tsx`. Do not label
draft output as a server evaluation.

Reuse `ColumnMultiSelect`'s virtualization constants. Do not render more than
~12 rows at once.

**Acceptance:** a 10,000-field asset opens without mounting the full inventory.
Copy explicitly states simulation.

---

### T4 — Coverage summary on the inventory and rule

Mockup: **Tab 1** summary strip, **Tab 5** mask-coverage card.

Inventory strip (4 tiles): Governed · No policy · Changed today · Live tokens.

Also in Tab 1, two corrections to current behaviour:

- **Owners inline.** `AssetInventory.tsx:77` truncates via
  `asset.owners[0].split("|").at(-1)` behind a popover. For a tool whose purpose
  is knowing *who can see what*, render the full `issuer|subject` inline.
- **Revision → who/when/what.** `AssetInventory.tsx` column `Revision` renders a
  bare number that nothing explains, while `AssetWorkspace.tsx:214` says "Live
  revision N". Make it `rev 14 · sfischer · 2h ago · 3 tokens revoked`.

Rule-level coverage replaces raw counts with mask-state tallies (same rendering
as T2 tier 3).

**Note:** "Live tokens" requires a server-side count. If no endpoint provides it,
**drop that tile** rather than inventing a client-side estimate. Do not fake a
number.

---

### T5 — Data-loss and unrecoverable-action guards

Four independent defects. Each is a small local change.

**5a — Sign out destroys unsaved edits.** `app/Workspace.tsx:328`:
`onLogout={() => void logout()}`. `SessionBoundary.logout()` → `invalidate()`
clears the query cache and remounts children via `key={scope.generation}`. The
`beforeunload` guard does **not** cover this path. Fix: route through the
existing `confirmDiscardUnsaved()`.

**5b — Row filter dropped silently.**
`features/policy/editor/RowFilterEditor.tsx:153`: clicking "All rows" commits
`onChange(null)` immediately, widening the rule's audience to every row with no
confirm and no undo. "Discard row changes" only reverts *unapplied* edits, so a
committed filter is unrecoverable. Fix: `confirm()` before discarding a
non-empty filter. Use `discardChanges("row filter", "…")`.

**5c — Save blocked, cause hidden.** Collapsing a rule leaves `pending` set, so
Save is disabled while the only explanation lives inside the collapsed card.
Fix: show the pending count on the collapsed rule header and focus it on click.

**5d — Duplicate test entry points.** `AssetWorkspace.tsx` has both "Test
Policy" and "Test saved policy" opening the same modal; only the modal body
reveals it evaluates the saved revision. Fix: one control, one label.

**Acceptance:** each has an e2e test asserting the guard fires. See gotcha G6.

---

### T6 — Column picker keyboard trap

`features/policy/columns/ColumnMultiSelect.tsx:418` sets `tabIndex={0}` on
every option row, while `:370` drives `aria-activedescendant` from the input.
These are contradictory patterns — Tab walks all 10,000 rows at the exact scale
`wide-selection.spec.ts` already tests.

Fix: roving tabindex. Rows get `tabIndex={-1}`; the active row is the only
tabbable one, and it receives focus on arrow-key navigation.

Also: "Select all except…" exclusions are silently discarded on Escape. Either
restore them or say so.

**Acceptance:** extend `wide-selection.spec.ts` with a Tab-through assertion.

---

### T7 — Form validation and misc accessibility

- **T7a** `features/settings/SettingsView.tsx` calls `setError(...)` on every
  keystroke — `"Issuer URL is required."` is raised at `:298` from
  `setError` (`:244`), and the message surfaces through `role="alert"`. Clearing
  a field to retype therefore fires an error immediately. Validate on blur and
  on submit instead.
- **T7b** `src/features/identity/LoginPanel.tsx:69` uses
  `autoComplete="current-password"` for the control-plane token. Change to
  `autoComplete="off"` — this field can mint an admin session.
- **T7c** `app/Workspace.tsx:136-139` focuses the hidden mobile-nav trigger on
  first paint, so keyboard focus never starts in main content. Skip on mount.
- **T7d** `useActivityQueries.ts` sets `loading` from `isFetching`, but
  `QueryView` ignores it once data exists — applying filters shows stale rows
  with no busy signal. Add `aria-busy` + "Updating activity…".

---

## Deliberately out of scope

- **Adding `leaf_count` to `SchemaNodeResponse`.** Not needed. If audit tooling
  later needs subtree sizes headlessly, add it then — `_arrow_field_node` can
  count bottom-up during its existing recursion at negligible cost against
  `MAX_SCHEMA_RESPONSE_BYTES`.
- **A draft-evaluation endpoint.** T3 simulates client-side and labels it as
  such. Adding `/policy-evaluate-draft` is a larger change needing its own
  plan.
- **Mobile inventory card layout** (mockup Tab 3). Separate task; the current
  760px table in a 390px viewport is genuinely broken, but it touches the
  inventory table and its e2e coverage independently of T1–T7.
- **P3 items** from the UX sweep: step numbers skipping 1–2, `key={index}` on
  repeatable rows, truncated resource IDs in Activity, missing `dateTime` on
  `<time>`.
- **Code style.** Biome formatting at width 100 already runs through
  `pnpm check`. Formatter migration was completed separately.

---

## Gotchas

**G1 — `groupMasks` includes an `<unmasked>` bucket.** `policy_editor.ts:111`
returns a group with `mask === undefined` for unmasked columns. Your aggregate
chip for "no mask" must read that group explicitly; don't filter on truthiness
of `group.mask` and silently drop the largest group. It will usually be the
biggest one.

**G2 — `maskKey` includes `exempt_principals`.** `policy_editor.ts:82`. Two
rules that differ only by exemption are *different* groups. If the aggregate
shows fewer tallies than expected, this is why — and it's correct behaviour.

**G3 — Do not reintroduce red for "no mask" in T2/T3.** The first mockup draft
did exactly this and contradicted its own caption. Red is for the *tally*
(a state summary in the aggregated header). In the value lens, unmasked values
render as ordinary stored data in neutral type with the column headed "Returned
as stored — no mask in this rule". An operator reading that must not feel
alarmed by a value they deliberately chose not to mask.

**G4 — StrictMode is already enabled.** The earlier review work enabled it in
`src/app/main.tsx`. The inventory hook also commits retained results in an effect.

**G5 — Workspace recovery is already implemented.**
`features/identity/WorkspaceErrorBoundary.tsx` fences private work through the
session owner and offers reconnect and reload actions.

**G6 — Main asset and schema fixtures use generated types.**
The earlier review work typed `api-types.ts`, `asset-api.ts` and `ux-fixture.ts`.
Keep new fixtures typed against the generated schema as well.

**G7 — Bundle budget has documented headroom.**
The aggregate JavaScript limit is 220 KiB. Its rationale lives in
`docs/governance-ui.md`. Keep new features within that cap.

**G8 — `requestAnimationFrame` focus in `AssetWorkspace.tsx:128-131`.** The
add-rule focus/scroll uses `requestAnimationFrame` after a
`document.querySelector` on `#rule-body-{ordinal} input`. T2 changes ordinal
rendering in the collapsed header. If you change ordinals or the key scheme,
re-verify add-rule focus lands on the right input.

---

## Verification

Run from the repository root. From `docs/testing.md`.

```sh
pnpm --dir apps/governance-ui install --frozen-lockfile
pnpm --dir apps/governance-ui test          # node:test, pure logic
pnpm --dir apps/governance-ui check         # Biome + tsc -b + tsc -p tsconfig.tests.json
pnpm --dir apps/governance-ui build         # tsc + vite + check-budgets
```

Story and browser lanes (need their own setup):

```sh
pnpm --dir apps/governance-ui test:stories
pnpm --dir apps/governance-ui test:e2e
```

`test:e2e` builds and serves via Playwright's `webServer`. To run against an
already-running instance set `DAL_OBSCURA_E2E_BASE_URL`.

**Required for any change touching these flows:**

- `e2e/policy/wide-selection.spec.ts` — must pass unmodified for T2/T3.
- `e2e/shell/design-system.spec.ts` — axe violations, contrast ≥4.5:1 on both
  schemes, focus restoration, CSP nonce. Must pass for T1 and any new colour use.
- New e2e coverage for each T5 guard (assert the confirmation appears and that
  cancelling preserves state).

Warnings fail Python qualification but the UI lanes have no equivalent strict
mode — assert on `role` explicitly rather than assuming a live region announced.

---

## Conventions to follow

- Tests live beside what they own. `src/**/*.test.mjs` uses `node:test` +
  `node:assert/strict` with `--experimental-strip-types`; pure logic only, no
  React rendering.
- `src/data/import_boundaries.test.mjs` enforces `data/` may not import
  `app|features`; `features/*` may not import `app/main` or `app/Workspace`;
  `connections|settings` may not import `features/activity`. New shared code
  goes in `src/shared/`. Features may import `app/navigation/routes` for types.
- Generated OpenAPI types are authoritative. Never hand-edit
  `src/generated/control_plane.d.ts`; `pnpm check:api-types` byte-compares it.
- Reuse `--success-surface` / `--warning-surface` / `--danger-surface` /
  `--selection` from `src/design/theme.ts`. New colours must pass the existing
  contrast assertions on both light and dark schemes.
- Focus must return to the invoking control after every dialog closes. The
  existing suite tests this — match it.

## Implementation notes

T1 through T7 are implemented in the UI. No server or generated API changes were
needed. The authoritative mockup remains unchanged.

- T1 uses typed action feedback with success, error and information tones.
  Owner, grant, provider, runtime, discovery and token feedback appears with its
  owning action. Errors use alerts.
- T2 keeps exact selected paths visible at small sizes, clamps medium lists
  visually, and orders grouped tallies by exposure.
- T3 adds a virtualized, on-demand value lens with synthetic text samples. It
  illustrates draft masks and inherited coverage, preserves NULL examples, and
  labels conflicting masks. It does not evaluate audiences, row filters,
  exemptions, other rules or Arrow type conversions. Only the saved-policy test
  calls the server evaluator.
- T4 reads governed and missing-policy totals from the existing summary API.
  Changed-today counts distinct assets in authorized audit results. A paginated
  result shows a lower bound with a plus sign. No live-token count is available,
  so that tile is omitted. Full owner identities are inline. Recent recorded
  changes appear inline from the shared activity cache; older policy history
  loads on demand to avoid one eager request for every inventory row.
- T5 confirms dirty sign-out and row-filter removal, exposes pending rule edits
  beside Save with a focus action, and keeps one saved-policy test entry point.
- T6 gives only the active picker option a tab stop, moves focus with arrow keys
  and retains unapplied exclusions when the picker closes.
- T7 validates provider text on blur and submit, disables token autofill,
  avoids initial focus on hidden navigation, and announces activity refresh
  while marking retained content busy.

Visual evidence lives in `docs/ui-review/governance-ux-plan/`. Reproduce it with
`review/governance-ux-plan.spec.ts` and `DAL_OBSCURA_REVIEW_PHASE=before|after`
against separate baseline and candidate preview URLs.


## Verification evidence, 2026-10-16

Passed 79 unit tests, 72 component stories and 132 browser journeys. Biome, both
TypeScript checks, API type drift, production build and Storybook build passed.
The final browser run includes the existing 10,000-field selection, keyboard,
responsiveness, page-width, contrast and axe checks. Value-lens checks cover
10,000 fields with at most twelve mounted rows and 320px/390px layouts in both
light and dark themes.

Production assets measure 203.24 KiB JavaScript gzip against the existing 220 KiB
limit, 44.06 KiB CSS gzip and 83.72 KiB fonts. No performance improvement is
claimed. The regression tests fail when exposure ordering or the row-filter
confirmation is removed, and pass with those protections restored.

Before/after screenshots were rendered from separate baseline and candidate
builds. The [visual evidence](../ui-review/governance-ux-plan/) includes inventory,
policy, error feedback and desktop/mobile value-lens captures. No commit, push
or deployment was performed.
