# Rule editor redesign: implementation handoff

Status: original handoff plan. The implemented UX now uses mask cards, contextual exception controls, independent pending-edit guards, and a local rule summary. See docs/policy-authoring.md for the current workflow. Policy browser acceptance tests now run successfully.

## 1. Outcome and scope

Replace the field-at-a-time workflow with a single editor where an owner can select multiple columns in a searchable dropdown, apply one mask to the selection, and then optionally add exemptions. Keep the complete policy save atomic and revision checked.

Confirmed scope: exemptions include both columns that skip the mask and people/groups that skip it. Also simplify row-filter configuration and explain how filters combine with masks and exemptions. All tasks below are in scope.

Use the steps and acceptance criteria below as implementation requirements. Do not invent a new policy language, redesign unrelated tabs, add a persisted draft workflow, or change ticket expiry/revocation behavior.

## 2. Findings in the current implementation

- `apps/governance-ui/src/components/AssetWorkspace.tsx`: the main rule form combines a separate schema pane, one selected field, an include checkbox, and a mask editor. Users must move between panels to repeat the same operation.
- `apps/governance-ui/src/main.tsx`: `toggleField()` changes one column; `setMask()` changes only `selectedField`. `replaceRules()` and `updateRule()` already own undo/redo and dirty-state behavior. Reuse those boundaries.
- `effectiveFields` currently unions fields across every rule. Do not use that union as the selected rule's column selection or as a persona's effective access.
- `src/dal_obscura/common/access_control/policy_resolution.py`: matching rules union columns, AND row filters, and combine compatible masks. Rule order is not an override mechanism. An additional unmasked grant does not cancel an existing mask.
- Current mask models contain only `type` and `value`; there is no people/group exemption contract. This requires backend work, not just a new UI field.
- Policy tests evaluate the saved live policy. They do not evaluate unsaved editor changes.
- `MaskEditor` currently receives no read-only prop. The new controls must all honor editing permissions.

## 3. Exact UX

Use one continuous form, not a wizard, drawer stack, or nested tabs. Keep a compact rule selector above it. Display each rule with a summary such as “3 columns · Hash · 2 groups”; use “Mixed masks” when needed. Put duplicate/delete/order controls in a secondary actions menu. Do not describe order as precedence.

### A. Columns

1. Label: **Columns this rule allows**.
2. Searchable multi-select dropdown populated from the authoritative schema. Each option shows its full human path and type; values preserve the server's exact path strings.
3. Keep dropdown open after selecting an option. Show checkmarks, selected count, removable selections, and a clear empty state.
4. Support keyboard search, arrows, Enter/Space selection, Escape close, and focus return to the trigger. Search must not discard selections.
5. For wide schemas, use a bounded, virtualized option list. “Select search results” selects only eligible filtered options; show the count before applying. Never silently insert `*`.
6. Preserve parent and collection path support. Use typed schema path segments to detect ancestry; never split paths on dots. Prevent new parent/descendant duplicate selections with an inline explanation. Keep existing unsupported/stale selections visible as removable invalid chips instead of silently dropping them.
7. Keep schema browsing available under **Browse schema** as secondary help. It must not be necessary for ordinary multi-column editing.

### B. Mask

1. Label: **Mask selected columns**. The mask target selector defaults to all columns allowed by the selected rule, but can select a subset without removing access to other columns.
2. Show supported mask types with readable names. Keep **No mask** distinct from the `null` mask; `null` returns typed NULL values.
3. Show a value input only where the chosen mask needs one. Preserve existing scalar parsing and backend validation. Keep partially typed/invalid input local until Apply; never silently replace invalid input with zero.
4. If target columns already have different configurations, show **Mixed masks**. Merely opening the editor or changing the target selection must not overwrite them.
5. Button: **Apply to N columns**. One click makes one local policy edit and one undo entry. Disable it for no targets, invalid values, missing schema, or read-only access.
6. Applying a mask copies its configuration to every targeted field. Removing a mask deletes only those mask entries; it preserves column access, other masks, principals, conditions, and row filters.
7. Show a compact summary grouped by equal mask configuration, including exemption configuration. Each group has an **Edit** action that selects its columns and loads its values. Grouping is display-only; do not persist UI group IDs.
8. Newly allowed columns remain unmasked until explicitly included in an Apply action. Selecting a column must never silently grant it a previously chosen mask/exemption configuration.
9. Removing an allowed field also removes mask entries covered exclusively by that field. Use typed path coverage; preserve masks still covered by another allowed selection. One undo restores both access and masks.

### C. Optional exemptions

Provide two separately labeled sections: **Columns without this mask** and **People/groups exempt from this mask**. Do not combine them into one ambiguous “exclusions” input.

Column exemptions:

1. After choosing a mask, offer **Exclude columns from this mask**, with a multi-select restricted to the current bulk target selection.
2. Apply writes the selected mask to target columns minus excluded columns, and removes any existing mask on the explicitly excluded target columns. Show this consequence in the local summary before Apply: “Hash 2 columns; leave 1 column unmasked in this rule.”
3. Excluded columns remain allowed. Excluding a column never changes audience, row restrictions, or another rule's masks.
4. This is an authoring convenience over the existing per-column mask map; do not persist a separate excluded-column list or a default-mask inheritance system. On reload, display such columns under **Unmasked in this rule**. Do not invent historical intent about why they are unmasked.
5. Keep a different existing mask on a column by leaving that column outside the bulk target selection, not by marking it exempt. Label this distinction in the form.
6. If all targets are excluded, show “This will remove masks from N columns” and require explicit Apply. Clear people/group exemption input when No mask is chosen; never persist exemptions without a mask.

People/group exemptions:

1. Place **Add mask exemption** below the selected mask and its value input. Hide it for No mask; enable it only after a real mask is chosen.
2. Expand inline into a token input labeled **People or groups that skip this mask**. Use the existing principal token syntax: a principal ID as stored today, or `group:<name>`. Do not assume an identity-directory search API exists.
3. Allow multiple entries; trim and deduplicate. Reject blank entries and malformed group tokens. Do not add a wildcard or “everyone” exemption.
4. Helper text: “Only skips this mask in this rule. Access and row restrictions still apply. Other rules may still mask these columns.”
5. Exemptions apply to the same target columns as the mask when **Apply to N columns** is clicked. Removing an exemption restores masking for that token in this rule.
6. Treat mask plus exemptions as one editable configuration. If selected columns have different exemption lists, show Mixed and require an explicit Apply to replace them.

### D. Audience and advanced restrictions

Keep **Who can access these columns** visible below masking, using a multi-token input instead of a comma-delimited text box. Audience must be explicit; do not default a new rule to everyone.

Keep **Rows this rule allows** visible as its own section with **All rows** / **Filter rows** options. “All rows” means this rule adds no restriction; another matching rule can still restrict rows. Use the configuration requirements in section 7.

Put identity claim conditions and their JSON editor under **Advanced audience conditions**. Start expanded when existing conditions are present or an error points there. Explain that audience conditions inspect identity claims, while row filters inspect table data. Retain lossless support for existing conditions.

### E. Review, save, and feedback

- Show selected columns, audience, mask groups, and exemption counts in a readable summary above the existing save action.
- Preserve Save policy, revision conflict handling, undo/redo, deny-all empty policy, and owner token revocation.
- “Apply to N columns” changes local editor state only. “Save policy” changes the live policy.
- If a mask form has unapplied changes, show Apply/Discard and block Save and rule/asset switching until resolved. Integrate this dirty state with existing unsaved-navigation protection. Avoid silently losing form input.
- Keep **Test saved policy** explicitly labeled. When unsaved edits exist, explain that testing uses the saved version. Never imply that Save is required merely to inspect a local summary.
- Map validation errors to their field/section, expand hidden error sections, and focus the first invalid control after a failed save.
- On narrow screens, stack these sections in the same order. The primary workflow must not depend on the old Fields/Rules panel switch.

## 4. Exemption semantics and contract

Add an optional `exempt_principals: string[]` to each persisted mask configuration, defaulting to an empty list when absent. Example:

```json
{
  "type": "hash",
  "exempt_principals": ["group:privacy-reviewers", "user:alice"]
}
```

The example principal ID `user:alice` is literal; do not automatically add a `user:` prefix to supplied IDs.

Persist this within existing mask JSON. No new table, server session, persisted UI grouping, or service state is required. Preserve strict validation of unknown mask keys, mask values, and field paths. Confirm all serialization paths preserve the optional property.

Resolution algorithm:

1. Match the rule's audience and claim conditions exactly as today.
2. Resolve allowed columns and row filters exactly as today.
3. For each mask in that matching rule, compare `exempt_principals` with authenticated `Principal.tokens()`.
4. If any token matches, skip that rule's contribution of this mask. Do not skip the rule's grant or row filter.
5. Otherwise, reduce the mask to its effective `type` and `value`, then run the existing mask-combination logic.
6. Combine all non-exempt contributions as today. An exemption never removes another rule's mask.

Do not include exemption metadata in effective transform masks or Flight tickets. It is policy-authoring metadata, resolved before ticket creation. Ensure equality/conflict comparisons operate on effective masks: two equal masks with different exemption lists must not become conflicting merely because their metadata differs.

Legacy records with no exemption field must behave identically. Include nonempty exemption configuration in policy version computation, with deterministic token ordering. Preserve legacy version behavior for empty/absent exemptions where the existing hashing contract permits it. Verify both `authorize()` and `current_policy_version()` paths and retain the existing ticket lifetime contract.

Deploy backend support before enabling exemption writes in the UI. An older backend must reject an unsupported property rather than silently discard it. Do not weaken existing overlap validation to make exemptions pass; ensure it still checks possible non-exempt overlaps.

## 5. Implementation tasks, in order

For each task: first add a failing behavioral test, make the smallest implementation change, then run its focused tests. Do not skip backend work while exposing a functional-looking exemption control.

### Task 1 — Capture baseline and add pure bulk-edit helpers

New files: `apps/governance-ui/src/policy_editor.ts` and `apps/governance-ui/tests/policy_editor.test.mjs`.

Implement pure immutable helpers for authoritative column options, typed path coverage, mask grouping, mixed-state detection, applying a mask to explicit targets with column exemptions, and removing column selections with their orphaned masks. Preserve unedited rule properties. Do not use `selectedField` as implicit bulk input.

Tests: two-column apply; subset apply; column exemption deletes only the selected mask and preserves access; all columns exempt; No mask; `null`; mixed state; unrelated-field preservation; typed nested paths and literal dotted names; missing schema; removed schema fields; no mutation of input objects.

### Task 2 — Build focused UI components

Create `ColumnMultiSelect.tsx`, `BulkMaskEditor.tsx`, `RowFilterEditor.tsx`, and `PolicyRuleEditor.tsx` under `apps/governance-ui/src/components/`. Reuse the installed Mantine components and theme. Keep changes scoped; do not reformat the entire existing workspace component. Build the row-filter helper and tests described in section 7 during tasks 1–3.

Start with Stories covering multi-selection, mixed masks, wide schemas, invalid values, missing schema, stale selections, read-only mode, and narrow layout. Use interaction tests, not only snapshots.

### Task 3 — Integrate the simpler editor

Edit `AssetWorkspace.tsx`, `main.tsx`, and the owning styles in `styles.css`. Replace the primary single-field checkbox/mask workflow. Keep schema inspection secondary. Route bulk Apply through one `updateRule()` call so history and preview invalidation stay consistent. Reset local mask-form selection when the rule or asset changes after dirty-state handling.

Update `AssetWorkspace.stories.tsx` and affected end-to-end selectors. Check that rule audience/conditions/row filters survive edits and save/reload. Remove misleading “persist precedence” copy from reorder feedback.

### Task 4 — Implement exemption behavior in the domain

Edit `src/dal_obscura/common/access_control/models.py`, `compiled_policy.py`, and `policy_resolution.py`. Add a default-empty immutable exemption representation to authoring masks and implement the resolution algorithm above.

Extend `tests/domain/access_control/test_policy_resolution.py`. Cover an ordinary reader, an exempt reader, a non-reader named in exemptions, overlapping group membership, a second non-exempt matching rule, condition mismatch, retained row filters, rule reordering, and mask compatibility after removing metadata.

### Task 5 — Carry exemptions through storage, API, and live reads

Audit and update these exact paths:

- `control_plane/application/policy_compiler.py`: `_compile_mask_rule()` and strict validation.
- `control_plane/application/policy_service.py`: compiled response construction and preview.
- `control_plane/infrastructure/repositories.py`: mask JSON save/read round trip.
- `control_plane/interfaces/routes/schemas.py`: applicable API schemas.
- `data_plane/infrastructure/adapters/live_config.py`: policy loading, `authorize()`, and `current_policy_version()`.
- `apps/governance-ui/src/api.ts`: `Mask` type and `normalizePolicyRule()`; currently normalization reconstructs masks and would otherwise drop new metadata.
- OpenAPI snapshot and generated TypeScript declarations: regenerate with the repository's existing generation workflow; do not manually patch generated files.

Paths above are relative to `src/dal_obscura/` unless prefixed with `apps/`.

Add API save/reload and preview cases under `tests/interfaces/control_plane/` and `tests/control_plane/test_evaluation_service.py`. Add a real planning/fetch regression using existing Flight fixtures. Test backward-compatible legacy policy loading and version sensitivity to exemption edits. Do not change ticket schema unless an actual failing contract test proves it necessary.

### Task 6 — Enable exemption controls and complete interaction tests

Wire the inline exemption section into `BulkMaskEditor`. Group mask summaries by both mask value and exemption set. Preserve distinct configurations when loading old or heterogeneous rules. Do not automatically merge or split persisted rules.

Add a dedicated `apps/governance-ui/e2e/rule-editor.spec.ts` using existing authenticated fixtures. Verify the acceptance scenario below, keyboard usage, reload, conflicts, undo/redo, read-only permissions, and mobile layout.

### Task 7 — Document and verify

Update `docs/policy-authoring.md` and the relevant UI section of `README.md`. Document bulk editing, Apply versus Save, rule-local exemptions, cross-rule masks, and saved-policy testing. Retain the existing ticket revocation explanation.

Run frontend checks:

```sh
pnpm --dir apps/governance-ui test
pnpm --dir apps/governance-ui check
pnpm --dir apps/governance-ui test:stories
pnpm --dir apps/governance-ui test:e2e
pnpm --dir apps/governance-ui build
pnpm --dir apps/governance-ui check:api-types
```

When exemption backend changes are included, run:

```sh
uv run pytest tests/domain/access_control/test_policy_resolution.py tests/control_plane/test_evaluation_service.py tests/interfaces/control_plane -q
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run ty check
```

Compare relevant planner/mask/filter benchmarks with `.benchmarks/` baselines because exemption resolution changes policy planning. Refresh the code index with `COCOINDEX_DISABLE_USAGE_TRACKING=1 ccc index` if available.

Baseline caveat: `apps/governance-ui/tests/policy_diff.test.mjs` currently imports `../src/policy_diff.ts`, which was absent from the inspected source inventory. Capture actual baseline failures before implementing; report unrelated failures separately rather than hiding them or broadening the redesign.

## 6. End-to-end acceptance scenario

1. Open an asset containing email, phone, and country. Create a rule for `group:analysts` that allows all three.
2. In the mask target dropdown, select all three columns. Choose Hash. Exclude country using **Columns without this mask**. Add `group:privacy-reviewers` as a people/group exemption. Apply once. Configure the row filter `country = 'US'`.
3. Summary shows two hashed columns with one exemption group; country remains unmasked. One Undo reverses the entire Apply and Redo restores it.
4. Save and reload. All column selections, masks, and exemptions survive unchanged.
5. An analyst without the exemption group gets hashed email/phone and original country, with only US rows returned.
6. A reader in both groups gets original values under this rule, while retaining its row restrictions.
7. A person only in the exemption group receives no access from this rule.
8. Add a second matching rule that hashes email without exemptions. The reader from step 6 still receives hashed email and original phone. Reordering the rules does not change that result.
9. Removing the exemption restores both masks. Removing only phone's mask preserves access to phone and leaves email's mask unchanged.
10. Repeat the main editing flow with keyboard only and at a narrow viewport. No separate schema-pane navigation is required.

Completion means all these behaviors pass and all existing policy semantics remain intact. A dropdown alone is not completion if exemptions are included in scope.

### Implementation and verification record

Implemented the bulk rule editor, row-filter builder, rule-local mask exemptions, backend resolution and persistence, API schemas and generated types, authoring documentation, and focused unit/integration coverage. The dedicated browser suite covers save/reload, keyboard selection, one-action undo/redo, read-only access, narrow viewport, and revision conflicts.

Verification completed in this workspace:

- Focused policy resolution, control-plane, fetch, and DuckDB tests passed.
- Focused policy editor, row-filter, and API normalization tests passed.
- TypeScript build check, generated API type check, production Vite build, Ruff checks, formatting check, and `ty check` passed.
- Full Python suite was attempted. Tests requiring local HTTP or Arrow Flight listeners failed because sandbox socket binds return `EPERM`; focused suites above pass.
- Storybook interaction tests and Playwright browser tests could not start because their local test servers fail to bind (`EPERM`). Their source and build-time type checks pass.
- The existing full Node test suite retains baseline failures in lifecycle deep links and missing `src/policy_diff.ts`; all new focused UI tests pass.
- CocoIndex Code search/indexing was unavailable because its daemon could not write `/Users/nithish/.cocoindex_code/daemon.log`.

## 7. Row filters: evaluation and better configuration

### Semantic decisions

Retain the existing AND composition across matching rules. Changing to OR or adding a row-filter bypass would change authorization semantics and is outside this UX redesign.

- Row filters apply to the entire result for a matching reader, not just the columns selected in that rule. This remains true when another matching rule grants different columns.
- Filters operate on original source values before masked values are returned. A filter on a hashed email field therefore compares the original email, not its hash. Preserve and test this behavior in the DuckDB adapter.
- Filter dependencies need not be visible output columns. Selecting a filter field must not add it to the rule's allowed columns. For example, return email while filtering on country without returning country.
- Column mask exemptions and people/group mask exemptions only affect values. Neither exemption bypasses row filters.
- An unfiltered matching rule contributes no new restriction; it does not undo another rule's filter.
- SQL predicates that evaluate to NULL do not pass a WHERE filter. Offer explicit “is empty (NULL)” / “is not empty (NULL)” operators; do not equate NULL with an empty string.

Show concise helper text beside the row editor: “Filters use original values. All matching rules' row filters must pass. Mask exemptions do not bypass row filters.”

### Builder contract

Use **All rows** / **Filter rows**, followed by **Builder** / **DuckDB SQL** within the Filter rows section. Default a new filter to Builder. Load existing arbitrary SQL in SQL mode unchanged; do not guess how to parse arbitrary SQL into builder rows.

For the initial builder, support one flat group with an **All conditions (AND)** / **Any condition (OR)** selector. Each condition contains a searchable schema column selector, a type-appropriate operator, and a typed value input. Nested boolean groups remain expressible in SQL mode, avoiding a complex visual tree in the primary workflow.

Initial supported operators:

- String: equals, does not equal, is one of, is NULL, is not NULL.
- Numeric: equals, does not equal, greater/less than, greater/less than or equal, is one of, is NULL, is not NULL.
- Boolean: equals true/false, is NULL, is not NULL.
- Other or unsupported types and collection traversals: preserve advanced SQL editing and explain that the builder does not support that field yet. Do not emit unverified collection-path SQL.

Add a pure `apps/governance-ui/src/row_filter_editor.ts` helper with tests in `apps/governance-ui/tests/row_filter_editor.test.mjs`. Use a typed local condition model and compile it to the existing `row_filter` SQL string. Persist only SQL, not UI builder state.

Rendering rules:

1. Derive identifier segments from authoritative typed schema paths. Double-quote identifiers, escaping embedded double quotes. Never interpolate `human_path` directly as SQL or split it on dots.
2. Quote string literals and double embedded apostrophes. Validate numeric input as a finite numeric literal before rendering. Boolean values render as TRUE/FALSE. Never allow a raw SQL fragment in a builder value.
3. Wrap each condition in parentheses and join with the selected AND/OR. Emit NULL checks with IS NULL/IS NOT NULL, not equality to NULL.
4. Require at least one complete condition when Filter rows is selected. Empty IN lists and half-completed rows block Save; do not silently downgrade to All rows.
5. Display the generated SQL as read-only text in Builder mode. Opening SQL mode copies the generated expression. After a user edits SQL, keep SQL mode authoritative; returning to Builder requires an explicit reset that states it replaces the SQL. Do not silently discard it.
6. Selecting All rows stores `row_filter: null` through one undoable rule edit. Returning to Filter rows within the same editing session can restore local input, but it must not silently reapply a discarded live restriction.
7. Keep backend DuckDB validation authoritative. The builder is constrained authoring, not a replacement SQL security validator. Preserve the existing prohibited-expression checks in `common/access_control/filters.py`.
8. Integrate incomplete builder/SQL state into the same save and navigation guards as unapplied mask edits. Do not send each incomplete keystroke as a valid policy change.

### Explain interaction through concrete examples

Include these examples in authoring docs and tests:

1. Analyst rule: `country = 'US'`, hash email, exempt `group:privacy-reviewers`. A reader in analysts and privacy-reviewers sees original email for US rows only.
2. Another matching rule: `active = true`. The same reader now receives rows satisfying `(country = 'US') AND (active = true)`.
3. Two matching rules: `country = 'US'` and `country = 'CA'`. Their conjunction returns no rows. An allowed access decision can still produce an empty result; do not label it an authorization denial.
4. If the intended audience should see US or CA rows, configure one rule's builder with Any conditions, yielding `(country = 'US') OR (country = 'CA')`. Creating two overlapping rules is not an equivalent configuration.
5. If one audience should see all rows and another should see only US rows, their matching rule sets must avoid the restrictive overlap. Identity groups can overlap; do not suggest a broad grant as an override. A future row-filter exemption feature would need a separate authorization design.

### Testing and feedback

Extend saved-policy test results to display **Combined row filter**, with “No row restriction” when absent, next to effective masks and output columns. Use the returned effective filter; do not infer matching-rule provenance that the API does not return. Do not promise row counts or sample data from a metadata-only simulation.

Add tests for identifier and literal escaping, literal dotted field names versus nested fields, AND/OR grouping, IN, NULL, invalid numbers, empty conditions, unsupported types, arbitrary existing SQL preservation, switching modes, read-only access, and undo/redo. Add transform tests proving filters still evaluate original masked-column values and that filter-only dependencies do not leak into output. Extend the acceptance scenario with overlapping filters and an exempt reader whose rows remain restricted.

Relevant existing owners: `src/dal_obscura/common/access_control/filters.py`, `src/dal_obscura/common/access_control/policy_resolution.py`, `tests/domain/access_control/test_row_filters.py`, and `tests/infrastructure/adapters/test_duckdb_transform.py`. Run the focused row-filter checks in AGENTS.md in addition to task 7's checks.
