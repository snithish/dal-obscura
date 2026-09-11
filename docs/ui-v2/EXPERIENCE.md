# Experience specification

## 1. Users and jobs

**Policy author / asset owner:** locate a dataset, understand existing protections, grant the intended fields, mask sensitive values, restrict rows, test behavior, and prepare a change. Ownership scopes administrative responsibility; it grants neither unrestricted data access nor publishing permission automatically.

**Publisher:** inspect proposed changes, compare effective access, identify uncertainty, publish an exact validated revision, and recover safely if behavior is wrong. Publishing permission is checked by the server for every affected asset.

**Platform operator:** register Iceberg connections and assets, assign administrative capabilities, diagnose drift/unavailable services, manage approved secret references, and verify active configuration.

**Auditor / investigator:** trace who changed access, when it became active, which revision was tested, and how a decision was derived. Read-only administrative access is separately scoped.

Data consumers remain Python, DuckDB, Spark, and other Arrow frameworks. Provide accurate connection instructions and compatibility information; do not turn the administration UI into a general analytics engine.

## 2. Information architecture

Primary navigation contains five destinations:

1. **Assets:** default working destination. Search by logical name, description, owner, connection, and status. Filters persist in the URL. Rows show ownership, policy state, last change, and actionable drift/connection issues. Counts always describe the currently visible scope.
2. **Changes:** saved drafts and publication history. Default to “My drafts”; offer all authorized drafts and published releases. Distinguish draft revision from active release everywhere.
3. **Activity:** permission-scoped authoring/publication/security events with actor, action, asset, time, outcome, and request reference. Expand into authorized details rather than exposing raw payloads.
4. **Connections:** operator-managed Iceberg catalogs, registration, connectivity diagnostics, capabilities, and secret-reference health. Non-operators see only metadata their workflow requires.
5. **Settings:** operator roles, ownership administration, deployment identity, supported auth configuration, and operational status. Startup-bound changes explain required restart/rollout; a saved setting is not presented as already applied.

A small “Needs attention” area above the asset inventory surfaces actionable work: draft conflict, schema drift, failed activation, or unhealthy connection. Avoid decorative KPI dashboards and empty charts. A first-run workspace instead presents three concrete tasks: connect Iceberg, register an asset, test and publish access.

Global search covers only authorized resources. A command palette is a convenience after navigation works, never the only way to perform a task. Use stable deep links to asset, field, rule, draft, and revision. Missing and forbidden resources must follow a consistent disclosure policy.

## 3. Asset workspace

The header shows logical asset name, description, owner, connection, active release, and explicit draft state. Place the primary action according to state: “Edit policy”, “Continue draft”, or “Review change”.

Tabs: **Overview**, **Schema & access**, **Policy**, **Tests**, **History**. Keep status and environment visible while switching tabs.

- Overview explains what this asset is, its owner, supported consumers, active protections, and problems requiring action. Physical identifiers are permission-scoped details, not primary labels.
- Schema & access shows the full administratively authorized schema and a separate persona-specific output view. Metadata visibility and row-read permissions are distinct.
- Policy is the principal authoring surface. Avoid a generic database-record CRUD form.
- Tests contains saved synthetic personas, fixture inputs, expected decisions, results, and freshness.
- History provides immutable revisions, contextual diffs, author/publisher attribution, and “Restore as draft”. It never directly reactivates an old generation.

## 4. Policy studio

At wide widths, use three adjustable regions: schema tree on the left, focused rule editor in the center, and effective-access inspector on the right. On narrower screens, keep the editor primary and switch between Schema / Rule / Result views with clear labels. Preserve selection and keyboard focus when layout changes.

### Nested schema tree

Render structs, list elements, map keys, and map values explicitly. Show field name, type, nullable state, and protection summary. Keep canonical typed path and Iceberg field ID in inspectable details. A literal field named `profile.email` must look different from the nested child `email` under `profile`.

Use tri-state selection with a text explanation of partial selection. Selecting a parent previews exactly which descendants become visible. Label inherited grants/masks separately from rules authored on the selected field. Show map-key dependency when granting map values; never silently grant it. New schema fields stay pending revalidation rather than appearing under an old wildcard grant.

Search reveals matching nodes and their ancestors. Keep selection across collapse/search. For large trees, lazy expansion and virtualization must preserve keyboard semantics and accessible names. Include “Select matching fields” only with an explicit count and preview of the affected set.

### Rule editor

Use an ordinary readable sequence: **Who → Which fields → Which rows → How values appear**. Users can inspect all sections without navigating a wizard for every edit.

- Who: explicit principals/groups and supported mapped attributes. Show issuer context where identity could be ambiguous. Do not enumerate the entire identity directory without an approved, permission-scoped API.
- Which fields: linked schema selection plus a readable summary. Explain that grants union fields across matching rules.
- Which rows: guided builder for supported expressions and an advanced DuckDB expression editor. Both operate on one server-validated expression representation. If an advanced expression cannot be represented visually, keep it intact in advanced mode; never discard clauses to switch modes.
- How values appear: `null`, `redact`, `hash`, `email`, `keep_last`, and `default`. Expose only valid parameters for the selected type; show null handling, output type, ancestor effects, and examples with synthetic values. Label hash as deterministic pseudonymization with equality disclosure, not anonymization.

Rule cards summarize intent in plain language and link to exact fields and expressions. Do not display a rule-order control: order is not a security precedence mechanism. Incomparable mask conflicts must be surfaced with both contributing rules and a precise repair path. Null dominance and compatible `keep_last` composition must match the canonical backend contract.

### Effective-access inspector

Select a synthetic persona and requested projection. Display Allowed / Denied / Invalid / Not evaluated distinctly, visible output fields, output types, effective masks, effective row restriction, and an explanation linking each outcome to matching rules. Show that matching row restrictions combine with AND, even when another rule grants fields.

Persona editing is explicitly a simulation. It never changes the logged-in user's identity or permissions. Unknown/unavailable evaluation must never render as allowed or “0 rows”. Backend evaluation remains authoritative; local hints are marked as such until validated.

Changing the draft invalidates prior preview/test results. Every result is bound to draft revision, schema fingerprint, evaluator version, and fixture/persona revision. Clear stale results or label them prominently and disable publication until required checks are refreshed.

## 5. Golden journeys

### J1 — First governed asset

An operator signs in, registers an allowed Iceberg connection using a secret reference, tests connectivity, selects a physical table, chooses a logical name and owner, verifies schema/capabilities, creates a scoped grant, tests synthetic personas, reviews impact, and publishes. The completion page shows active release and copyable Python/DuckDB/Spark instructions without secrets. Setup failures retain non-secret input and give the next useful action.

### J2 — Nested policy change

An owner opens `customer_revenue`, creates a draft from its active release, grants `profile.contact.email`, selects an email mask, and restricts rows to a region. The studio shows sibling fields excluded and any parent mask effect. Tests cover an allowed analyst, an unauthorized reader, and overlapping group membership. Review shows only the intended field/rule changes and actual combined result.

### J3 — Investigate a denial

An authorized investigator chooses an asset and a synthetic persona, runs an explanation, sees no matching grant or a specific invalid projection, and follows links to the relevant rule/schema node. UI distinguishes simulation from a captured production event. Source rows and bearer tokens are absent from explanations and logs.

### J4 — Publish safely

The publisher reviews the exact draft revision against the current active generation. The page shows affected assets, added/removed visibility, changed masks/restrictions, schema changes, tested personas, and untested scope. Do not infer whole-population impact from three personas. When exact symbolic impact is unavailable, say “Impact not fully determined” and show the literal change plus bounded test evidence.

Explain the current gateway behavior: activating a generation invalidates older tickets across the deployment, including unrelated assets; running streams follow the documented freshness bound. Require an explicit Publish action on this review page, bound to expected generation and draft revision. No optimistic activation UI, automatic publish from autosave, or hidden second submission after timeout.

After submission show “Publishing” until the server resolves the operation. On timeout show “Outcome unknown — checking publication”; reconcile via operation ID and current generation before offering retry. Success links to the immutable release and gateway readiness. “Published” and “Observed active by runtime” are separate facts.

### J5 — Resolve conflict or restore

Two authors edit the same draft. The later stale write receives a conflict with base/local/server versions. Preserve the local edit and offer a deliberate reconciliation flow; never silently last-write-wins. A new active generation invalidates the review and requires revalidation. Restoring history creates a new draft and passes today's schema, policy, and capability checks.

### J6 — Manage and troubleshoot

Operator discovers a drifted schema or failing connection from Assets, follows a scoped diagnostic, fixes a secret reference or revalidates the asset, reviews the effect, and verifies service readiness. Disabling an asset is a reviewed configuration change with visible ticket impact. Removing a connection requires dependency inspection and rejects while referenced. No action deletes source Iceberg data.

## 6. States and interaction rules

Every route and major component must specify loading, empty, populated, partial, error, forbidden, stale, unsaved, saving, saved, conflict, and session-expired states where applicable.

- Autosave only to a draft, after a short idle debounce; use revision-aware writes and display Saving / Saved / Save failed. Never claim persistence before server acknowledgement. Explicit Save remains available.
- Keep unsaved data in memory during transient failures. Do not put tokens, policy literals, or drafts in localStorage by default. On session expiry lock sensitive content and offer sign-in/recovery; reauthorize before restoring access to the draft. Warn before leaving when unsaved changes could be lost.
- Field errors sit next to their controls with an error summary linking to them. Preserve submitted values after validation failure. Expose safe error code/request ID for support, not server stack traces.
- Reserve destructive confirmation for actual deletion/disable/activation consequences. Ordinary navigation, viewing, and draft edits do not need repeated confirmation.
- Never rely on a toast alone for save failure, denied access, conflict, or publication outcome. Toasts may supplement persistent status.
- Keyboard shortcuts are discoverable and do not override assistive-technology patterns. Keep browser navigation, selection, copy/paste, undo in editors, and deep links working.

## 7. Visual system

Proposed direction: a precise editorial workspace with warm neutral surfaces, dark ink text, restrained teal for primary actions, and amber/red for attention/errors. Use typography and spacing to establish hierarchy. Dense data tables and schema trees should feel composed, not compressed into tiny text.

Prototype two treatments in U01: this light editorial direction and a neutral high-density counterpart. Choose through the golden-task sessions and owner visual review. Do not spend a sprint implementing multiple production themes before choosing the core hierarchy.

Define tokens for typography, spacing, semantic colors, focus, borders, elevation, and motion. Use a consistent 4px spacing base, 14–16px working text, larger task headings, and monospace only for paths/expressions/IDs. Tune actual values through readability and contrast checks. Prefer side panels for context and full pages for complex editing; avoid nested modal workflows.

Design real content first: long dataset names, multiple rules, empty schemas, deep nested collections, deleted users, stale revisions, and unavailable services. No dashboard mockups whose apparent usability depends on ideal short labels.

## 8. Accessibility and quality targets

Target WCAG 2.2 AA, including keyboard access, visible/unobscured focus, contrast, accessible authentication, error identification, and target sizing. Automated checks supplement manual screen-reader and keyboard sessions. Reference: [W3C WCAG 2.2](https://www.w3.org/TR/WCAG22/).

All golden journeys must work at 200% zoom and with keyboard-only input. At narrow viewports, ordinary content reflows; inherently two-dimensional tables may scroll with clear labels. Provide accessible alternative navigation for complex trees and diffs. Respect reduced motion. Status must use text/icons as well as color. Announce async results without moving focus unexpectedly.

Proposed performance budgets on a documented reference laptop and seeded server: p95 local edit feedback under 100ms; p95 asset search/list response under 500ms for 10,000 assets; p95 synthetic policy evaluation under 1s for the reference policy corpus; first usable asset view under 2.5s on a 100ms RTT connection. Schema fixture: 5,000 fields, depth 12, with 100 rules. Measure browser memory after 30 open/edit/close cycles; no unbounded retained drafts or editor instances. These are engineering targets to verify, not guarantees inferred from component virtualization.

## 9. Security and trust boundaries

Deploy the UI and administrative API as a separately authorized control-plane surface. Browser calls a same-origin admin boundary; Flight readers cannot call authoring operations by possession of a read token. Prefer established OIDC authorization-code flow with PKCE and server-managed secure HttpOnly sessions, validated state/nonce, session expiry, and CSRF protection for mutations. Review existing auth plumbing before reuse. No homemade identity protocol.

API denies by default and checks action plus resource scope on every request, including metadata, preview, history, exports, and background operation status. Hidden buttons are convenience, not authorization. Ownership updates cannot bootstrap publishing privilege. Reference: [OWASP Authorization Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html).

Policy definitions and metadata may themselves be sensitive. Use no-store responses for sensitive payloads, scoped search/results, sanitized diagnostics, a restrictive CSP, escaped catalog labels, and no third-party session recording. Never send secrets or raw policy content into analytics. Browser receives approved secret reference names/status only; actual secret resolution happens on the server.

Preview cannot execute unrestricted SQL, fetch arbitrary URLs, or impersonate a real user to read storage. Enforce input/fixture size, expression depth, runtime, concurrency, and output limits on the server. Reuse the same evaluator as production and the CLI. Guided editor convenience never bypasses backend validation.

Publication rechecks actor capabilities, current generation, exact draft revision/digest, schema fingerprint, runtime compatibility, and fresh validation evidence inside the authoritative operation. A rollback is another reviewed publication, not a historical bypass. Audit events are recorded server-side with safe attribution; ordinary logs exclude tokens, rows, and raw policy literals.
