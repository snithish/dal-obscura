# UI and UX implementation contract

Applies to N06–N11; measured by B09–B15/B19/B22 in [acceptance](ACCEPTANCE.md).
This specifies the replacement experience. Existing screenshots and prototypes
are historical references, not constraints. This document does not implement UI.

## Product and information architecture

Make the next safe action obvious without hiding governance details. The primary
journey is: sign in → connect → discover/govern → author → test → review/publish →
consume → audit/revoke. Reuse the backend for every authoritative decision.

Use a persistent application shell with these destinations: Assets, Connections,
Activity, Settings. An asset workspace has Policy, Test access, Changes, History,
and Consume tabs. Asset ownership and grants live in its Access panel. Settings
contains Runtime, Identity providers, and Session/account information. Display the
deployment name and local/production label without exposing infrastructure secrets.

On desktop, show a 224px navigation rail, a compact header with breadcrumbs/search/
account, and a workspace that uses remaining width. Policy editing uses schema
navigation, rule editor, and a collapsible impact inspector. On tablet, collapse
the rail. Below 768px, use a menu drawer and single-column editor with explicit
Schema/Rules/Impact tabs. Never hide account or sign-out to fit the screen.
Do not make wide technical tables force the entire page to scroll horizontally.

Use one typed route definition and stable asset/catalog/version identifiers.
Reload, copied deep links, back/forward and unauthorized/missing targets must work.
Preserve filters in the URL; never put credentials, draft policy contents or
personal identity claims there. Returning from login may restore only a validated
same-origin application route. Revalidate its authorization after login.

Command palette (Cmd/Ctrl+K) supports destination navigation, currently authorized
asset search and help. It does not execute publish/delete/revoke. Advertise only
implemented keyboard shortcuts. Allow Escape to close and restore trigger focus.

## Visual foundation

Use semantic CSS variables plus component CSS modules. Starting palette below is
a concrete design direction, not a claim of measured contrast. Adjust token values
during N06 until every actual text/icon/focus pairing meets B09.

- Light: canvas #F8FAFC; surface #FFFFFF; subtle surface #F1F5F9; text #0F172A;
  secondary text #475569; border #CBD5E1; primary #4338CA with #FFFFFF text;
  selection #EEF2FF with #3730A3 text; focus #4F46E5.
- Dark: canvas #0B1220; surface #111827; elevated surface #1E293B; text #F8FAFC;
  secondary text #CBD5E1; border #64748B; primary #A5B4FC with #111827 text;
  selection #312E81 with #E0E7FF text; focus #A5B4FC.
- Status: success uses teal, warning amber, failure rose, information indigo.
  Pair each with an icon and explicit word. Pending/unknown must not look successful.
  Light status text starts #115E59/#92400E/#9F1239/#3730A3; dark starts
  #99F6E4/#FDE68A/#FDA4AF/#C7D2FE on a sufficiently dark status surface.

Use system sans-serif type and a local monospace stack for SQL/IDs. Base text 16px,
secondary/table text at least 14px, headings 20–28px, line height at least 1.4.
Use a 4px spacing scale, 8–12px control/card radius, restrained borders/shadows,
and clear page hierarchy. Do not wrap every field in a separate decorative card.
Use comfortable default density and consistent 40px controls; important mobile
targets are at least 44px. Keep essential helper text visible, not tooltip-only.

Use Lucide React icons at consistent 16/20/24px sizes and stroke width:
Database for Assets, Plug for Connections, Activity for Activity, Settings for
Settings, ShieldCheck for governance, Search for search, History for history,
LogOut for sign-out, CircleAlert for warnings, CircleCheck for completed actions.
An icon-only button needs an accessible name and visible tooltip; decorative icons
are hidden from assistive technology. No emoji as the primary navigation system.
Semantic meaning must survive grayscale.

Offer Light/Dark/System preference. Only this non-sensitive preference may persist
in browser storage. Respect reduced motion; ordinary transitions <=150ms. Do not
animate removal of private data on logout. Avoid celebratory effects, decorative
charts and simulated activity. Engagement comes from fast, clear completed work.

## Interaction and state contract

Every data panel implements initial loading, loaded, true empty, no match,
forbidden, unavailable and stale states. Skeletons retain layout; long operations
show phase and cancel where supported. A failed GET must not become an empty list,
zero count, default editable settings or a green health indicator. Show last
successful observation time when retaining stale non-sensitive results.

Field errors appear beside their fields and in a focusable submit summary.
Preserve valid draft edits on validation/conflict; show server revision and actions
to inspect/reload/reapply. Never overwrite newer server state implicitly. Provide
Retry for safe reads and Reconcile for uncertain mutations. Pending operations
disable duplicate submissions without disabling navigation or sign-out.
Toasts supplement persistent operation state; they are never its only record.

All editable forms warn before abandoning unsaved edits. Save is explicit.
Offer local undo/redo for policy edits, cleared when changing identity or asset;
do not persist policy drafts in localStorage. Display Saved revision and Active
version independently. Keyboard save does not publish.

## Required workflows and backend obligations

1. **Login and onboarding (N05/N06).** Normal OIDC login works in secure local
   and production modes, with provider selection only when multiple are configured.
   Explain denied/expired/unavailable login and provide the appropriate retry.
   Authenticated first use shows actionable Connect → Govern → Publish progress,
   derived from actual server state. A user lacking permission sees who can perform
   the step, without exposing forbidden identities. No demo-token primary login.
2. **Assets (N08).** Search/filter by name, catalog and governance status with
   bounded server results. Each row shows name, catalog/format, draft/active status
   and last publication when available. Selection retains the exact resource.
   Missing schema is a recoverable error, not an empty schema.
3. **Policy (N08).** Select exact nested field identities, including literal dots,
   lists/maps and nullability. Show type/ID and unsupported-operation reasons.
   Support null, redact, hash, email, keep_last and default masks with typed values;
   source the supported set from the canonical contract. Add/duplicate/reorder/delete rules, edit subjects
   and typed conditions, and edit validated DuckDB row predicates. Condition groups
   use explicit AND/OR, operators and typed operands limited to backend semantics.
   Advanced JSON is lossless and optional. Never simplify an unrepresentable
   expression silently. Show structural errors before save. Empty policy is an
   explicit deny-all draft requiring impact review; it is not an editor error.
4. **Testing (N09).** Choose a test principal and synthetic typed row fixture.
   Results distinguish visible/hidden/masked fields, filter result and policy
   explanation. Label synthetic results prominently. Do not show unauthorized raw
   source rows or claim test fixtures establish live customer-data correctness.
5. **Review/history (N09).** Show semantic before/after fields, masks, predicates,
   conditions and scope tied to saved revisions, active generation and ticket
   impact. Publish has explicit confirmation and authoritative outcome. No stale
   review can publish. A lost response reconciles the original operation key.
   History detail/diff/restore creates a draft, never implicit activation.
   A saved draft offers Copy review link. The reference identifies asset/draft/
   revision, contains no credential and is authorized again when opened.
   A distinct publisher sees the author and exact revision in a read-only view,
   then evaluates/reviews/publishes it. Changing the author's draft invalidates
   that review. No implicit copy to the reviewer's personal draft.
6. **Connections (N10).** Descriptor-driven forms preserve booleans/numbers/enums/
   strings/structured fields and secret references. Existing safe values prefill.
   Separate validation from activation; show affected assets and generation.
   Discovery is searchable/cancellable/bounded. Ambiguous supported format requires
   explicit selection. Expose disabled/retiring/missing/incompatible states with
   exact next action. Retiring a connection never deletes the source dataset.
7. **Access/settings (N11).** Show effective capabilities with explanations, edit
   owners/grants with exact issuer and principal kind, and enforce revision checks.
   Runtime/provider editing includes backend validation and activation status.
   Last-admin protection verifies candidate login before removing current access,
   with audited operator recovery. Client-side hiding never substitutes for API
   authorization. A denied deep link has a useful recovery path.
8. **Activity/health/consume (N11).** Provide bounded audit filtering/pagination,
   redacted detail and request IDs. Separate observed control-plane and Flight
   health; unknown means unknown. Copyable Python/DuckDB/Spark examples target
   the exact governed asset and documented credential acquisition. Qualified
   versions reflect B17 evidence. Never embed credentials or direct source URLs.

## Accessibility and acceptance

Target WCAG 2.2 AA: text contrast >=4.5:1 (large text >=3:1), meaningful graphical
controls/focus >=3:1, visible keyboard focus, labels, logical focus order, status
announcements, accessible names and target sizing. Use native semantics or accessible
primitives; complete manual keyboard and screen-reader review. Automated checks
alone cannot prove accessibility. [WCAG 2.2](https://www.w3.org/TR/WCAG22/)

Validate light/dark, narrow/desktop and 200% zoom; no clipped save/publish/sign-out.
Schema virtualization preserves tree position, selection, keyboard navigation and
an accessible path to every node. Screen-reader output cannot announce a false
total or skip selected offscreen nodes. B12 defines performance gates.

A page is finished only when its real authorized/forbidden/error/conflict workflows
pass, its controls have backend behavior, and its layout satisfies B09. B22 adds
owner visual acceptance and representative-user evidence. A screenshot or color
change alone does not close the UI work.

## Backend ownership for page completion

Use existing route/service families rather than adding one backend per page:

- Assets and schema: assets.py + schema routes/services; consolidate inventory on
  the paginated contract and use authorized IDs from that response.
- Policy/Test/Changes/History: policies.py with draft, policy-evaluate,
  policy-review, policy-versions and policy-operations. N02 removes obsolete
  public policy-rules/policy-preview routes; retain internal evaluator helpers.
  N03 supplies referenced saved drafts for the separate publisher journey.
- Connections: catalog/plugin routes and configuration activation service, with
  descriptor types and authoritative format candidates from N03/N10.
- Access/Settings: existing owner/grant/runtime/auth-provider routes. Metadata
  read and platform administration never bypass governed Flight policy. B15 fixes
  the capability fixtures; the UI must not invent its own role implications.
- Activity: current audit route/service/repository need N11 cursor/filter/query
  extensions. Do not implement filters over the first 200 events in the browser.
- Login: existing session/OIDC routes, extended by N05. Reuse no-store responses,
  CSRF and origin enforcement; do not rebuild an identity provider.

At each page's completion, its primary journey must pass against the actual API.
Mock responses are reserved for deterministic interleaving/error tests. A polished
control with no authorized server effect remains OPEN in the ledger.
