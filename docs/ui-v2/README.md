# A new governance workspace

Status: product specification exists; implementation and validation are incomplete.
Paid-production release is **on hold**. See the code-backed
[production readiness review and required backend/deployment work](PRODUCTION_READINESS.md).
The source application now includes OIDC/PKCE browser sessions, scoped owner and
delegated capabilities, canonical nested Iceberg schema authoring, durable
revisioned drafts, reviewed/idempotent publication, history/restore, activity,
catalog discovery, asset onboarding, and production-profile startup guards. The
control-plane executable, data-plane TLS checks, Flight readiness action, restore
invalidation command, and local UI build are implemented. Clean installed-wheel,
container, PostgreSQL, real-IdP/browser, and consumer evidence remain unverified;
paid-production release stays on hold. The rewritten handoff describes remaining
work only, with one canonical sequence. Prior planning does not satisfy prototype
or usability acceptance.

Owner direction: policy authoring and management UI is non-negotiable. Design the experience from scratch. Existing screens, navigation, framework, and component library impose no constraints. Existing security invariants, durable authoring records, Iceberg scope, nested-schema requirements, and DuckDB/Spark/Arrow consumers remain constraints. Preserve pickle logic under the owner's separate instruction.

This plan supersedes instructions to retire the UI or administrative authoring API in `docs/gateway-v1`. It supplements W02–W05 and W09–W13; it does not declare those gateway packages complete. The CLI remains a supported automation interface sharing the same application services.

Read together:

- [Authoritative remaining implementation plan](EXECUTION_HANDOFF.md)
- [Execution status and evidence ledger](EXECUTION_STATUS.md)
- [Backend coverage and paid-production release gates](PRODUCTION_READINESS.md)
- [Experience and product specification](EXPERIENCE.md)
- [Plan entry point and older package mapping](IMPLEMENTATION.md)
- [Visual experience map](experience-map.html), a static planning artifact with illustrative data

## Product thesis

Build a calm, precise workspace that answers three questions: **Who can access this data? What will they receive? What will this change do?**

Make safe governance understandable without requiring authors to reason about manifests, database records, ticket internals, or deployment identifiers. Technical detail stays available when it explains a decision or resolves a problem. The UI must make uncertain or unsupported results explicit.

The primary workflow is: find an asset, inspect its schema and effective access, edit a draft, test representative personas, review the change, publish, and verify activation. Asset owners can finish daily work here without exporting JSON or switching to the CLI.

## Product boundaries

Required release surface: authentication; asset and connection management; nested policy authoring; all six masks; row restrictions; effective-access explanations; synthetic policy tests; draft recovery and conflict handling; reviewed publication; history and restore-as-draft; operator permissions; audit activity; operational status; consumer setup guidance; accessibility; onboarding and complete failure states.

Deferred until evidence supports them: live multi-user cursors, configurable approval chains, access-request marketplaces, IdP administration, multi-tenant self-service, arbitrary SQL workbenches, additional backends, and AI-generated policy changes. Basic ownership, explicit publishing permission, and human review of changes are required; a workflow automation product is not.

A management role does not imply permission to read underlying rows. Default previews use synthetic fixtures. Actual data access stays governed through the gateway with a separately authenticated reader identity.

Every required screen must map to a real authorized backend service, a durable
workflow where applicable, and API/browser acceptance evidence. Mocked fixtures,
placeholder destinations, and static build checks cannot satisfy release scope.
P13–P16 add deployment, recovery, operations, and promotion requirements to P00–P12.

## Decisions and assumptions

- Start with one configured workspace per deployment. Show its friendly name and environment prominently; keep internal cell/tenant IDs in diagnostics.
- Design desktop-first for substantial authoring, with usable narrow-screen management and review. No essential operation depends on hover, dragging, or a wide display.
- Use an asset-centered model. Global Changes collects draft work across assets; releases reflect the gateway's actual generation-level activation semantics.
- Start with persistent personal drafts and optimistic concurrency. Reuse existing durable control-plane storage; any new tables require a reviewed migration. Do not introduce another database or make the data plane stateful.
- Use synthetic previews at launch. A separate live-preview capability is not a hidden prerequisite for the policy studio.
- Platform administration, asset ownership, policy editing, and publication are separate capabilities. A small deployment may explicitly assign several capabilities to one person.
- Keep the current React/TypeScript/Vite foundation and committed lockfile. Verify
  the pinned toolchain in a clean build; organize features without an unnecessary
  framework migration.

## What “best UI/UX” means here

Visual polish matters, but the release bar is measurable comprehension and safe completion. Users must understand nested grants, AND-combined row restrictions, overlapping masks, unsaved versus saved versus active policy, and generation-wide publication impact. An attractive interface that leaves those ambiguous fails.

Validate the design with five representative authors/operators initially. Target at least four completing each core scenario without help, no critical misunderstanding of effective access or publication scope, and no accidental activation. These are proposed pilot thresholds, not results already achieved. Failed scenarios require design changes and another test round.

Use the evolving real vertical workflow for interaction review, with prototype
fixtures explicitly isolated. Complete login through publication verification,
including failure paths; do not build every screen independently and defer
integration. Owner visual and participant usability acceptance remain required.
