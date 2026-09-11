# A new governance workspace

Status: product specification exists; implementation and validation are incomplete.
The source application, demo cookie login/logout, and UI container packaging exist.
Production OIDC, complete scoped authorization, canonical nested authoring, durable
revision workflows, required management screens, and real-stack/browser evidence
remain incomplete. The local control-plane executable is missing at baseline
`612bb1c`; the documented startup has not been verified. Planning U00/U01 does
not mean their inventory, prototype, or usability acceptance gates have passed.

Owner direction: policy authoring and management UI is non-negotiable. Design the experience from scratch. Existing screens, navigation, framework, and component library impose no constraints. Existing security invariants, durable authoring records, Iceberg scope, nested-schema requirements, and DuckDB/Spark/Arrow consumers remain constraints. Preserve pickle logic under the owner's separate instruction.

This plan supersedes instructions to retire the UI or administrative authoring API in `docs/gateway-v1`. It supplements W02–W05 and W09–W13; it does not declare those gateway packages complete. The CLI remains a supported automation interface sharing the same application services.

Read together:

- [Concrete execution handoff for a less capable coding model](EXECUTION_HANDOFF.md)
- [Execution status and evidence ledger](EXECUTION_STATUS.md)
- [Experience and product specification](EXPERIENCE.md)
- [Implementation packages and acceptance gates](IMPLEMENTATION.md)
- [Visual experience map](experience-map.html), a static planning artifact with illustrative data

## Product thesis

Build a calm, precise workspace that answers three questions: **Who can access this data? What will they receive? What will this change do?**

Make safe governance understandable without requiring authors to reason about manifests, database records, ticket internals, or deployment identifiers. Technical detail stays available when it explains a decision or resolves a problem. The UI must make uncertain or unsupported results explicit.

The primary workflow is: find an asset, inspect its schema and effective access, edit a draft, test representative personas, review the change, publish, and verify activation. Asset owners can finish daily work here without exporting JSON or switching to the CLI.

## Product boundaries

Required release surface: authentication; asset and connection management; nested policy authoring; all six masks; row restrictions; effective-access explanations; synthetic policy tests; draft recovery and conflict handling; reviewed publication; history and restore-as-draft; operator permissions; audit activity; operational status; consumer setup guidance; accessibility; onboarding and complete failure states.

Deferred until evidence supports them: live multi-user cursors, configurable approval chains, access-request marketplaces, IdP administration, multi-tenant self-service, arbitrary SQL workbenches, additional backends, and AI-generated policy changes. Basic ownership, explicit publishing permission, and human review of changes are required; a workflow automation product is not.

A management role does not imply permission to read underlying rows. Default previews use synthetic fixtures. Actual data access stays governed through the gateway with a separately authenticated reader identity.

## Decisions and assumptions

- Start with one configured workspace per deployment. Show its friendly name and environment prominently; keep internal cell/tenant IDs in diagnostics.
- Design desktop-first for substantial authoring, with usable narrow-screen management and review. No essential operation depends on hover, dragging, or a wide display.
- Use an asset-centered model. Global Changes collects draft work across assets; releases reflect the gateway's actual generation-level activation semantics.
- Start with persistent personal drafts and optimistic concurrency. Reuse existing durable control-plane storage; any new tables require a reviewed migration. Do not introduce another database or make the data plane stateful.
- Use synthetic previews at launch. A separate live-preview capability is not a hidden prerequisite for the policy studio.
- Platform administration, asset ownership, policy editing, and publication are separate capabilities. A small deployment may explicitly assign several capabilities to one person.
- Treat a clean React/TypeScript application as the initial implementation candidate, not a dependency on the previous UI. The U02 source foundation uses React, TypeScript, and Vite with pinned version ranges; generate and commit the lockfile before its first release build. No framework migration for its own sake.

## What “best UI/UX” means here

Visual polish matters, but the release bar is measurable comprehension and safe completion. Users must understand nested grants, AND-combined row restrictions, overlapping masks, unsaved versus saved versus active policy, and generation-wide publication impact. An attractive interface that leaves those ambiguous fails.

Validate the design with five representative authors/operators initially. Target at least four completing each core scenario without help, no critical misunderstanding of effective access or publication scope, and no accidental activation. These are proposed pilot thresholds, not results already achieved. Failed scenarios require design changes and another test round.

The first implementation deliverable is a clickable prototype covering one coherent task from login through published verification, including failure paths. Do not build every screen independently and postpone workflow integration.
