# Implementer handoff and operating rules

Audience: a lower-cost implementation LLM working under owner review. This document is intentionally prescriptive. It does not authorize implementation during the planning task that created it, external outreach, publication or deployment.

## 1. Mission

Owner scope update: policy authoring and management UI is non-negotiable. Follow [the fresh UI/UX plan](../ui-v2/README.md), which supersedes previous UI/API retirement instructions. Existing screens do not constrain the new design. Preserve durable authoring records, the separate admin security boundary, and the owner's pickle-preservation instruction.

Implement only the focused Iceberg gateway for DuckDB, Spark and Arrow-capable frameworks specified in [README.md](README.md). Security invariants S01–S15 are mandatory. Deletion is allowed when explicitly scheduled and verified; a wholesale rewrite is not the default method.

Do not equate passing existing tests with correctness: the reviewed code passed 361 tests while containing demonstrated security/correctness failures. Do not call removed behavior fixed until the retained public surface explicitly rejects it and migration guidance exists.

## 2. How to use the plan without wasting context

At bootstrap, read root AGENTS.md and README.md here. Read the ordered package list and choose the first eligible incomplete package. Owner may assign a particular eligible subpackage.

For each subsequent assignment, read:

1. Applicable AGENTS.md and the scope/security sections of README.md.
2. The assigned section of WORK_PACKAGES.md and its dependency completion packets.
3. Only relevant EVALUATION.md cases plus common evidence rules, and NESTED_AND_CONSUMER_CONTRACT.md for field paths, masks, protocol or consumer work.
4. The specific current source/tests necessary to implement that package. Use known paths/exact search first; use ccc for conceptual search with `COCOINDEX_DISABLE_USAGE_TRACKING=1`. Keep index fresh after meaningful code changes.

Do not reread the entire repository or rerun every expensive benchmark on every small packet. Do not create subordinate agents unless the owner or applicable instructions explicitly request them. A stronger independent review is a gate, not authorization to spawn one automatically.

Use `uv` for Python commands. Run only commands/tools that actually exist; proposed CLI names in this plan are not available until implemented. Inspect installed/locked dependency APIs rather than guessing function signatures.

## 3. Work loop

### Step A: establish state

- Inspect git status and preserve unrelated owner changes.
- Record assigned package/subpackage, prerequisites, current commit and relevant invariant IDs.
- Read code before editing. Name the current failure, expected behavior and smallest useful change.
- If required behavior or an API is unclear, produce a bounded investigation or ask one precise question. Do not make a silent security assumption.

### Step B: tests and expectations first

- Add focused runnable tests using existing fixtures where sound.
- Ensure security tests use real policy/transform code with independently expected rows/values. Keep dataset small unless memory/capacity is the assigned concern.
- Run new tests and show expected semantic failures against current implementation. Import failures do not demonstrate the intended defect.
- For removal/refactoring/packaging work, provide concrete behavioral expectations, deletion inventory and validation commands. Documentation-only corrections need reviewable text, not fabricated tests.
- Prepare the red-phase packet below and wait for owner test approval before production changes.

The source of this gate is root AGENTS.md: “Follow TDD: add tests and expectations first, get review from user only then start code implementation.” The owner approved a detailed planning task and scope reduction, not unspecified tests that have not yet been written. Do not reinterpret silence, elapsed time, green tests or your own review as owner approval. The owner may explicitly approve multiple concrete packets together or change the workflow.

### Step C: implement only approved scope

- Make the smallest change satisfying approved expectations. Prefer 1–3 meaningful production modules per subpackage. If a patch spans many responsibilities, stop and split it before expanding.
- Use strict typed data models and narrow ports. Do not add optional compatibility fallbacks, arbitrary plugins or runtime module imports.
- Keep schema declaration and actual emitted batches consistent. Never suppress exceptions or drop fields/rows silently to satisfy tests.
- Do not modify reviewed expected results merely to match your implementation. A necessary specification change goes back through review.
- No blanket `except Exception: return cached/allow/empty`, permissive defaults, unbounded collection, or hidden whole-result materialization on retained paths.
- Preserve full final policy enforcement even if backend says filter pushdown succeeded.

### Step D: verify

Run approved focused tests, impacted security/conformance cases and relevant quality checks. Run the full inexpensive supported suite before marking a package complete; run expensive suites when that package changes their guarantees or at W13.

Current baseline commands (adjust only when W10–W12 explicitly update package/test layout):

```bash
uv run pytest -m 'not heavy'
uv run ruff check .
uv run ruff format --check .
uv run ty check
```

Do not blindly run `ruff format .` during a narrow change; it can introduce unrelated edits. Format touched files as needed. Environment-limited socket/network failures must be distinguished from real failures; request appropriate execution permissions through normal tooling rather than changing assertions.

Check diff, imports, dependency/package metadata, docs and supported/removed CLI behavior. If public protocol/config changes, update migration examples and old-version rejection tests in the same package.

### Step E: report and checkpoint

Produce the completion packet below. Mark complete only when required tests and reviews exist. If blocked, identify exact missing information or failure; leave status open. Do not auto-advance into an unreviewed production patch.

Do not make commits, publish packages, push images, deploy to a partner or contact anyone unless separately authorized for that action. Preparing a reviewable patch/evaluation artifact is within the implementation assignment; external release is a distinct gate.

## 4. Red-phase packet template

```text
Package/subpackage:
Prerequisites and their evidence:
Security/evaluation IDs:
Current behavior and concrete failure:
Expected behavior (including rejection, error and schema semantics):
Test files added/changed:
Exact test commands and observed failures:
Why failures demonstrate the intended defect:
Proposed production files and change boundaries:
Compatibility/data implications:
Questions that must be resolved before implementation:
Requested decision: approve these tests/expectations, or revise them.
```

If asking approval, explain the AGENTS.md test-review requirement, not a made-up tool restriction. Do not demand approval for every read-only command or routine test run.

## 5. Completion packet template

```text
Package/subpackage and status:
Approved test packet reference:
Behavior changed and why:
Files changed/deleted:
Requirement IDs satisfied and evidence:
Commands, exit codes, passed/failed/skipped counts:
Expected-output / streaming / concurrency / memory proof as relevant:
Mutation or independent-review evidence as relevant:
Migration and rollback implications:
Known limitations and unresolved findings:
No-scope-expansion check:
Next eligible package (not started without its test-review gate):
```

Screenshots and prose claims are not substitutes for machine-readable metrics/test logs where those are required. Never report a run you did not perform. Distinguish baseline results from current results.

## 6. State ledger

During implementation, create proposed `docs/gateway-v1/STATUS.md`. Initially every W00–W15 entry is pending. For each subpackage record owner, dependencies, tests-written status, test-review decision/reference, implementation status, verification artifact path and unresolved issues. Use explicit states: pending, tests-ready, approved, implementing, verification-failed, review-required, complete.

This planning task does not create a completed status ledger because no implementation package has run. A future agent should not mark W00 complete merely because this plan includes old baseline numbers.

Store evaluation artifacts under `evaluation/<run-id>/` when implementation begins. Keep secret-free raw metrics and small reproduction fixtures; do not commit huge datasets, virtualenvs, dependency caches or partner data. Retention of large local artifacts must be documented.

## 7. Stop conditions for a cheaper implementer

Stop the dependent work and request targeted review when:

- Correct Iceberg task reconstruction requires guessing private APIs or dropping delete/field-ID semantics.
- Two written security requirements conflict, or a test's expected authorization outcome is ambiguous.
- A proposed optimization skips authentication, final filtering/masking, generation checks or atomic reservation.
- A malformed/unsupported input currently falls back to a less protected path and the intended rejection contract is unclear.
- A migration would destroy existing authoring/config records or rollback would reactivate known unsafe code.
- A supported security case fails, or a required test is being skipped/xfail'ed to get CI green.
- Cancellation cannot interrupt/block IO within reviewed limits; resource bounds cannot be demonstrated.
- A requirement exceeds the explicit nested/mask/consumer contract, introduces another storage backend, online revocation, server-side SQL or multi-tenancy. Nested types, all six masks and Spark are already required; do not remove them.
- Current source significantly differs from plan assumptions. Report the difference before making a broad compatibility layer.

Make progress on independent authorized tests/documentation while waiting, but do not bypass the unresolved boundary. No automatic rewrite to Rust, no new orchestration framework, no new policy language, no custom cryptographic protocol beyond reviewed standard primitives.

## 8. Copyable assignment prompt

```text
Work on dal-obscura package <Wxx/subpackage> from docs/gateway-v1/WORK_PACKAGES.md.

Read root AGENTS.md, docs/gateway-v1/README.md, the assigned package,
NESTED_AND_CONSUMER_CONTRACT.md, relevant EVALUATION.md cases, and
AGENT_HANDOFF.md. Respect existing changes.

Goal: full governed read capabilities on Iceberg for DuckDB, Spark and other
Arrow frameworks, including nested schemas and all six masks. Security invariants
S01–S15 are mandatory. No unrelated features or broad refactors. Use uv;
use ccc for conceptual search with usage tracking disabled.

First phase: add the specified focused tests and expected outcomes. Run them
against current code, confirm semantic failures, and produce a red-phase
review packet. Stop before production implementation until I approve the
concrete tests/expectations. Do not treat approval of the overall plan as
approval of tests not yet written.

After approval: implement only this subpackage, run its required checks,
review the diff, update STATUS.md, and produce the completion packet. Do not
weaken assertions, skip security cases, or fabricate evidence. Escalate any
unclear security/Iceberg/serialization decision instead of guessing.

Do not deploy, publish, contact third parties, start another task, or spawn
agents. Report the next eligible package without implementing it unreviewed.
```

## 9. Reviewer guidance

Review meaning before style. A reduced feature set with precise rejection is preferable to a broad surface whose behavior is guessed. Check the actual emitted values, schema and resource lifecycle, not only helper return objects.

Security-critical packets W02–W09 (including distributed consumer attempts) require owner/capable review of semantics. W13 requires a reviewer independent from the implementer's self-assessment. The cheaper model can do mechanical, well-bounded work; it should not be entrusted with silently choosing the security model.
