# Plugin platform: review and implementation handoff

Created 2026-09-12 against commit `5208eee`. **This is a review and a proposed
implementation plan. It does not implement plugins or certify production readiness.**
Paid-production release remains **HOLD**.

## Read in this order

1. [Implementation review](IMPLEMENTATION_REVIEW.md): observed defects, missing
   functionality, evidence limits, and the repair task for each finding.
2. [Architecture contract](ARCHITECTURE.md): the boundaries and decisions that
   implementations must preserve.
3. [Implementation packets](IMPLEMENTATION_PLAN.md): ordered, bounded tasks for
   an implementation agent, including tests and strict completion criteria.
4. [Acceptance specification](ACCEPTANCE.md): exact scenarios and release gates.
5. [Progress ledger](STATUS.md): current state and evidence requirements.

## Authority and scope

The owner's latest request adds a plugin-based design for multiple catalogs and
data formats. This supersedes the **planning restriction** to one Iceberg backend
in the earlier [UI handoff](../ui-v2/EXECUTION_HANDOFF.md). It does not authorize
advertising untested integrations. Iceberg remains the reference implementation.

This plan is the next execution sequence. Existing UI P00–P16 and gateway W task
IDs remain historical requirements/evidence, not a second competing queue. The
packets below link their remaining requirements into this sequence. A previously
green test or completed task does not close a newly identified regression.

Preserve these owner constraints:

- Policy authoring and management UI, backend enforcement, authentication,
  authorization, nested schemas, and secure local operation are required.
- Preserve the existing pickle-based logic exactly, including serialized class
  import paths and execution semantics. This plan does not approve replacing it.
- Flight workers remain stateless. Use the existing control-plane database for
  approved durable configuration; do not introduce a new state service.
- Masks and row filters remain DuckDB SQL expressions under core validation.
- One isolated deployment, database, and key set per customer is the release
  assumption. Shared multi-customer hosting is not covered by this plan.
- Commit small, verified units. Do not deploy, delete customer data, contact
  reviewers, publish packages, or change the serialization boundary as part of
  completing a packet without the relevant authorization.

## Definition of the desired output

A third-party developer can build and install a catalog or table-format wheel
against a versioned SDK, pass a reusable conformance suite, and add an approved
integration without editing the core router, compiler, policy engine, or UI source.
Operators explicitly install, pin, and enable trusted plugins. The UI configures
only those approved plugins. All supported integrations use the same governed
publication, authentication, authorization, schema, and Flight read paths.

Prove this with separate SQL-Iceberg and REST-Iceberg catalog configurations and
an independently packaged manifest catalog plus Parquet dataset format. These
are proposed qualification targets, not claims of current support. Additional
catalogs and formats follow the same onboarding contract; there is no automatic
promise that every catalog can serve every format.

## Instructions to the implementation agent

Start at **X00**. Read its prerequisites and acceptance scenarios. Add failing
behavioral tests, make the smallest implementation change, run the required
checks, then record exact evidence and an atomic commit. Continue only when the
packet's prerequisites are satisfied. Never mark an unexecuted live test passed.

Do not reinterpret “extensible” as unrestricted dynamic imports, user-uploaded
Python, a browser plugin marketplace, optional authorization, or a single generic
dictionary forwarded into arbitrary provider constructors. Do not silently
remove difficult nested-schema, security, or UI requirements to finish sooner.
