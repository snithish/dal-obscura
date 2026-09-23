# Live configuration cutover

**Accepted:** 2026-09-23

The product uses one directly editable live policy per governed asset. The
control plane validates a complete policy replacement and commits it against an
optimistic `policy_revision`. Policy drafts, per-asset review/publish flows,
workspace bundle publication, publication history, and manifest packaging for
publishing configuration are removed from the product and API. The authoring UI
remains a first-class authenticated part of the service.

Tickets capture their authorized columns, filters, and masks when planned. A
policy edit does not change already issued tickets. By default, they remain
valid until normal expiry. An asset owner or platform admin can revoke all
outstanding tickets for an asset, either as part of a policy save or through a
separate explicit action. Every ticket is bound to a required immutable asset
UUID so revocation has one unambiguous target; older tickets without that
identity are unsupported. Revocation is audited and returns the number of
affected tickets.

This is a deliberate breaking schema cutover. The packaged Alembic history is a
single initial baseline for the current live schema. Databases created with the
previous publication/draft history are unsupported; the baseline does not
convert or preserve those records. Start a new database. The discarded migration
chain and old-schema data conversion helpers stay removed.

The codebase keeps its existing security boundary and does not change the
trusted internal pickle-based scan-ticket logic. The UI still uses authenticated
sessions, CSRF protections, owner authorization, and the supported local SSO or
bootstrap profiles.

Current usage docs: [quickstart](../quickstart.md), [policy authoring](../policy-authoring.md),
[operators](../operators.md), [security](../security.md).
