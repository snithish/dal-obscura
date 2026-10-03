# Live configuration

The product uses one directly editable live policy per governed asset. The
control plane validates a complete policy replacement and commits it against an
optimistic `policy_revision`. There is no separate draft, review, or publish stage. The authoring UI
remains a first-class authenticated part of the service.

There is no deployment-wide configuration generation or version-keyed policy
cache. Each schema or read-planning request materializes its asset binding,
catalog configuration, policy rules, and schema admission in one short database
snapshot: PostgreSQL `REPEATABLE READ READ ONLY`, or an explicit SQLite read
transaction. The same request context resolves the source and authorizes it.
The database session closes before provider IO and scan planning. A concurrent
edit affects the next request; it cannot splice new policy rules into an old
asset binding. Configuration database failures fail closed without stale fallback.

Policy, asset, catalog, runtime-settings, and authentication-provider revisions
remain resource-local edit preconditions. They reject concurrent stale writes;
they are not policy history or published configuration versions. Catalog revisions
also participate in the public plugin handle identity contract.

Only provider instances are reused. Their key includes logical catalog identity,
plugin identity, catalog revision, and resolved connection options; it excludes
asset identity and policy revisions. Thus different assets share the same provider
and policy edits do not rebuild it. Each process has a bounded LRU provider pool,
with leases held through discovery, planning, and scan-task serialization. Eviction
closes idle providers; shutdown defers active providers until their final lease
ends. Concurrent misses for the same configuration create one provider. Cold
construction runs outside the pool lock. Full pools wait for a bounded duration,
then return Flight `UNAVAILABLE`. Provider creation failures release reserved slots.

Runtime authentication, path rules, ticket limits, and plugin admission are startup
settings; restart workers to apply those changes. The control-plane observations
endpoint no longer emits a generation identifier or implies worker reload.

Tickets capture their authorized columns, filters, and masks when planned. A
policy edit does not change already issued tickets. By default, they remain
valid until normal expiry. An asset owner or platform admin can revoke all
outstanding tickets for an asset, either as part of a policy save or through a
separate explicit action. Every ticket is bound to a required immutable asset
UUID so revocation has one unambiguous target; older tickets without that
identity are unsupported. Revocation is audited and returns the number of
affected tickets.

The packaged Alembic history contains one fresh-schema baseline. Earlier databases
and tickets are unsupported; no data-conversion or alteration migrations are
shipped. Follow the [fresh deployment guide](core-cutover.md) with a new database.
API 1 plugins and mixed old/new workers are unsupported.

Tickets now contain bounded passive JSON scan envelopes tied to admitted plugin
artifacts. Browser sessions retain CSRF protection, owner authorization and the
supported SSO/bootstrap profiles.

Related guides: [quickstart](quickstart.md), [policy authoring](policy-authoring.md),
[operators](operators.md), [security](security.md).

## Verification and operational limits

Request races are exercised on SQLite WAL and PostgreSQL, including a writer
commit between the asset/catalog SELECT and the policy/schema SELECTs. Provider
regressions cover shared reuse across assets, concurrent misses, bounded capacity,
factory failure, delayed eviction, active shutdown, and Flight `UNAVAILABLE`.
Locked resource reads refresh ORM identities before revision comparisons;
PostgreSQL regressions reject stale writes for all five resource kinds even when
those objects were read before another writer committed.

Metadata snapshots perform three targeted SELECTs, plus transaction setup. They
trade the previous warm-cache counter check for consistent fresh reads. The global
counter write and deployment-wide mutation contention are gone. This is a
correctness and lifecycle improvement, not a measured production throughput claim.
Native catalog resolution still serializes calls within each reused provider;
scan execution and ticket fan-out remain independent. Provider factory/close and
backend calls need their own IO timeouts: the provider-pool timeout bounds waiting
for a slot or another constructor, not arbitrary blocking plugin IO.

The sole packaged baseline is `20261003_0001`, including nested types as text.
Initialize a fresh database before starting services; earlier schemas are unsupported.
No alteration or data-conversion migrations are shipped. Runtime/auth/path
settings require worker restart. Run the PostgreSQL regressions with
`DAL_OBSCURA_POSTGRES_TEST_URL` pointing to a disposable PostgreSQL database;
each test creates and cleans up a unique schema instead of touching existing
application tables.
