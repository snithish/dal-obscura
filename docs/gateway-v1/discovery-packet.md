# Design-partner discovery packet

This packet prepares W14 interviews. It is not evidence of demand. The owner
must authorize recruiting and any external sharing before this packet is sent.

## Interview script

1. Describe the governed Iceberg read that is slow, unsafe, or hard to operate
   today. What consumer executes it: DuckDB, Spark, or another Arrow client?
2. What current engine or product handles it? What failure, cost, or control is
   unacceptable?
3. Show the smallest representative schema. Capture structs, lists, maps,
   decimals, timestamps, binary/fixed values, deletes, and schema evolution.
4. Which fields require each of null, redact, hash, email, keep-last, or
   default masking? Can a parent or collection field be governed directly?
5. Which identities, attributes, and group changes must affect a read? What
   OIDC issuer, audience, TLS, and credential-refresh constraints apply?
6. What Spark, Scala, Java, executor, retry, speculation, and deployment
   versions are required? Which framework receives the same data besides Spark?
7. Can users receive storage credentials today? Is removing that access a real
   requirement with an owner who can enforce it?
8. What scale, latency, concurrency, retention, cost, and operational SLOs
   decide whether this is useful?
9. Will the team provide a synthetic schema/workflow, an evaluation environment,
   and a named engineer for a time-bounded design-partner evaluation?

## Evidence log

Create one entry per conversation. Do not place tokens, credentials, real data,
or unredacted personal information in this repository.

| Field | Record |
| --- | --- |
| Date and interviewer | |
| Role and organization category | |
| Exact workflow and current alternative | |
| Operational buyer and decision owner | |
| Required nested types, masks, and filters | |
| Iceberg catalog, delete, snapshot, and schema-evolution needs | |
| Spark/other consumer versions and executor model | |
| Identity, credential, and storage-boundary requirements | |
| Measurable acceptance criteria | |
| Concrete commitment and next date | |
| Disqualifiers or existing-engine fit | |

## Design-partner threshold

Count a partner only after it commits a named owner, a concrete workflow, a
synthetic or approved evaluation dataset, and time to run the gated evaluation.
Interest, survey responses, and benchmark feedback do not meet this threshold.

## Decision record template

Use this for W15 after independent W13 evidence and W14 conversations.

| Decision input | Evidence | Result |
| --- | --- | --- |
| Security and conformance gates | immutable evaluation artifact | pass / fail |
| Supported contract | compatibility matrix and tested scenarios | |
| Open risks | owner, mitigation, date | |
| Partner commitments | two named commitments or absence evidence | |
| Operational cost | deployment, storage, identity, support | |
| Decision | restricted pilot / hold / pivot / pause | |
| Owner and date | | |

Release a restricted pilot only when every required security/conformance gate
passes and two committed partners have a concrete workflow. Hold when demand is
real but engineering gates fail. Pivot or pause when partners confirm existing
engines solve the workflow or refuse evaluation resources. The preferred pivot
is policy change-impact reporting for one existing engine; it does not imply a
new data plane.
