# Nested governance and framework consumer contract

Normative revision requested by the owner: the pilot includes nested schemas, all existing mask capabilities, DuckDB, Spark and a framework-neutral consumer interface. The backend remains Iceberg. This overrides the earlier primitive-only/two-mask/Python-only proposal, which must not be used as an implementation shortcut.

Full capabilities means completing the declared governed-read contract and its compatibility matrix. It does not mean claiming every Iceberg/framework release works without testing. A missing required capability blocks completion; an implementer may not silently exclude it or mark its tests skipped to ship a smaller pilot.

## 1. Canonical schema and path model — W02

Use one immutable field tree shared by authorization, projection, mask resolution, filter dependency extraction, Arrow schema construction and Iceberg scan planning. Preserve Iceberg field IDs, collection element/key/value IDs, nullability and type metadata needed for correct reads. Display names alone must not bind a stored decision after schema evolution.

Represent paths internally as typed segments: field(name/id), list-element, map-key and map-value. A literal field named `a.b` differs from nested fields `a` then `b`. Define versioned wire encoding in the existing protobuf contract; do not split every string on a dot. A human-readable quoted syntax may parse into these segments, but parsers in Java and Python must share golden fixtures. Do not invent incompatible per-framework dialects.

Required shapes: primitive fields, struct of structs, list of primitives, list of structs, list of lists, map to primitive/struct/list values, and combinations. Distinguish a null container, an empty container, a null element/value and an element with null fields. Do not flatten or explode rows during projection/masking.

Schema evolution: a newly planned snapshot must match a revalidated published schema/field tree. An already planned read remains pinned to its original schema/snapshot; field IDs prevent rebinding to a same-named different field. Required consumer mappings must document decimal precision, timestamp timezone/unit, binary/fixed/UUID and map constraints. Never silently stringify an unsupported type.

## 2. Nested grants and projection — W02/W03

- A parent grant authorizes its descendants in the published field tree; a leaf grant does not authorize siblings. Added descendants require revalidation/republish before wildcard/parent grants can expose them.
- Resolve matching grants to a canonical authorized path tree. Requesting a parent produces only its authorized descendants; an explicit forbidden leaf rejects. Wildcard follows the same pruning rule. No authorized leaves means denial, not a zero-column success.
- Projection retains container structure and nulls. Selecting a leaf in list elements preserves each list's length/order and element nulls, with unauthorized sibling fields removed. Permitting the leaf also permits structural information needed to represent its containing list/map; document this disclosure.
- Map projection retains keys and authorized values. Reading map entries requires permission to disclose their keys as well as requested value fields. Do not expose keys as an incidental implementation detail. Key-changing masks on map keys reject, because they can create null/duplicate keys or change lookup semantics; masking the entire map with typed null remains valid.
- Parent/child request overlap canonicalizes deterministically: the broader request subsumes the narrower shape, but masking and grants are resolved independently and cannot be bypassed by projection order. Duplicate identical explicit paths reject consistently in Java/Python and at the server boundary, as specified in W02.
- Output Arrow schema is generated from this exact projected/masked tree. Spark's declared read schema and actual batches must match it. The connector cannot assume the requested schema equals the authorized schema.

Required examples, with independent expected nested Arrow values:

1. Grant `profile.name`, request `profile`: return a struct containing name only; never return ssn.
2. Grant profile, null-mask profile, request profile.ssn: return a pruned profile field whose value is typed null, not an unmasked ssn or a struct of newly created non-null values.
3. Grant profile, redact-mask profile, request profile: return the replacement string. Request profile.ssn: reject the invalid child projection beneath a scalar replacement. This targeted rejection is not blanket nested rejection.
4. Grant list-element contact.email, mask that email, request contacts: preserve list/null structure, return masked email only, never contact.ssn.
5. Grant map keys and one map-value struct leaf: return keys with pruned values. Without key permission, reject that map read.

## 3. Full mask contract — W02/W03

Retain `null`, `redact`, `hash`, `email`, `keep_last`, `default`. Publish strict parameter/applicability models and golden values/schema tests for all six. These rules replace the old order-dependent integer precedence.

- `null`: typed NULL for any eligible primitive or compound node. Parent null dominates every descendant transform; projected children cannot recover source data. Preserve the projected compound type where descendant projection remains meaningful.
- `redact`: explicit string replacement, including the empty string; source null remains null. Can replace an entire compound node with a scalar string. Descendant projection under that replacement is invalid.
- `default`: explicit non-null finite scalar literal with a strictly declared/canonical output type; outputs the constant even for source null. May replace a compound node with a scalar. No arbitrary SQL, expressions or compound Python values. Descendant projection beneath scalar replacement rejects. Source-null behavior differs intentionally from redact and is tested/documented.
- `hash`: deterministic SHA-256 over a versioned canonical primitive encoding defined by W03, including type/decimal/temporal normalization; null input stays null. Apply at eligible scalar leaves, including leaves inside collections. Do not hash raw language object repr or silently cast whole structs/maps to strings. Document equality disclosure and dictionary-attack limits; never claim anonymization. Compatibility change from prior varchar hashing needs explicit migration/version notes and golden hashes.
- `email`: eligible string leaves only; preserve null. Define one reviewed email-shape rule and replacement behavior; malformed/nonmatching input becomes NULL, never passes through unchanged. Exact output examples required for Unicode, empty input, multiple `@` and missing-domain cases. Do not claim comprehensive email-address validation.
- `keep_last`: eligible string/binary-to-approved-string scalar leaves only with a documented encoding; non-negative integer N with configured upper bound, booleans rejected. N=0 conceals all characters, null remains null. Values of length <=N remain fully visible by explicit policy intent. Define whether N counts Unicode code points (default) and test it. No implicit whole-struct/list casting.

Mask applicability errors reject publication, before a reader receives a ticket. Different applicable output types must be represented exactly in Arrow and all required consumers.

Conflict rules for the same field across potentially overlapping matching rules:

1. Identical normalized masks merge.
2. Null dominates all alternatives, including ancestor/descendant masks.
3. Two keep_last masks select smaller N.
4. Other different masks or different constants reject publication unless an explicitly reviewed equivalence rule proves identical output type/value/null semantics. In particular, hash versus email has no guessed “stricter” ordering.
5. A whole-parent scalar replacement hides the entire subtree. Do not apply leaf masks to reconstruct source beneath it. Conflicting replacements on the same parent follow rules above.

The validator can conservatively reject conflicts across rules whose disjointness it cannot prove. Return useful field/rule diagnostics. Rule order must never change disclosure.

## 4. Nested filters and inference boundary — W03

Scalar struct-leaf predicates are required. Collection predicates require explicit typed existential/universal quantifiers over elements/entries; bare ambiguous list-leaf comparison must not imply an accidental flattening or scalar cast. Extend the existing restricted expression model with reviewed typed predicate nodes and compile them to DuckDB SQL. This is a constrained predicate representation, not arbitrary SQL/UDF execution.

Define quantifier behavior: null collection -> SQL NULL; empty EXISTS -> false; empty ALL -> true; true/false/unknown elements use SQL three-valued existential/universal logic. Top-level filtering retains rows only when result is true. Tests must compute expected rows independently. Map predicates referencing keys require key authorization.

Client predicate dependency checks include every referenced node and ancestor/descendant mask overlap. A caller cannot test an original child beneath a masked parent, test a masked leaf through its collection, or select hidden values using map lookup/quantification. Internal policy predicates may reference hidden fields, but those dependencies must stay out of emitted schemas. Final server-side enforcement remains mandatory after pushdown.

Iceberg/Spark predicate pushdown is an optimization. Retain residual evaluation when exact semantics are unproven. Do not report a filter as fully handled merely because it can be converted to text.

## 5. Consumer support levels — W08/W11/W12

Core interface: versioned Arrow Flight schema/plan/partition-attempt/fetch contract, one canonical authorized Arrow schema, typed errors and documented ownership/cancellation. This custom protocol requires an adapter; generic Arrow support alone does not make an arbitrary Flight client automatically compatible. Flight SQL, JDBC and ADBC compatibility must not be advertised without an implemented and tested bridge.

Required first-class pilot clients:

- Python/PyArrow: context-managed streaming reader plus explicit materializing convenience method, token-refresh hook and bounded resources.
- DuckDB: consumes that reader as a relation without preloading the table. Required tests include nested SQL selection, collection operations, masks and cancellation/lifetime. State whether the relation is single-pass; user's query operators may independently materialize data.
- Java + Spark: retain existing client and DataSource V2 reader, implement nested column pruning, columnar batch mapping, predicate/residual handling, multiple partitions, executor authentication, cancellation and retry/speculation behavior. Begin with existing Spark 3/Java compatibility; W00 records exact Scala/Java/Spark versions and W11 publishes tested combinations. Other major Spark versions are explicit compatibility work, never assumed.

Required portable example: another Arrow-capable framework, Polars by default, consumes governed batches using the same client/adapter contract. Show nested values and masks and distinguish streaming ingestion from an intentionally materializing DataFrame conversion. Do not claim native lazy predicate pushdown without a real adapter/test. Publish a small adapter guide and cross-language golden protocol fixtures so pandas, Ray or other frameworks can integrate without changing the backend.

Consumers must receive equivalent authorized row multisets and schema meaning for the same principal/plan. Differences in framework physical types must use explicit, lossless documented mappings; no silent decimal precision loss, timezone shift, nested sibling exposure or null-container flattening. An unsupported mapping returns a precise capability error and blocks that required matrix cell's completion.

## 6. Spark planning, filtering and zero-column reads — W08c

- Driver plans once against an immutable snapshot/generation and receives serializable partition references and authorized output schema. No open Flight clients, raw storage secrets or bearer tokens embedded in serialized task descriptors.
- Executors authenticate through a configured secure credential provider. Token refresh between attempts is supported; stale groups/attributes/generation remain invalid. Never ship a driver-only access token as a permanent executor credential.
- Each partition maps to its planned scan task(s). Driver/executor separation, multiple executors and actual task scheduling must be tested; local single-thread execution is insufficient.
- Predicate pushdown returns unhandled filters to Spark. Filters on masked fields must be evaluated by Spark over the masked output unless a proven safe server operation exists; do not send them as forbidden predicates on source data. The connector therefore needs authoritative masked-field metadata or another reviewed capability mechanism. Client metadata never substitutes for server enforcement.
- Keep fields needed for residual predicates available in the connector's authorized read schema. Prune before returning final Spark schema; never request unauthorized hidden dependencies on the client's behalf. Nested pruning uses the canonical field tree, not top-level-name shortcuts.
- For Spark count/empty requested schema, request a deterministic authorized sentinel field, apply all server policy filters, and emit zero-column batches carrying correct row counts inside Spark. No unrestricted backend count optimization. If no authorized field exists, deny. Test null-masked sentinel, empty results and row restrictions.

## 7. Attempts, retries and speculation — W06/W08c/W09

Separate three concepts:

- Logical plan: immutable authorized snapshot/partition specification with a bounded configured plan lifetime. Plan reference alone is not bearer authorization to fetch data.
- Attempt: framework task attempt for one planned partition, authenticated/re-authorized when issued.
- Attempt ticket: short-lived single-exchange credential bound to plan, partition, attempt, identity context, generation, payload digest and issuing token expiry.

Add an explicit authenticated attempt-issuance operation. It validates current identity, effective decision, active generation, plan expiry, partition membership and per-plan/partition attempt/concurrency budgets before issuing a ticket. It never replans to latest snapshot. The logical plan can outlive an original access token only because each new attempt requires a newly valid token and full checks; ticket expiry still cannot exceed that attempt's token expiry. The owner-approved maximum plan lifetime bounds retention; no implicit extension.

Attempt IDs are idempotency identifiers, not an authorization claim. A duplicate issuance request for the same attempt may return the same still-unconsumed ticket; after reservation it returns an explicit consumed state/error rather than creating a second ticket. New framework attempt IDs may obtain new bounded attempts. Forged/unbounded attempt IDs cannot bypass issuance quotas.

Exactly one reservation wins per attempt ticket across replicas. Failed/cancelled/partially emitted attempts remain consumed. A retry starts the whole same partition under a new authorized attempt. Spark isolates/discards failed or losing speculative task output; gateway may deliver a partition to more than one authorized attempt and must not claim global exactly-once delivery. Tests prove no duplicate committed Spark results under failure and speculation. Python batch iteration does not silently restart a partly yielded stream.

Both concurrent speculative attempts obey resource admission and authorization. Losing attempts are cancelled and readers closed. Revocation/expiry/plan deletion applies to future attempts as well as current streams. Storage snapshot expiration causes a terminal failure, not a fresh snapshot fallback.

W06 tests and schema/transaction design must cover issuance races separately from reservation races. Store bounded attempt history/quotas and cleanup without weakening replay protection. Use real PostgreSQL and two processes for race tests.

## 8. Required implementation decomposition and exit gates

- W02a: canonical nested field/path tree and versioned Java/Python wire fixtures.
- W02b: all-six-mask strict models and conflict/applicability rules.
- W03a: nested grants/pruned Arrow schema; W03b: compound/leaf masking; W03c: nested/collection filter dependency checks and SQL semantics. Each needs its own reviewed tests.
- W06a: single-attempt ticket integrity/reservation; W06b: authenticated logical-plan/attempt issuance, idempotency, limits and retention.
- W07: native nested field-ID/delete correctness and capability inventory; no silent loss of equality/v3 semantics.
- W08a: Python/core protocol; W08b: DuckDB and portable Arrow example; W08c: Java/Spark distributed schema/filter/read behavior; W08d: executor auth, retries, speculation and cancellation.
- W11/W12: lightweight Python packaging, Java artifacts/Spark compatibility matrix, Maven/distributed CI and cross-consumer golden fixtures.

Each required subpackage has independent red-phase owner review. Overall package completion requires all subpackages. Independent reviewer must attempt parent-mask and collection-filter inference bypasses and inspect Spark retry/credential handling before G2.

The revised scope costs more than the superseded flat-schema Python-only proposal. Keep customer-discovery checkpoints, but do not let a cheaper implementer silently reinstate those cuts to meet a time/performance target.
