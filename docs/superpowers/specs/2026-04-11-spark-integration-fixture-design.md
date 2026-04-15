# Spark Integration Fixture Design

## Goal

Strengthen the Spark connector integration suite so it exercises realistic
`dal-obscura` behavior over a large Iceberg-backed dataset instead of a trivial
flat table.

The new integration harness must:

- keep using the Python-driven local fixture builder and a real local
  `dal-obscura` process
- continue using the Iceberg table format
- reuse existing Python test helpers as much as possible
- generate a complex schema with at least 20 fields, multiple levels of nesting,
  and complex Arrow-compatible types
- generate at least 100,000 rows
- produce multiple planned tickets in realistic reads
- cover pushed filters, residual Spark filters, masking behavior, nested
  projection, and partition-aware planning behavior

## Why Change

The current integration fixture is too small and too flat to validate the
connector against the behaviors that matter in production:

- nested projection and dotted-path semantics
- masking across top-level, nested-struct, and list-of-struct fields
- correctness when Spark mixes pushed and residual predicates
- large scans that require multiple planned tickets
- selective reads over physically partitioned data

The test suite should still be understandable when it fails. That argues for
one heavyweight shared fixture plus several focused integration tests, not a
single giant assertion block.

## Chosen Shape

Use one heavyweight Iceberg fixture shared by several focused Spark integration
tests.

### Shared fixture responsibilities

`tests/support/build_connector_fixture.py` remains the JVM-facing entry point
and emits the same bundle metadata expected by the Java testkit. Its job grows
from "build a tiny demo table" to "orchestrate a realistic fixture":

- define the complex Arrow schema and deterministic dataset values
- create the Iceberg table through reusable Python support helpers
- write catalog, policy, and app configuration files
- emit fixture metadata for the JVM harness

### Spark integration test responsibilities

`SparkReadIT` should stop being two minimal smoke tests and instead become a
small set of focused end-to-end tests over the same shared fixture:

1. pushed projection and filter correctness
2. residual filter correctness for function-shaped Spark expressions
3. masking over top-level and nested fields
4. large scan / multi-ticket behavior and selective planning behavior
5. missing-token or authorization failure coverage

This keeps the fixture realistic without making every failure opaque.

## Reuse Strategy

The fixture must reuse existing Python support code instead of introducing a
parallel Iceberg setup path.

### Existing helpers to reuse

- `tests/support/iceberg.py:create_iceberg_table`
- existing policy and masking semantics already validated in Python tests
- existing app-config layout used by the current JVM fixture builder

### Required helper evolution

The current `create_iceberg_table(...)` helper only supports the minimal
`id/email/region` schema. It should be refactored into a more general helper
that still preserves current callers:

- keep the current simple call pattern working unchanged
- add optional parameters for:
  - custom Arrow schema
  - custom append batches or Arrow tables
  - custom table properties
  - optional partition spec
  - identifier override

The integration fixture should call the generalized helper rather than creating
its own direct PyIceberg setup logic.

This keeps one canonical Iceberg test-creation path in the repo and prevents
fixture drift between Python and JVM tests.

## Dataset Design

The shared dataset should be deterministic and broad enough to exercise nested
projection, masking, residual filtering, and selective planning.

### Row volume

- at least 100,000 rows
- written across multiple append batches / files
- enough physical distribution that `dal-obscura` planning can return multiple
  tickets on broad reads

### Schema requirements

The schema should include at least 20 projected fields across:

- top-level scalar fields:
  - ids
  - tenant / region / status style dimensions
  - booleans
  - integers and longs
  - decimals
  - dates
  - timestamps
- nested structs, for example:
  - `user.address.zip`
  - `profile.contacts.email`
  - `device.metadata.os`
- list-of-struct fields, for example:
  - `preferences.history[*].theme`
- maps and nullable branches where supported cleanly by the existing row
  transform and masking path

The data values should be formulaic so the tests can assert exact counts,
ordered ids, and selected nested-field values without randomness.

## Masking Coverage

The policy fixture should cover all masking rule types that the current masking
adapter and tests already support. Based on the existing Python tests, that
includes:

- `null`
- `redact`
- `hash`
- `default`
- `email`
- `keep_last`

Masking should be exercised over:

- top-level scalar fields
- nested struct descendants
- list-of-struct descendants

At least one requested projection should include both masked and unmasked nested
fields from the same parent structure so schema-shaping errors become visible.

## Partitioned Iceberg Layout

The fixture should use a partitioned Iceberg table if the generalized helper can
support it cleanly.

Recommended partition dimensions:

- coarse event-date or event-month partition
- region / tenant-style partition dimension

The purpose is not to test Spark's native Iceberg datasource pruning. Spark only
sees the `dal_obscura` datasource. The purpose is to make selective reads result
in different service-side planning outcomes so the harness can validate that
predicate selectivity materially changes the number of planned partitions or
tickets.

If partition-spec support cannot be added to the existing helper cleanly, the
fallback is:

- keep the table Iceberg-backed
- still write many files / append batches
- still assert multi-ticket broad scans

But partitioned layout is preferred.

## Test Coverage Plan

### 1. Pushed projection and pushed filter test

Verify that a read using only directly translatable predicates:

- selects nested and top-level columns
- returns the expected rows
- preserves masked field semantics
- uses a pushed `row_filter` path

Assertions should include exact result rows or exact ordered ids.

### 2. Residual filter test

Use a function-shaped predicate such as:

- `upper(name) = 'HOUSE'`

or a mixed pushed-plus-residual expression.

Verify:

- final results are correct
- unsupported filter parts are evaluated by Spark
- the connector does not approximate the function-shaped predicate into pushed
  SQL

This test exists specifically to protect the translator hardening work.

### 3. Masking test

Read a projection that includes:

- top-level masked columns
- nested masked dotted-path columns
- list-of-struct masked descendants

Verify both the resulting values and the exposed schema shape.

### 4. Multi-ticket / selective-planning test

Compare:

- a broad scan with weak or no filters
- a selective scan over partition-aligned dimensions

Verify:

- broad scans produce multiple planned tickets
- selective scans produce fewer planned tickets or partitions
- row counts remain correct

This requires exposing planning evidence from the harness, for example through
fixture-side logs, a lightweight test-only endpoint, or testkit access to plan
metadata. The preferred option is a testkit-level inspection hook that stays out
of the production connector API.

### 5. Failure-path test

Keep one focused auth/config failure case, such as missing token, because the
heavy fixture should not erase basic connector ergonomics coverage.

## Harness Observability

To validate pushed vs residual behavior and planning selectivity, the harness
needs more than final row values.

Preferred observability additions:

- testkit access to the most recent plan request payload(s)
- plan count / ticket count visibility for a given read
- optionally, captured service logs when assertions fail

These should remain test-only helpers and must not alter the production runtime
contract.

## Non-Goals

- changing the production `dal-obscura` protocol
- adding write-path integration coverage
- benchmarking absolute Spark performance
- replacing Python fixture generation with a JVM-native fixture builder
- introducing a second independent Iceberg table-creation path

## Risks And Mitigations

### Risk: oversized fixture makes the suite flaky or slow

Mitigation:

- keep one shared fixture bundle per test class
- use deterministic generated values instead of expensive random generation
- keep focused assertions per test rather than repeated broad scans

### Risk: partition-aware assertions become brittle

Mitigation:

- assert relative planning differences, such as "selective read plans fewer
  tickets than broad read", rather than hard-coding exact ticket counts unless
  the helper can make that stable

### Risk: helper refactor breaks existing Python tests

Mitigation:

- evolve `create_iceberg_table(...)` compatibly
- preserve current defaults and call signatures
- verify existing helper callers still pass unchanged

## Implementation Direction

The implementation should proceed in this order:

1. generalize the Iceberg helper compatibly
2. build the heavyweight Python fixture on top of that helper
3. extend the JVM testkit only as needed for planning observability
4. replace the minimal Spark integration tests with focused tests over the
   shared fixture
5. run full JVM verification and the Spark integration suite from a clean build

## Acceptance Criteria

The redesign is complete when:

- the shared fixture is Iceberg-backed and generated via reusable Python support
  code
- the dataset contains at least 100,000 rows and a 20+ field complex schema
- the policy exercises all supported masking rule types
- the Spark IT suite includes separate focused tests for pushed filters,
  residual filters, masking, and selective planning / multi-ticket behavior
- the suite passes through the normal Maven reactor
- existing simpler Python Iceberg helper callers continue to work
