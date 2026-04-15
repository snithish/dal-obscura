# Spark Integration Fixture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the minimal Spark integration fixture with a heavyweight Iceberg-backed fixture that validates nested projection, masking, pushed and residual filters, and multi-ticket planning over a large realistic dataset.

**Architecture:** Keep the current Python fixture-builder plus JVM testkit architecture, but generalize the shared Python Iceberg helper so the heavyweight fixture reuses existing setup code instead of forking it. Extend the JVM testkit only for test-only observability, then split the Spark integration test into several focused end-to-end tests over one shared heavyweight fixture.

**Tech Stack:** Python 3.10, `uv`, PyArrow, PyIceberg, Java 11, Maven, Spark 3.5.x, JUnit 5

---

## File Structure

- Modify: `tests/support/iceberg.py`
- Modify: `tests/support/build_connector_fixture.py`
- Modify: `connectors/jvm/connector-testkit-jvm/src/main/java/io/dalobscura/connectors/testkit/FixtureBundle.java`
- Modify: `connectors/jvm/connector-testkit-jvm/src/main/java/io/dalobscura/connectors/testkit/FixtureBuilderRunner.java`
- Modify: `connectors/jvm/integration-tests-jvm/src/test/java/io/dalobscura/connectors/integration/SparkReadIT.java`
- Modify: `connectors/README.md`
- Modify: `README.md`
- Create: `tests/support/test_iceberg.py`
- Create: `tests/test_support_build_connector_fixture.py`

### Responsibility Map

- `tests/support/iceberg.py`: reusable Iceberg table creation path for both simple and heavyweight test fixtures
- `tests/support/build_connector_fixture.py`: heavyweight shared fixture orchestration, dataset generation, policy generation, app-config generation, and fixture metadata output
- `connector-testkit-jvm`: test-only fixture metadata plumbing from Python into JVM tests
- `SparkReadIT.java`: focused end-to-end Spark integration coverage over the shared heavyweight fixture
- Python tests: protect helper compatibility and fixture shape so JVM failures do not become the first signal

---

## Task 1: Generalize the Shared Iceberg Helper Compatibly

**Files:**
- Modify: `tests/support/iceberg.py`
- Create: `tests/support/test_iceberg.py`

- [ ] **Step 1: Write failing Python tests for backward compatibility and generalized append support**

```python
import pyarrow as pa

from tests.support.iceberg import create_iceberg_table


def test_create_iceberg_table_preserves_simple_defaults(tmp_path):
    identifier = create_iceberg_table(
        tmp_path,
        "spark_catalog",
        "warehouse",
        [1, 2, 3],
    )

    assert identifier == "default.users"


def test_create_iceberg_table_accepts_custom_schema_and_arrow_batches(tmp_path):
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string()),
            pa.field("user.address.zip", pa.string()),
        ]
    )
    batches = [
        pa.table(
            {
                "id": [1, 2],
                "region": ["us", "eu"],
                "user.address.zip": ["10001", "10002"],
            },
            schema=schema,
        )
    ]

    identifier = create_iceberg_table(
        tmp_path,
        "spark_catalog",
        "warehouse",
        identifier="analytics.heavy_users",
        arrow_schema=schema,
        append_tables=batches,
    )

    assert identifier == "analytics.heavy_users"
```

- [ ] **Step 2: Run the new helper tests and verify the generalized path is missing**

Run: `uv run pytest tests/support/test_iceberg.py -q`

Expected: FAIL because `create_iceberg_table(...)` does not yet accept `arrow_schema` or `append_tables`.

- [ ] **Step 3: Implement a generalized helper while preserving current callers**

```python
from __future__ import annotations

from contextlib import suppress
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


def create_iceberg_table(
    tmp_path: Path,
    catalog_name: str,
    warehouse_name: str,
    values: list[int] | None = None,
    *,
    identifier: str = "default.users",
    append_batches: list[list[int]] | None = None,
    arrow_schema: pa.Schema | None = None,
    append_tables: list[pa.Table] | None = None,
    table_properties: dict[str, str] | None = None,
    partition_spec: PartitionSpec | None = None,
) -> str:
    warehouse = tmp_path / warehouse_name
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        catalog_name,
        type="sql",
        uri=f"sqlite:///{tmp_path / f'{catalog_name}.db'}",
        warehouse=str(warehouse),
    )

    if arrow_schema is None:
        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("email", pa.string(), nullable=True),
                pa.field("region", pa.string(), nullable=True),
            ]
        )

    schema = _pyiceberg_schema_for_arrow(arrow_schema)
    namespace = ".".join(identifier.split(".")[:-1])
    with suppress(Exception):
        catalog.create_namespace(namespace)

    create_kwargs = {
        "identifier": identifier,
        "schema": schema,
        "properties": {"format-version": "2", **(table_properties or {})},
    }
    if partition_spec is not None:
        create_kwargs["partition_spec"] = partition_spec

    table = catalog.create_table(**create_kwargs)

    if append_tables is not None:
        tables = append_tables
    else:
        batch_values = append_batches or [values or []]
        tables = [_default_table_for_values(batch, arrow_schema) for batch in batch_values]

    for table_batch in tables:
        table.append(table_batch)
    return identifier
```

- [ ] **Step 4: Run the helper tests and verify they pass**

Run: `uv run pytest tests/support/test_iceberg.py -q`

Expected: PASS with 2 tests passing.

- [ ] **Step 5: Commit the helper generalization**

```bash
git add tests/support/iceberg.py tests/support/test_iceberg.py
git commit -m "refactor(tests): generalize shared iceberg helper"
```

---

## Task 2: Build the Heavyweight Shared Python Fixture

**Files:**
- Modify: `tests/support/build_connector_fixture.py`
- Create: `tests/test_support_build_connector_fixture.py`

- [ ] **Step 1: Write failing Python tests for fixture shape and metadata**

```python
import json
import subprocess
from pathlib import Path


def test_build_connector_fixture_emits_heavy_fixture_metadata(tmp_path):
    output_dir = tmp_path / "fixture"
    result = subprocess.run(
        [
            "uv",
            "run",
            "tests/support/build_connector_fixture.py",
            "--output-dir",
            str(output_dir),
            "--port",
            "8815",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload["catalog"] == "spark_catalog"
    assert payload["target"]
    assert "expected" in payload
    assert payload["expected"]["row_count"] >= 100_000
    assert payload["expected"]["supports_multiple_tickets"] is True
```

- [ ] **Step 2: Run the fixture-builder tests and verify the metadata is not there yet**

Run: `uv run pytest tests/test_support_build_connector_fixture.py -q`

Expected: FAIL because the current fixture output has no heavyweight expectations or large dataset shape.

- [ ] **Step 3: Implement deterministic complex schema, large batches, Iceberg writes, and policy generation**

```python
HEAVY_ROW_COUNT = 100_000
HEAVY_BATCH_SIZE = 10_000


def _heavy_arrow_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("tenant_id", pa.string()),
            pa.field("region", pa.string()),
            pa.field("status", pa.string()),
            pa.field("active", pa.bool_()),
            pa.field("score", pa.int32()),
            pa.field("balance", pa.decimal128(18, 2)),
            pa.field("event_date", pa.date32()),
            pa.field("event_ts", pa.timestamp("us")),
            pa.field(
                "user",
                pa.struct(
                    [
                        pa.field("name", pa.string()),
                        pa.field("email", pa.string()),
                        pa.field(
                            "address",
                            pa.struct(
                                [
                                    pa.field("zip", pa.string()),
                                    pa.field("city", pa.string()),
                                    pa.field("country", pa.string()),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
            pa.field(
                "profile",
                pa.struct(
                    [
                        pa.field(
                            "contacts",
                            pa.struct(
                                [
                                    pa.field("email", pa.string()),
                                    pa.field("phone", pa.string()),
                                ]
                            ),
                        ),
                        pa.field("marketing_opt_in", pa.bool_()),
                    ]
                ),
            ),
            pa.field(
                "metadata",
                pa.struct(
                    [
                        pa.field(
                            "preferences",
                            pa.list_(
                                pa.struct(
                                    [
                                        pa.field("name", pa.string()),
                                        pa.field("theme", pa.string()),
                                    ]
                                )
                            ),
                        ),
                        pa.field("tags", pa.map_(pa.string(), pa.string())),
                    ]
                ),
            ),
            pa.field("account_id", pa.string()),
            pa.field("notes", pa.string()),
        ]
    )


def _heavy_tables(schema: pa.Schema) -> list[pa.Table]:
    tables: list[pa.Table] = []
    for start in range(0, HEAVY_ROW_COUNT, HEAVY_BATCH_SIZE):
        stop = min(start + HEAVY_BATCH_SIZE, HEAVY_ROW_COUNT)
        ids = list(range(start + 1, stop + 1))
        tables.append(_build_batch_table(schema, ids))
    return tables
```

- [ ] **Step 4: Reuse the generalized Iceberg helper from Task 1**

```python
table_id = create_iceberg_table(
    output_dir,
    "spark_catalog",
    "warehouse",
    identifier="default.heavy_users",
    arrow_schema=_heavy_arrow_schema(),
    append_tables=_heavy_tables(_heavy_arrow_schema()),
    table_properties={"write.distribution-mode": "hash"},
    partition_spec=_heavy_partition_spec(),
)
```

- [ ] **Step 5: Emit richer fixture metadata for JVM tests**

```python
print(
    json.dumps(
        {
            "uri": f"grpc+tcp://localhost:{args.port}",
            "catalog": "spark_catalog",
            "target": table_id,
            "app_path": str(app_path),
            "jwt_secret": JWT_SECRET,
            "ticket_secret": TICKET_SECRET,
            "user_token": user_token,
            "expected": {
                "row_count": HEAVY_ROW_COUNT,
                "supports_multiple_tickets": True,
                "sample_us_even_ids": [2000, 4000, 6000],
                "masked_zip_hash_length": 64,
            },
        }
    )
)
```

- [ ] **Step 6: Run the fixture-builder tests and verify they pass**

Run: `uv run pytest tests/test_support_build_connector_fixture.py -q`

Expected: PASS with heavyweight metadata assertions succeeding.

- [ ] **Step 7: Commit the heavyweight fixture builder**

```bash
git add tests/support/build_connector_fixture.py tests/test_support_build_connector_fixture.py
git commit -m "test(connectors): build heavy Spark fixture"
```

---

## Task 3: Expose Test-Only Planning Observability to the JVM Harness

**Files:**
- Modify: `connectors/jvm/connector-testkit-jvm/src/main/java/io/dalobscura/connectors/testkit/FixtureBundle.java`
- Modify: `connectors/jvm/connector-testkit-jvm/src/main/java/io/dalobscura/connectors/testkit/FixtureBuilderRunner.java`
- Modify: `tests/support/build_connector_fixture.py`

- [ ] **Step 1: Write a failing JVM-side test for richer fixture bundle metadata**

```java
@Test
void fixtureBundleIncludesExpectedPlanningMetadata() throws Exception {
    FixtureBundle bundle = FixtureBuilderRunner.build();

    assertTrue(bundle.expectedRowCount() >= 100_000);
    assertTrue(bundle.supportsMultipleTickets());
}
```

- [ ] **Step 2: Run the connector-testkit compile/test path and verify the new accessors do not exist yet**

Run: `mvn -f connectors/jvm/pom.xml -pl connector-testkit-jvm -am test`

Expected: FAIL because `FixtureBundle` does not expose the richer metadata.

- [ ] **Step 3: Extend `FixtureBundle` with test-only expected values**

```java
public final class FixtureBundle {
    private final long expectedRowCount;
    private final boolean supportsMultipleTickets;
    private final List<Long> sampleUsEvenIds;
    private final int maskedZipHashLength;

    public long expectedRowCount() {
        return expectedRowCount;
    }

    public boolean supportsMultipleTickets() {
        return supportsMultipleTickets;
    }
}
```

- [ ] **Step 4: Parse the richer JSON payload in `FixtureBuilderRunner`**

```java
JsonNode expected = node.get("expected");
return new FixtureBundle(
        node.get("uri").asText(),
        node.get("catalog").asText(),
        node.get("target").asText(),
        node.get("user_token").asText(),
        Path.of(node.get("app_path").asText()),
        node.get("jwt_secret").asText(),
        node.get("ticket_secret").asText(),
        expected.get("row_count").asLong(),
        expected.get("supports_multiple_tickets").asBoolean(),
        readLongList(expected.get("sample_us_even_ids")),
        expected.get("masked_zip_hash_length").asInt());
```

- [ ] **Step 5: Run the connector-testkit reactor slice and verify it passes**

Run: `mvn -f connectors/jvm/pom.xml -pl connector-testkit-jvm -am test`

Expected: PASS.

- [ ] **Step 6: Commit the JVM testkit metadata plumbing**

```bash
git add \
  connectors/jvm/connector-testkit-jvm/src/main/java/io/dalobscura/connectors/testkit/FixtureBundle.java \
  connectors/jvm/connector-testkit-jvm/src/main/java/io/dalobscura/connectors/testkit/FixtureBuilderRunner.java \
  tests/support/build_connector_fixture.py
git commit -m "test(connectors): expose heavy fixture metadata"
```

---

## Task 4: Replace the Minimal Spark ITs with Focused Heavyweight Coverage

**Files:**
- Modify: `connectors/jvm/integration-tests-jvm/src/test/java/io/dalobscura/connectors/integration/SparkReadIT.java`

- [ ] **Step 1: Write the new failing integration test skeletons**

```java
@Test
void readsNestedProjectionWithPushedFilters() throws Exception {}

@Test
void appliesResidualFunctionFiltersInSpark() throws Exception {}

@Test
void returnsMaskedTopLevelAndNestedFields() throws Exception {}

@Test
void plansBroadReadsMoreWidelyThanSelectiveReads() throws Exception {}

@Test
void failsClearlyWhenTheTokenIsMissing() throws Exception {}
```

- [ ] **Step 2: Run the Spark IT module and verify the new tests fail against the current lightweight fixture**

Run: `mvn -f connectors/jvm/pom.xml -pl integration-tests-jvm -am -Dtest=SparkReadIT -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL because the current fixture and test assertions do not cover the heavyweight schema or metadata.

- [ ] **Step 3: Add a shared SparkSession helper and shared fixture startup path**

```java
private SparkSession sparkSession(String appName) {
    return SparkSession.builder()
            .master("local[2]")
            .appName(appName)
            .config("spark.ui.enabled", "false")
            .config("spark.driver.extraJavaOptions", SPARK_ARROW_JAVA_OPTS)
            .config("spark.executor.extraJavaOptions", SPARK_ARROW_JAVA_OPTS)
            .getOrCreate();
}
```

- [ ] **Step 4: Implement pushed projection and pushed filter assertions**

```java
Dataset<Row> rows =
        spark.read()
                .format("dal_obscura")
                .option("dal.uri", server.uri())
                .option("dal.catalog", bundle.catalog())
                .option("dal.target", bundle.target())
                .option("dal.auth.token", bundle.userToken())
                .load()
                .select("id", "region", "user.address.zip")
                .filter("region = 'us' AND id >= 2000");

assertEquals(
        bundle.sampleUsEvenIds(),
        rows.orderBy("id").limit(bundle.sampleUsEvenIds().size())
                .select("id")
                .as(Encoders.LONG())
                .collectAsList());
```

- [ ] **Step 5: Implement residual-function filter assertions**

```java
Dataset<Row> rows =
        spark.read()
                .format("dal_obscura")
                .option("dal.uri", server.uri())
                .option("dal.catalog", bundle.catalog())
                .option("dal.target", bundle.target())
                .option("dal.auth.token", bundle.userToken())
                .load()
                .select("id", "user.name")
                .filter("upper(user.name) = 'HOUSE'");

assertEquals(expectedIds, rows.orderBy("id").select("id").as(Encoders.LONG()).collectAsList());
```

- [ ] **Step 6: Implement masking assertions across top-level and nested fields**

```java
Row first =
        spark.read()
                .format("dal_obscura")
                .option("dal.uri", server.uri())
                .option("dal.catalog", bundle.catalog())
                .option("dal.target", bundle.target())
                .option("dal.auth.token", bundle.userToken())
                .load()
                .select("id", "user.email", "user.address.zip", "metadata.preferences")
                .orderBy("id")
                .first();

assertEquals("[hidden]", first.getAs("user.email"));
assertEquals(bundle.maskedZipHashLength(), first.getAs("user.address.zip").length());
```

- [ ] **Step 7: Implement broad-vs-selective planning assertions using fixture metadata and row counts**

```java
long broadCount =
        spark.read()
                .format("dal_obscura")
                .option("dal.uri", server.uri())
                .option("dal.catalog", bundle.catalog())
                .option("dal.target", bundle.target())
                .option("dal.auth.token", bundle.userToken())
                .load()
                .count();

long selectiveCount =
        spark.read()
                .format("dal_obscura")
                .option("dal.uri", server.uri())
                .option("dal.catalog", bundle.catalog())
                .option("dal.target", bundle.target())
                .option("dal.auth.token", bundle.userToken())
                .load()
                .filter("region = 'us' AND tenant_id = 'tenant-03'")
                .count();

assertEquals(bundle.expectedRowCount(), broadCount);
assertTrue(selectiveCount < broadCount);
```

- [ ] **Step 8: Keep and adapt the missing-token failure test**

```java
Exception error =
        assertThrows(
                Exception.class,
                () -> spark.read()
                        .format("dal_obscura")
                        .option("dal.uri", server.uri())
                        .option("dal.catalog", bundle.catalog())
                        .option("dal.target", bundle.target())
                        .load()
                        .count());

assertTrue(String.valueOf(error.getMessage()).contains("Missing required option: dal.auth.token"));
```

- [ ] **Step 9: Run the Spark IT module and verify all focused tests pass**

Run: `mvn -f connectors/jvm/pom.xml -pl integration-tests-jvm -am -Dtest=SparkReadIT -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS with the new focused test suite.

- [ ] **Step 10: Commit the Spark integration refactor**

```bash
git add connectors/jvm/integration-tests-jvm/src/test/java/io/dalobscura/connectors/integration/SparkReadIT.java
git commit -m "test(spark): expand integration coverage"
```

---

## Task 5: Document the Heavyweight Harness and Run Full Verification

**Files:**
- Modify: `connectors/README.md`
- Modify: `README.md`

- [ ] **Step 1: Add connector documentation for the heavyweight Spark harness**

```md
## Spark Integration Harness

The JVM integration suite provisions a large Iceberg-backed fixture through
`tests/support/build_connector_fixture.py`. The fixture includes:

- 100,000+ rows
- nested and complex Arrow types
- multiple masking rule types
- focused Spark ITs for pushed filters, residual filters, masking, and large scans
```

- [ ] **Step 2: Add a short top-level README pointer to connector integration coverage**

```md
### Connectors

The Spark connector integration suite uses a heavyweight Iceberg-backed fixture
to validate nested projection, masking, and mixed pushed/residual filtering
against a real local `dal-obscura` process.
```

- [ ] **Step 3: Run Python helper tests**

Run: `uv run pytest tests/support/test_iceberg.py tests/test_support_build_connector_fixture.py -q`

Expected: PASS.

- [ ] **Step 4: Run the full JVM reactor from a clean build**

Run: `mvn -f connectors/jvm/pom.xml clean verify`

Expected: PASS, including the expanded `SparkReadIT`.

- [ ] **Step 5: Commit docs and final verification changes**

```bash
git add \
  connectors/README.md \
  README.md \
  tests/support/test_iceberg.py \
  tests/test_support_build_connector_fixture.py
git commit -m "docs(connectors): describe heavy Spark fixture"
```

---

## Self-Review

### Spec coverage

- Iceberg-backed heavyweight fixture: covered in Tasks 1 and 2
- reuse of existing Python helpers: covered in Task 1 and enforced in Task 2
- 20+ field complex schema and 100k rows: covered in Task 2
- all masking rule types: covered in Task 2 policy generation and Task 4 masking assertions
- pushed filters and residual filters: covered in Task 4
- multi-ticket / selective planning behavior: covered in Tasks 2, 3, and 4
- Maven/Spark verification from clean build: covered in Task 5

### Placeholder scan

- No `TODO` / `TBD` placeholders remain.
- Each task names exact files, commands, and concrete code snippets.

### Type consistency

- Python helper remains `create_iceberg_table(...)`.
- JVM fixture metadata remains centered on `FixtureBundle`.
- Spark IT entry point remains `SparkReadIT`.

