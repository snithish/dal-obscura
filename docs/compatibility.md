# Compatibility

The gateway protocol is custom Arrow Flight. Generic Flight, Flight SQL, JDBC,
or ADBC clients are not compatible unless they implement this project's plan
and ticket contract.

## Verified matrix

| Surface | Version | Evidence | Status |
| --- | --- | --- | --- |
| Python SDK | Python 3.12 | CI `python-quality`, contract and integration lanes | supported |
| PyArrow / DuckDB | locked project dependencies | Python connector and transformation suites | supported for tested reads |
| Polars | 1.44.2 | `tests/connectors/test_python_sdk.py` | supported through explicit Arrow-table materialization |
| Java client | Java 17 runtime; Java 11 bytecode target | `mvn -f connectors/jvm/pom.xml verify` | supported for tested Flight reads |
| Spark DataSource V2 | Spark 3.5.6, Scala 2.12, Java 17 | local `SparkReadIT` in the Maven reactor | supported only for the tested local read path |

The project and published plugin metadata support Python 3.12 only (`>=3.12,<3.13`),
matching CI and the production image. The Maven `spark-4.0` profile is a build profile, not
a support claim: it needs its own complete integration run before release.

## Consumer semantics

`DalObscuraClient.read_batches()` is the Python streaming API. `read_table()`
and `read_polars()` intentionally materialize the full authorized result.
The Polars conversion is not lazy and does not add client-side predicate
pushdown.

DuckDB receives an Arrow record-batch reader. Its own joins, sorts, and
aggregates may materialize data after the gateway boundary.

Spark maps the tested Arrow types as follows: signed 32/64-bit integers to
Spark integer/long, booleans, UTF-8 strings, binary, date, timestamp,
precision-preserving decimal, float/double, structs, lists, and maps. Unknown
Arrow types fail with a capability error; the connector does not stringify
unsupported data. Timestamp timezone semantics, UUID/fixed binary, Spark 4,
distributed executor credential refresh, retry/speculation attempt issuance,
and zero-column reads are not yet supported pilot cells.

Each Flight ticket is single-use. Re-running a materialized Spark dataset is
not a retry protocol; it needs the planned attempt-issuance work before it can
be treated as supported distributed retry behavior.
