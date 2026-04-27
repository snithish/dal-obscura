# Connectors

This workspace hosts engine-specific clients that read through `dal-obscura`.

- `contract-fixtures/`: language-neutral compatibility cases
- `jvm/dal-obscura-client-java/`: Java Flight read client
- `jvm/spark3-datasource/`: Spark 3.x DataSource V2 adapter
- `jvm/connector-testkit-jvm/`: shared JVM integration helpers
- `jvm/integration-tests-jvm/`: end-to-end Spark integration tests

## Protocol v1

All connectors use the same read contract:

- transport: Arrow Flight
- command payload: JSON `FlightDescriptor.command`
- required command fields: `protocol_version: 1`, `catalog`, `target`, `columns`
- optional command field: `row_filter`, rendered as validated DuckDB SQL
- auth: `Authorization: Bearer <token>` on schema, plan, and fetch calls
- result: Arrow schema from `get_schema`/`get_flight_info`, one opaque ticket per endpoint, and Arrow record batches from `do_get`

Missing `protocol_version` is accepted as v1 for compatibility with early clients.
Unsupported versions are rejected server-side.

## Spark 3.x Connector

Build and verify the JVM workspace with:

```bash
mvn -f connectors/jvm/pom.xml -Pspark-3.5 verify
```

Spark 4 is tracked as a separate compatibility lane because Spark 4.x uses the
Scala 2.13 artifact line and JDK 17 baseline:

```bash
mvn -f connectors/jvm/pom.xml -Pspark-4.0 verify
```

Use the datasource through the standard Spark read path:

```java
spark.read()
     .format("dal_obscura")
     .option("dal.uri", "grpc+tcp://localhost:8815")
     .option("dal.catalog", "analytics")
     .option("dal.target", "default.users")
     .option("dal.auth.token", token)
     .load();
```

Session-level fallback options use Spark's datasource prefix:

```java
spark.conf().set("spark.datasource.dal_obscura.uri", "grpc+tcp://localhost:8815");
spark.conf().set("spark.datasource.dal_obscura.catalog", "analytics");
spark.conf().set("spark.datasource.dal_obscura.target", "default.users");
spark.conf().set("spark.datasource.dal_obscura.auth.token", token);
```

The Spark connector is read-only in v1. Planning, authn/authz, row-filter
validation, masking, and ticket minting remain in `dal-obscura`; the connector
only requests plans and executes the returned tickets.

## Python SDK and DuckDB

The Python SDK proof point lives in `dal_obscura.connectors.python_sdk`.
It exposes schema, plan, batch, table, and DuckDB relation helpers over the same
protocol v1 Flight contract used by Spark.
