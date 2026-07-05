# Connector Workspace

This workspace hosts language and engine connectors for the dal-obscura Flight
read contract. For user-facing connector selection, see
[`docs/connectors.md`](../docs/connectors.md).

## Contents

- [Modules](#modules)
- [Protocol v1](#protocol-v1)
- [Spark 3.x Connector](#spark-3x-connector)
- [Spark 4.x Verification](#spark-4x-verification)
- [Python SDK](#python-sdk)

## Modules

| Path | Purpose |
| --- | --- |
| `contract-fixtures/` | Language-neutral connector contract cases. |
| `jvm/dal-obscura-client-java/` | Java Flight read client. |
| `jvm/spark3-datasource/` | Spark DataSource V2 adapter. |
| `jvm/connector-testkit-jvm/` | Shared JVM integration helpers. |
| `jvm/integration-tests-jvm/` | End-to-end Spark integration tests. |

## Protocol v1

All connectors use the same read contract:

| Item | Contract |
| --- | --- |
| Transport | Arrow Flight. |
| Command payload | Protobuf `dal_obscura.flight.v1.PlanRequest` in `FlightDescriptor.command`. |
| Required fields | `protocol_version: 1`, `catalog`, `target`, `columns`. |
| Optional fields | `row_filter`, rendered as validated DuckDB SQL. |
| Auth | Usually `Authorization: Bearer <token>` on schema, plan, and fetch calls. |
| Result | Arrow schema from `get_schema` or `get_flight_info`; opaque tickets from `get_flight_info`; Arrow batches from `do_get`. |

Missing or unsupported `protocol_version` values are rejected server-side.

Nested entries in `columns`, such as `user.address.zip`, are returned as pruned
nested Arrow structs rooted at `user`, not as dotted top-level fields.

Tickets are opaque. Connectors must not assume they can mutate or replay the
original plan request during streaming.

## Spark 3.x Connector

Build and verify the default JVM workspace:

```bash
mvn -f connectors/jvm/pom.xml verify
```

Verify explicitly with the Spark 3.5 profile:

```bash
mvn -f connectors/jvm/pom.xml -Pspark-3.5 verify
```

Read through Spark:

```java
spark.read()
     .format("dal_obscura")
     .option("dal.uri", "grpc+tcp://localhost:8815")
     .option("dal.catalog", "analytics")
     .option("dal.target", "default.users")
     .option("dal.auth.token", token)
     .load();
```

`dal.auth.token` becomes `Authorization: Bearer <token>`.

For API keys, gateway-injected headers, or other header-based schemes, pass
explicit headers:

```java
spark.read()
     .format("dal_obscura")
     .option("dal.uri", "grpc+tcp://localhost:8815")
     .option("dal.catalog", "analytics")
     .option("dal.target", "default.users")
     .option("dal.auth.header.authorization", "Bearer " + token)
     .option("dal.auth.header.x-api-key", apiKey)
     .load();
```

Auth headers are optional at the connector boundary for deployments that
authenticate with mTLS peer identity or another transport-level mechanism.

The Spark connector is read-only. Planning, authn/authz, row-filter validation,
masking, and ticket minting remain in dal-obscura.

## Spark 4.x Verification

Spark 4.x uses the Scala 2.13 artifact line and JDK 17 baseline. Verify with:

```bash
mvn -f connectors/jvm/pom.xml -Pspark-4.0 verify
```

## Python SDK

The Python SDK lives in `dal_obscura.connectors.python_sdk`. It exposes schema,
plan, batch, table, and DuckDB relation helpers over the same protocol v1 Flight
contract used by Spark.
