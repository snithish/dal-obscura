# Connectors

This workspace hosts engine-specific clients that read through `dal-obscura`.

- `contract-fixtures/`: language-neutral compatibility cases
- `jvm/dal-obscura-client-java/`: Java Flight read client
- `jvm/spark3-datasource/`: Spark 3.x DataSource V2 adapter
- `jvm/connector-testkit-jvm/`: shared JVM integration helpers
- `jvm/integration-tests-jvm/`: end-to-end Spark integration tests

## Spark 3.x Connector

Build and verify the JVM workspace with:

```bash
mvn -f connectors/jvm/pom.xml verify
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

`dal.auth.token` is a convenience option that becomes
`Authorization: Bearer <token>`. For API keys, gateway-injected headers, or any
other header-based scheme, pass explicit headers instead:

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

Auth headers are optional at the connector boundary. That allows deployments
that authenticate with mTLS peer identity or another transport-level mechanism.

Session-level fallback options use Spark's datasource prefix:

```java
spark.conf().set("spark.datasource.dal_obscura.uri", "grpc+tcp://localhost:8815");
spark.conf().set("spark.datasource.dal_obscura.catalog", "analytics");
spark.conf().set("spark.datasource.dal_obscura.target", "default.users");
spark.conf().set("spark.datasource.dal_obscura.auth.token", token);
spark.conf().set("spark.datasource.dal_obscura.auth.header.x-api-key", apiKey);
```

The Spark connector is read-only in v1. Planning, authn/authz, row-filter
validation, masking, and ticket minting remain in `dal-obscura`; the connector
only requests plans and executes the returned tickets.
