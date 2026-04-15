package io.dalobscura.connectors.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dalobscura.connectors.testkit.FixtureBundle;
import io.dalobscura.connectors.testkit.FixtureBuilderRunner;
import io.dalobscura.connectors.testkit.LocalDalObscuraServer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class SparkReadIT {
    private static final String SPARK_ARROW_JAVA_OPTS =
            "-XX:+IgnoreUnrecognizedVMOptions "
                    + "--add-opens=java.base/java.lang=ALL-UNNAMED "
                    + "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                    + "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                    + "--add-opens=java.base/java.io=ALL-UNNAMED "
                    + "--add-opens=java.base/java.net=ALL-UNNAMED "
                    + "--add-opens=java.base/java.nio=ALL-UNNAMED "
                    + "--add-opens=java.base/java.util=ALL-UNNAMED "
                    + "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                    + "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                    + "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED "
                    + "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                    + "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
                    + "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
                    + "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
                    + "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED "
                    + "-Djdk.reflect.useDirectMethodHandle=false "
                    + "-Dio.netty.tryReflectionSetAccessible=true";
    @Test
    void readsNestedProjectionWithPushedFilters() throws Exception {
        try (SparkFixture fixture = SparkFixture.create("spark-nested-projection-it")) {
            List<Row> rows =
                    fixture.read()
                            .filter("market = 'enterprise'")
                            .filter("active = true")
                            .filter("created_at = TIMESTAMP '2024-01-01 12:16:00'")
                            .selectExpr(
                                    "support_ticket.channel AS ticket_channel",
                                    "account.manager.region AS manager_region")
                            .collectAsList();

            assertEquals(1, rows.size());
            assertEquals("chat", rows.get(0).getString(0));
            assertEquals("amer", rows.get(0).getString(1));
        }
    }

    @Test
    void appliesResidualFunctionFiltersInSpark() throws Exception {
        try (SparkFixture fixture = SparkFixture.create("spark-residual-filter-it")) {
            long matchingRows =
                    fixture.read()
                            .filter("market IN ('enterprise', 'partner')")
                            .filter("lower(market) = 'enterprise'")
                            .filter("created_at < TIMESTAMP '2024-01-01 12:30:00'")
                            .count();

            assertEquals(4L, matchingRows);
        }
    }

    @Test
    void returnsMaskedTopLevelAndNestedFields() throws Exception {
        try (SparkFixture fixture = SparkFixture.create("spark-masked-fields-it")) {
            Row topLevelRow =
                    fixture.read()
                            .filter("created_at = TIMESTAMP '2024-01-01 12:16:00'")
                            .selectExpr(
                                    "email AS masked_email",
                                    "notes AS redacted_note",
                                    "status AS default_status",
                                    "nickname",
                                    "account_number")
                            .head();

            Row nestedRow =
                    fixture.read()
                            .filter("created_at = TIMESTAMP '2024-01-01 12:16:00'")
                            .selectExpr("support_ticket.ticket_id AS masked_ticket_id")
                            .head();

            assertEquals("u***@example.com", topLevelRow.getString(0));
            assertEquals("[redacted-note]", topLevelRow.getString(1));
            assertEquals("partner-visible", topLevelRow.getString(2));
            assertNull(topLevelRow.get(3));
            assertNotEquals(originalAccountNumber(16L), topLevelRow.getString(4));
            assertTrue(topLevelRow.getString(4).endsWith("0016"));
            assertEquals(fixture.bundle().maskedZipHashLength(), nestedRow.getString(0).length());
            assertTrue(nestedRow.getString(0).matches("[0-9a-f]+"));
        }
    }

    @Test
    void usesBroadAndSelectivePlanningAppropriatelyForTheHeavyFixture() throws Exception {
        try (SparkFixture fixture = SparkFixture.create("spark-planning-it")) {
            assertEquals(125_000L, fixture.bundle().expectedRowCount());
            assertTrue(fixture.bundle().supportsMultipleTickets());

            Dataset<Row> broad = fixture.read().filter("market IS NOT NULL");
            Dataset<Row> selective =
                    fixture.read()
                            .filter("market = 'enterprise'");

            long broadCount = broad.count();
            long selectiveCount = selective.count();
            int broadPartitions = broad.javaRDD().getNumPartitions();
            int selectivePartitions = selective.javaRDD().getNumPartitions();

            assertEquals(countPolicyVisibleRows(fixture.bundle().expectedRowCount()), broadCount);
            assertEquals(countPolicyVisibleEnterpriseRows(fixture.bundle().expectedRowCount()), selectiveCount);
            assertTrue(broadPartitions > 1, "expected multiple Spark partitions for the broad scan");
            assertTrue(
                    selectivePartitions < broadPartitions,
                    "expected the selective read to plan fewer Spark partitions");
        }
    }

    @Test
    void failsClearlyWhenTheTokenIsMissing() throws Exception {
        try (SparkFixture fixture = SparkFixture.create("spark-auth-it")) {
            Exception error =
                    assertThrows(
                            Exception.class,
                            () -> fixture.readWithoutToken().count());

            String message = error.getMessage() == null ? "" : error.getMessage();
            assertTrue(message.contains("Missing required option: dal.auth.token"));
        }
    }

    private static long countPolicyVisibleRows(long upperBoundExclusive) {
        long count = 0L;
        for (long rowId = 0L; rowId < upperBoundExclusive; rowId++) {
            if (rowId % 2 == 0 && rowId % 3 != 0) {
                count++;
            }
        }
        return count;
    }

    private static long countPolicyVisibleEnterpriseRows(long upperBoundExclusive) {
        long count = 0L;
        for (long rowId = 0L; rowId < upperBoundExclusive; rowId++) {
            if (rowId % 2 == 0 && rowId % 3 != 0 && (rowId % 5 == 0 || rowId % 5 == 1)) {
                count++;
            }
        }
        return count;
    }

    private static String originalAccountNumber(long rowId) {
        return String.format("ACCT-%012d", rowId);
    }

    private static final class SparkFixture implements AutoCloseable {
        private final FixtureBundle bundle;
        private final LocalDalObscuraServer server;
        private final SparkSession spark;

        private SparkFixture(FixtureBundle bundle, LocalDalObscuraServer server, SparkSession spark) {
            this.bundle = bundle;
            this.server = server;
            this.spark = spark;
        }

        static SparkFixture create(String appName) throws Exception {
            FixtureBundle bundle = FixtureBuilderRunner.build();
            LocalDalObscuraServer server = LocalDalObscuraServer.start(bundle);
            SparkSession spark =
                    SparkSession.builder()
                            .master("local[2]")
                            .appName(appName)
                            .config("spark.ui.enabled", "false")
                            .config("spark.driver.extraJavaOptions", SPARK_ARROW_JAVA_OPTS)
                            .config("spark.executor.extraJavaOptions", SPARK_ARROW_JAVA_OPTS)
                            .getOrCreate();
            return new SparkFixture(bundle, server, spark);
        }

        FixtureBundle bundle() {
            return bundle;
        }

        Dataset<Row> read() {
            return reader(true).load();
        }

        Dataset<Row> readWithoutToken() {
            return reader(false).load();
        }

        private DataFrameReader reader(boolean includeToken) {
            DataFrameReader reader =
                    spark.read()
                            .format("dal_obscura")
                            .option("dal.uri", server.uri())
                            .option("dal.catalog", bundle.catalog())
                            .option("dal.target", bundle.target());
            if (includeToken) {
                reader = reader.option("dal.auth.token", bundle.userToken());
            }
            return reader;
        }

        @Override
        public void close() throws Exception {
            try {
                spark.close();
            } finally {
                server.close();
            }
        }
    }
}
