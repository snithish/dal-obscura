package io.dalobscura.connectors.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

class SparkReadIT {
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
            assertNotEquals("ACCT-000000000016", topLevelRow.getString(4));
            assertTrue(topLevelRow.getString(4).endsWith("0016"));
            assertEquals(fixture.bundle().maskedZipHashLength(), nestedRow.getString(0).length());
            assertTrue(nestedRow.getString(0).matches("[0-9a-f]+"));
        }
    }

    @Test
    void usesBroadAndSelectivePlanningAppropriatelyForTheHeavyFixture() throws Exception {
        try (SparkFixture fixture = SparkFixture.create("spark-broad-planning-it")) {
            Dataset<Row> broad = fixture.read().filter("market IS NOT NULL");
            long broadCount = broad.count();

            assertEquals(41_666L, broadCount);
        }

        try (SparkFixture fixture = SparkFixture.create("spark-selective-planning-it")) {
            Dataset<Row> selective = fixture.read().filter("market = 'enterprise'");
            long selectiveCount = selective.count();

            assertEquals(16_666L, selectiveCount);
        }
    }

    @Test
    void readsUsingExplicitAuthorizationHeaderOption() throws Exception {
        try (SparkFixture fixture = SparkFixture.create("spark-auth-header-it")) {
            long matchingRows =
                    fixture.readWithAuthorizationHeader()
                            .filter("market = 'enterprise'")
                            .count();

            assertEquals(16_666L, matchingRows);
        }
    }

    @Test
    void failsClearlyWhenNoAuthHeadersAreProvided() throws Exception {
        try (SparkFixture fixture = SparkFixture.create("spark-auth-it")) {
            Exception error =
                    assertThrows(
                            Exception.class,
                            () -> fixture.readWithoutToken().count());

            String message = error.getMessage() == null ? "" : error.getMessage();
            assertTrue(
                    message.contains("Unauthorized") || message.contains("UNAUTHENTICATED"),
                    "expected server-side auth failure but got: " + message);
        }
    }

}
