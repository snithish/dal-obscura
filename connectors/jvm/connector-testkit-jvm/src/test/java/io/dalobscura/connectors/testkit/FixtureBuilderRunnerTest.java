package io.dalobscura.connectors.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class FixtureBuilderRunnerTest {
    @Test
    void rejectsMalformedExpectedMetadataInsteadOfCoercingIt() throws Exception {
        String payload =
                "{"
                        + "\"uri\":\"grpc+tcp://localhost:31337\","
                        + "\"catalog\":\"spark_catalog\","
                        + "\"target\":\"default.complex_users\","
                        + "\"user_token\":\"token\","
                        + "\"database_url\":\"sqlite+pysqlite:////tmp/control-plane.db\","
                        + "\"cell_id\":\"3c6f4e88-1e15-448c-9b72-24f98d171017\","
                        + "\"jwt_secret\":\"jwt\","
                        + "\"ticket_secret\":\"ticket\","
                        + "\"expected\":{"
                        + "\"row_count\":\"125000\","
                        + "\"supports_multiple_tickets\":true,"
                        + "\"sample_us_even_ids\":[0,4,8],"
                        + "\"masked_zip_hash_length\":64"
                        + "}"
                        + "}";

        assertThrows(
                IllegalStateException.class,
                () -> FixtureBuilderRunner.parseFixturePayload(payload),
                "strict parser should reject string row_count");
    }

    @Test
    void exposesExpectedMetadataFromThePythonFixturePayload() throws Exception {
        FixtureBundle bundle = FixtureBuilderRunner.build();

        assertTrue(bundle.databaseUrl().startsWith("sqlite+pysqlite:///"));
        assertTrue(bundle.cellId().matches("[0-9a-f-]{36}"));
        assertEquals(125_000L, bundle.expectedRowCount());
        assertTrue(bundle.supportsMultipleTickets());
        assertEquals(List.of(0L, 4L, 8L), bundle.sampleUsEvenIds());
        assertEquals(64, bundle.maskedZipHashLength());
    }
}
