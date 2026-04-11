package io.dalobscura.connectors.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class FixtureBuilderRunnerTest {
    @Test
    void exposesExpectedMetadataFromThePythonFixturePayload() throws Exception {
        FixtureBundle bundle = FixtureBuilderRunner.build();

        assertEquals(125_000L, bundle.expectedRowCount());
        assertTrue(bundle.supportsMultipleTickets());
        assertEquals(List.of(0L, 2L, 4L), bundle.sampleUsEvenIds());
        assertEquals(64, bundle.maskedZipHashLength());
    }
}
