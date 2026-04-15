package io.dalobscura.connectors.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class FlightDalObscuraReadClientTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void encodesThePlanRequestAsDalObscuraJson() throws Exception {
        DalObscuraPlanRequest request =
                new DalObscuraPlanRequest(
                        "analytics",
                        "default.users",
                        List.of("id", "email"),
                        Optional.of("region = 'us'"));

        byte[] payload = FlightDalObscuraReadClient.encodePlanCommand(request);

        assertEquals(
                "{\"catalog\":\"analytics\",\"target\":\"default.users\",\"columns\":[\"id\",\"email\"],\"row_filter\":\"region = 'us'\"}",
                new String(payload, StandardCharsets.UTF_8));
    }

    @Test
    void omitsRowFilterWhenRequestDoesNotProvideOne() throws Exception {
        DalObscuraPlanRequest request =
                new DalObscuraPlanRequest(
                        "analytics",
                        "default.users",
                        List.of("id", "email"),
                        Optional.empty());

        byte[] payload = FlightDalObscuraReadClient.encodePlanCommand(request);
        Map<?, ?> decoded = MAPPER.readValue(payload, Map.class);

        assertFalse(decoded.containsKey("row_filter"));
    }

    @Test
    void buildsAuthorizationHeaderValueForPlanAndFetch() {
        String headerValue = FlightDalObscuraReadClient.authorizationHeaderValue("token-123");

        assertEquals("Bearer token-123", headerValue);
    }

    @Test
    void supportsBothSecureAndInsecureFlightUris() {
        assertDoesNotThrow(() -> FlightDalObscuraReadClient.locationFor("grpc+tcp://localhost:8815"));
        assertDoesNotThrow(() -> FlightDalObscuraReadClient.locationFor("grpc+tls://localhost:8815"));
    }

    @Test
    void mapsFlightEndpointsIntoOpaquePartitions() {
        DalObscuraPlannedRead read =
                new DalObscuraPlannedRead(null, List.of(new DalObscuraPlannedPartition("ticket-a")));

        assertEquals(1, read.partitions().size());
        assertEquals("ticket-a", read.partitions().get(0).ticket());
        assertTrue(read.partitions().get(0).locations().isEmpty());
    }
}
