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
import org.apache.arrow.flight.FlightCallHeaders;
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
                "{\"protocol_version\":1,\"catalog\":\"analytics\",\"target\":\"default.users\",\"columns\":[\"id\",\"email\"],\"row_filter\":\"region = 'us'\"}",
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
    void includesProtocolVersionInPlanCommands() throws Exception {
        DalObscuraPlanRequest request =
                new DalObscuraPlanRequest(
                        "analytics",
                        "default.users",
                        List.of("id"),
                        Optional.empty());

        byte[] payload = FlightDalObscuraReadClient.encodePlanCommand(request);
        Map<?, ?> decoded = MAPPER.readValue(payload, Map.class);

        assertEquals(1, decoded.get("protocol_version"));
    }

    @Test
    void buildsBearerAuthorizationHeadersFromTokenConvenience() {
        DalObscuraAuth auth = DalObscuraAuth.bearerToken("token-123");

        assertEquals(Map.of("authorization", "Bearer token-123"), auth.headers());
    }

    @Test
    void buildsFlightHeadersFromGenericAuthHeaders() {
        FlightCallHeaders headers =
                FlightDalObscuraReadClient.flightHeaders(
                        new DalObscuraAuth(
                                Map.of(
                                        "Authorization", "Bearer token-123",
                                        "X-Api-Key", "secret-1")));

        assertEquals("Bearer token-123", headers.get("authorization"));
        assertEquals("secret-1", headers.get("x-api-key"));
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
