package io.dalobscura.connectors.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.dalobscura.flight.v1.DalObscuraFlightProto;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.flight.FlightCallHeaders;
import org.junit.jupiter.api.Test;

class FlightDalObscuraReadClientTest {
    @Test
    void encodesPlanRequestsAsProtobufContract() throws Exception {
        DalObscuraPlanRequest request =
                new DalObscuraPlanRequest(
                        "analytics",
                        "default.users",
                        List.of("id", "email"),
                        Optional.of("region = 'us'"));

        byte[] payload = FlightDalObscuraReadClient.encodePlanCommand(request);
        DalObscuraFlightProto.PlanRequest decoded =
                DalObscuraFlightProto.PlanRequest.parseFrom(payload);

        assertEquals(FlightDalObscuraReadClient.PROTOCOL_VERSION, decoded.getProtocolVersion());
        assertEquals("analytics", decoded.getCatalog());
        assertEquals("default.users", decoded.getTarget());
        assertEquals(List.of("id", "email"), decoded.getColumnsList());
        assertEquals("region = 'us'", decoded.getRowFilter());
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

}
