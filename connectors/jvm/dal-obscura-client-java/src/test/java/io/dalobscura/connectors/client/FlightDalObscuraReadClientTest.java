package io.dalobscura.connectors.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.dalobscura.flight.v1.DalObscuraFlightProto;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.flight.FlightCallHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
        assertEquals(2, decoded.getColumnPathsCount());
        assertEquals(DalObscuraFlightProto.FieldPathSegment.Kind.FIELD,
                decoded.getColumnPaths(0).getSegments(0).getKind());
        assertEquals("id", decoded.getColumnPaths(0).getSegments(0).getName());
    }

    @Test
    void encodesQuotedAndCollectionPathsAsTypedProtobuf() throws Exception {
        DalObscuraPlanRequest request =
                new DalObscuraPlanRequest(
                        "analytics",
                        "default.users",
                        List.of("[\"profile.name\"]", "contacts.$element.email", "tags.$value.label"),
                        Optional.empty());

        DalObscuraFlightProto.PlanRequest decoded = DalObscuraFlightProto.PlanRequest.parseFrom(
                FlightDalObscuraReadClient.encodePlanCommand(request));

        assertEquals("profile.name", decoded.getColumnPaths(0).getSegments(0).getName());
        assertEquals(DalObscuraFlightProto.FieldPathSegment.Kind.LIST_ELEMENT,
                decoded.getColumnPaths(1).getSegments(1).getKind());
        assertEquals(DalObscuraFlightProto.FieldPathSegment.Kind.MAP_VALUE,
                decoded.getColumnPaths(2).getSegments(1).getKind());
    }

    @ParameterizedTest(name = "rejects canonical path {0}")
    @ValueSource(strings = {"profile..name", "profile.$unknown"})
    void rejectsAmbiguousOrInvalidCanonicalPaths(String path) {
        assertThrows(IllegalArgumentException.class, () -> DalObscuraFieldPath.parse(path));
    }

    @Test
    void snapshotsCallerColumnsBeforeEncoding() throws Exception {
        List<String> columns = new ArrayList<>(List.of("id"));
        DalObscuraPlanRequest request =
                new DalObscuraPlanRequest("analytics", "default.users", columns, Optional.empty());
        columns.set(0, "email");

        DalObscuraFlightProto.PlanRequest decoded = DalObscuraFlightProto.PlanRequest.parseFrom(
                FlightDalObscuraReadClient.encodePlanCommand(request));

        assertEquals(List.of("id"), decoded.getColumnsList());
        assertEquals("id", decoded.getColumnPaths(0).getSegments(0).getName());
    }

    @Test
    void omitsTypedPathsForWildcardSchemaDiscovery() throws Exception {
        DalObscuraPlanRequest request =
                new DalObscuraPlanRequest("analytics", "default.users", List.of("*"), Optional.empty());

        DalObscuraFlightProto.PlanRequest decoded = DalObscuraFlightProto.PlanRequest.parseFrom(
                FlightDalObscuraReadClient.encodePlanCommand(request));

        assertEquals(List.of("*"), decoded.getColumnsList());
        assertEquals(0, decoded.getColumnPathsCount());
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
