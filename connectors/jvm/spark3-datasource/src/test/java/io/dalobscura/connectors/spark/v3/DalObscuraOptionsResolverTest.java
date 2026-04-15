package io.dalobscura.connectors.spark.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

class DalObscuraOptionsResolverTest {
    private final DalObscuraOptionsResolver resolver = new DalObscuraOptionsResolver();

    @Test
    void prefersReadOptionsOverSessionConf() {
        DalObscuraConnectorOptions options =
                resolver.resolve(
                        new CaseInsensitiveStringMap(
                                Map.of(
                                        "dal.uri", "grpc+tcp://read-option:8815",
                                        "dal.catalog", "analytics",
                                        "dal.target", "default.users",
                                        "dal.auth.token", "read-token",
                                        "uri", "grpc+tcp://session-option:8815",
                                        "auth.token", "session-token")));

        assertEquals("grpc+tcp://read-option:8815", options.uri());
        assertEquals("analytics", options.catalog());
        assertEquals("default.users", options.target());
        assertEquals("read-token", options.authToken());
    }

    @Test
    void fallsBackToSessionConfWhenReadOptionMissing() {
        DalObscuraConnectorOptions options =
                resolver.resolve(
                        new CaseInsensitiveStringMap(
                                Map.of(
                                        "dal.target", "default.users",
                                        "uri", "grpc+tcp://session-option:8815",
                                        "catalog", "analytics",
                                        "auth.token", "session-token")));

        assertEquals("grpc+tcp://session-option:8815", options.uri());
        assertEquals("analytics", options.catalog());
        assertEquals("default.users", options.target());
        assertEquals("session-token", options.authToken());
    }

    @Test
    void rejectsMissingToken() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                resolver.resolve(
                                        new CaseInsensitiveStringMap(
                                                Map.of(
                                                        "dal.uri", "grpc+tcp://localhost:8815",
                                                        "dal.catalog", "analytics",
                                                        "dal.target", "default.users"))));

        assertEquals("Missing required option: dal.auth.token", error.getMessage());
    }
}
