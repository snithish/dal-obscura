package io.dalobscura.connectors.spark.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
                                        "dal.auth.header.x-api-key", "read-secret",
                                        "uri", "grpc+tcp://session-option:8815",
                                        "auth.token", "session-token",
                                        "auth.header.x-api-key", "session-secret")));

        assertEquals("grpc+tcp://read-option:8815", options.uri());
        assertEquals("analytics", options.catalog());
        assertEquals("default.users", options.target());
        assertEquals("Bearer read-token", options.auth().header("authorization"));
        assertEquals("read-secret", options.auth().header("x-api-key"));
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
                                        "auth.header.x-api-key", "session-secret")));

        assertEquals("grpc+tcp://session-option:8815", options.uri());
        assertEquals("analytics", options.catalog());
        assertEquals("default.users", options.target());
        assertEquals("session-secret", options.auth().header("x-api-key"));
    }

    @Test
    void letsTransportLevelAuthenticationRunWithoutHeaders() {
        DalObscuraConnectorOptions options =
                resolver.resolve(
                        new CaseInsensitiveStringMap(
                                Map.of(
                                        "dal.uri", "grpc+tcp://localhost:8815",
                                        "dal.catalog", "analytics",
                                        "dal.target", "default.users")));

        assertNull(options.auth().header("authorization"));
        assertNull(options.auth().header("x-api-key"));
    }

    @Test
    void explicitAuthorizationHeaderOverridesTokenConvenience() {
        DalObscuraConnectorOptions options =
                resolver.resolve(
                        new CaseInsensitiveStringMap(
                                Map.of(
                                        "dal.uri", "grpc+tcp://localhost:8815",
                                        "dal.catalog", "analytics",
                                        "dal.target", "default.users",
                                        "dal.auth.token", "read-token",
                                        "dal.auth.header.authorization", "ApiKey read-secret")));

        assertEquals("ApiKey read-secret", options.auth().header("authorization"));
    }
}
