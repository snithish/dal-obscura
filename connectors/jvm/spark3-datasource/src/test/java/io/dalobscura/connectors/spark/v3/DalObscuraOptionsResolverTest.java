package io.dalobscura.connectors.spark.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DalObscuraOptionsResolverTest {
    private final DalObscuraOptionsResolver resolver = new DalObscuraOptionsResolver();

    private static Map<String, String> readOptions() {
        return new HashMap<>(Map.of(
                "dal.uri", "grpc+tcp://localhost:8815",
                "dal.catalog", "analytics",
                "dal.target", "default.users",
                "dal.executor.auth.token-env", "DAL_OBSCURA_TOKEN"));
    }

    @Test
    void resolvesReadOptions() {
        Map<String, String> values = readOptions();
        values.put("dal.uri", "grpc+tcp://read-option:8815");
        values.put("dal.auth.token", "read-token");
        values.put("dal.auth.header.x-api-key", "read-secret");

        DalObscuraConnectorOptions options = resolver.resolve(new CaseInsensitiveStringMap(values));

        assertEquals("grpc+tcp://read-option:8815", options.uri());
        assertEquals("analytics", options.catalog());
        assertEquals("default.users", options.target());
        assertEquals("Bearer read-token", options.auth().header("authorization"));
        assertEquals("read-secret", options.auth().header("x-api-key"));
        assertEquals("DAL_OBSCURA_TOKEN", options.executorAuthProvider().reference());
    }

    @Test
    void rejectsUnprefixedOptions() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> resolver.resolve(new CaseInsensitiveStringMap(Map.of(
                        "uri", "grpc+tcp://session-option:8815", "catalog", "analytics",
                        "target", "default.users", "auth.header.x-api-key", "session-secret"))));

        assertEquals("Missing required option: dal.uri", error.getMessage());
    }

    @Test
    void letsTransportLevelAuthenticationRunWithoutHeaders() {
        DalObscuraConnectorOptions options = resolver.resolve(new CaseInsensitiveStringMap(readOptions()));

        assertNull(options.auth().header("authorization"));
        assertNull(options.auth().header("x-api-key"));
    }

    @ParameterizedTest(name = "rejects {0} executor references")
    @MethodSource("invalidExecutorReferences")
    void rejectsMissingOrAmbiguousExecutorCredentialReferences(String scenario, Map<String, String> references) {
        Map<String, String> values = readOptions();
        values.remove("dal.executor.auth.token-env");
        values.putAll(references);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> resolver.resolve(new CaseInsensitiveStringMap(values)));

        assertEquals("Specify exactly one executor credential reference: "
                + "dal.executor.auth.token-env or dal.executor.auth.token-property", error.getMessage());
    }

    private static Stream<Arguments> invalidExecutorReferences() {
        return Stream.of(
                Arguments.of("missing", Map.of()),
                Arguments.of("ambiguous", Map.of("dal.executor.auth.token-env", "DAL_OBSCURA_TOKEN",
                        "dal.executor.auth.token-property", "dal.obscura.token")));
    }

    @Test
    void resolvesDriverBearerTokenFromAReferencedSystemProperty() {
        String key = "dal.obscura.driver.token";
        String previous = System.getProperty(key);
        try {
            System.setProperty(key, "property-token");
            Map<String, String> values = readOptions();
            values.put("dal.auth.token-property", key);

            DalObscuraConnectorOptions options = resolver.resolve(new CaseInsensitiveStringMap(values));

            assertEquals("Bearer property-token", options.auth().header("authorization"));
        } finally {
            if (previous == null) System.clearProperty(key);
            else System.setProperty(key, previous);
        }
    }

    @Test
    void rejectsMultipleDriverBearerTokenSources() {
        Map<String, String> values = readOptions();
        values.put("dal.auth.token", "direct");
        values.put("dal.auth.token-property", "dal.obscura.driver.token");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> resolver.resolve(new CaseInsensitiveStringMap(values)));

        assertEquals("Specify at most one driver bearer-token source: dal.auth.token, "
                + "dal.auth.token-env, or dal.auth.token-property", error.getMessage());
    }

    @Test
    void explicitAuthorizationHeaderOverridesTokenConvenience() {
        Map<String, String> values = readOptions();
        values.put("dal.auth.token", "read-token");
        values.put("dal.auth.header.authorization", "ApiKey read-secret");

        DalObscuraConnectorOptions options = resolver.resolve(new CaseInsensitiveStringMap(values));

        assertEquals("ApiKey read-secret", options.auth().header("authorization"));
    }
}
