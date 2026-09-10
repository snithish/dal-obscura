package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraAuth;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public final class DalObscuraOptionsResolver {
    private static final String READ_HEADER_PREFIX = "dal.auth.header.";

    public DalObscuraConnectorOptions resolve(CaseInsensitiveStringMap options) {
        String uri = options.get("dal.uri");
        String catalog = options.get("dal.catalog");
        String target = options.get("dal.target");

        require("dal.uri", uri);
        require("dal.catalog", catalog);
        require("dal.target", target);

        return new DalObscuraConnectorOptions(
                uri, catalog, target, resolveAuth(options), resolveExecutorAuthProvider(options));
    }

    private static void require(String key, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required option: " + key);
        }
    }

    private static DalObscuraExecutorAuthProvider resolveExecutorAuthProvider(
            CaseInsensitiveStringMap options) {
        String environment = options.get("dal.executor.auth.token-env");
        String property = options.get("dal.executor.auth.token-property");
        boolean hasEnvironment = environment != null && !environment.isBlank();
        boolean hasProperty = property != null && !property.isBlank();
        if (hasEnvironment == hasProperty) {
            throw new IllegalArgumentException(
                    "Specify exactly one executor credential reference: "
                            + "dal.executor.auth.token-env or dal.executor.auth.token-property");
        }
        return hasEnvironment
                ? DalObscuraExecutorAuthProvider.fromEnvironment(environment)
                : DalObscuraExecutorAuthProvider.fromSystemProperty(property);
    }

    private static DalObscuraAuth resolveAuth(CaseInsensitiveStringMap options) {
        Map<String, String> rawOptions = options.asCaseSensitiveMap();
        LinkedHashMap<String, String> headers = new LinkedHashMap<>();
        applyDriverBearerToken(headers, rawOptions);
        addHeaderOptions(headers, rawOptions, READ_HEADER_PREFIX);
        return new DalObscuraAuth(headers);
    }

    private static void applyDriverBearerToken(
            Map<String, String> headers, Map<String, String> options) {
        String direct = options.get("dal.auth.token");
        String environment = options.get("dal.auth.token-env");
        String property = options.get("dal.auth.token-property");
        int sourceCount = nonBlankCount(direct, environment, property);
        if (sourceCount > 1) {
            throw new IllegalArgumentException(
                    "Specify at most one driver bearer-token source: dal.auth.token, "
                            + "dal.auth.token-env, or dal.auth.token-property");
        }
        if (sourceCount == 0) {
            return;
        }
        String token = isNonBlank(direct)
                ? direct
                : isNonBlank(environment) ? System.getenv(environment) : System.getProperty(property);
        if (!isNonBlank(token)) {
            throw new IllegalStateException("Driver bearer token is unavailable from configured source");
        }
        headers.put("authorization", "Bearer " + token);
    }

    private static int nonBlankCount(String... values) {
        int count = 0;
        for (String value : values) {
            if (isNonBlank(value)) {
                count++;
            }
        }
        return count;
    }

    private static boolean isNonBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static void addHeaderOptions(
            Map<String, String> headers, Map<String, String> options, String prefix) {
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null || value == null || value.isBlank()) {
                continue;
            }
            String normalizedKey = key.toLowerCase(Locale.ROOT);
            if (!normalizedKey.startsWith(prefix)) {
                continue;
            }
            String headerName = key.substring(prefix.length());
            if (headerName.isBlank()) {
                continue;
            }
            headers.put(headerName, value);
        }
    }
}
