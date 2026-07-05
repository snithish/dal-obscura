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

        return new DalObscuraConnectorOptions(uri, catalog, target, resolveAuth(options));
    }

    private static void require(String key, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required option: " + key);
        }
    }

    private static DalObscuraAuth resolveAuth(CaseInsensitiveStringMap options) {
        Map<String, String> rawOptions = options.asCaseSensitiveMap();
        LinkedHashMap<String, String> headers = new LinkedHashMap<>();
        applyBearerToken(headers, rawOptions.get("dal.auth.token"));
        addHeaderOptions(headers, rawOptions, READ_HEADER_PREFIX);
        return new DalObscuraAuth(headers);
    }

    private static void applyBearerToken(Map<String, String> headers, String token) {
        if (token != null && !token.isBlank()) {
            headers.put("authorization", "Bearer " + token);
        }
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
