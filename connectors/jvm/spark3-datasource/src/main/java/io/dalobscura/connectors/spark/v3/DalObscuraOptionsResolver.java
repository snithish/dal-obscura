package io.dalobscura.connectors.spark.v3;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public final class DalObscuraOptionsResolver {
    public DalObscuraConnectorOptions resolve(CaseInsensitiveStringMap options) {
        String uri = firstNonBlank(options.get("dal.uri"), options.get("uri"));
        String catalog = firstNonBlank(options.get("dal.catalog"), options.get("catalog"));
        String target = firstNonBlank(options.get("dal.target"), options.get("target"));
        String token = firstNonBlank(options.get("dal.auth.token"), options.get("auth.token"));

        require("dal.uri", uri);
        require("dal.catalog", catalog);
        require("dal.target", target);
        require("dal.auth.token", token);

        return new DalObscuraConnectorOptions(uri, catalog, target, token);
    }

    private static String firstNonBlank(String primary, String fallback) {
        return primary != null && !primary.isBlank() ? primary : fallback;
    }

    private static void require(String key, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required option: " + key);
        }
    }
}
