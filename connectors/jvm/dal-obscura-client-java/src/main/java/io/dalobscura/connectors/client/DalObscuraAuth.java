package io.dalobscura.connectors.client;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class DalObscuraAuth implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String AUTHORIZATION_HEADER = "authorization";

    private final Map<String, String> headers;

    public DalObscuraAuth(Map<String, String> headers) {
        LinkedHashMap<String, String> normalized = new LinkedHashMap<>();
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                String name = normalizeHeaderName(entry.getKey());
                String value = entry.getValue();
                if (name == null || value == null || value.isBlank()) {
                    continue;
                }
                normalized.put(name, value);
            }
        }
        this.headers = Collections.unmodifiableMap(normalized);
    }

    public static DalObscuraAuth bearerToken(String token) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("auth token must not be blank");
        }
        return new DalObscuraAuth(Map.of(AUTHORIZATION_HEADER, "Bearer " + token));
    }

    public Map<String, String> headers() {
        return headers;
    }

    public String header(String name) {
        String normalizedName = normalizeHeaderName(name);
        if (normalizedName == null) {
            return null;
        }
        return headers.get(normalizedName);
    }

    public boolean isEmpty() {
        return headers.isEmpty();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DalObscuraAuth)) {
            return false;
        }
        DalObscuraAuth that = (DalObscuraAuth) other;
        return headers.equals(that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(headers);
    }

    @Override
    public String toString() {
        return "DalObscuraAuth{" + "headers=" + headers + '}';
    }

    private static String normalizeHeaderName(String name) {
        if (name == null || name.isBlank()) {
            return null;
        }
        return name.toLowerCase(Locale.ROOT);
    }
}
