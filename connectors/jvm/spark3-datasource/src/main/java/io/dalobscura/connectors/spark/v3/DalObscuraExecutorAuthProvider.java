package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraAuth;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/** Serializable reference to a credential source; it never contains the credential itself. */
public final class DalObscuraExecutorAuthProvider implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Source source;
    private final String reference;

    private DalObscuraExecutorAuthProvider(Source source, String reference) {
        this.source = Objects.requireNonNull(source, "source");
        if (reference == null || reference.isBlank()) {
            throw new IllegalArgumentException("Executor credential reference must not be blank");
        }
        this.reference = reference;
    }

    public static DalObscuraExecutorAuthProvider fromEnvironment(String variable) {
        return new DalObscuraExecutorAuthProvider(Source.ENVIRONMENT, variable);
    }

    public static DalObscuraExecutorAuthProvider fromSystemProperty(String property) {
        return new DalObscuraExecutorAuthProvider(Source.SYSTEM_PROPERTY, property);
    }

    public DalObscuraAuth resolve() {
        String token = source == Source.ENVIRONMENT
                ? System.getenv(reference)
                : System.getProperty(reference);
        if (token == null || token.isBlank()) {
            throw new IllegalStateException(
                    "Executor bearer token is unavailable from " + source.description + ": " + reference);
        }
        return new DalObscuraAuth(Map.of("authorization", "Bearer " + token));
    }

    public String reference() {
        return reference;
    }

    private enum Source {
        ENVIRONMENT("environment variable"),
        SYSTEM_PROPERTY("system property");

        private final String description;

        Source(String description) {
            this.description = description;
        }
    }
}
