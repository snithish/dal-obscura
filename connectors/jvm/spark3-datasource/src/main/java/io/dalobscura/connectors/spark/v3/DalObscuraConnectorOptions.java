package io.dalobscura.connectors.spark.v3;

import java.io.Serializable;

public final class DalObscuraConnectorOptions implements Serializable {
    private final String uri;
    private final String catalog;
    private final String target;
    private final String authToken;

    public DalObscuraConnectorOptions(String uri, String catalog, String target, String authToken) {
        this.uri = uri;
        this.catalog = catalog;
        this.target = target;
        this.authToken = authToken;
    }

    public String uri() {
        return uri;
    }

    public String catalog() {
        return catalog;
    }

    public String target() {
        return target;
    }

    public String authToken() {
        return authToken;
    }
}
