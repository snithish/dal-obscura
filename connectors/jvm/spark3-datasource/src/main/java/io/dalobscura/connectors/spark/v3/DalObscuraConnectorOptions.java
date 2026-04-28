package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraAuth;
import java.io.Serializable;

public final class DalObscuraConnectorOptions implements Serializable {
    private final String uri;
    private final String catalog;
    private final String target;
    private final DalObscuraAuth auth;

    public DalObscuraConnectorOptions(String uri, String catalog, String target, DalObscuraAuth auth) {
        this.uri = uri;
        this.catalog = catalog;
        this.target = target;
        this.auth = auth;
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

    public DalObscuraAuth auth() {
        return auth;
    }
}
