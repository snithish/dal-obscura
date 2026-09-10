package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraAuth;
public final class DalObscuraConnectorOptions {
    private final String uri;
    private final String catalog;
    private final String target;
    private final DalObscuraAuth auth;
    private final DalObscuraExecutorAuthProvider executorAuthProvider;

    public DalObscuraConnectorOptions(
            String uri,
            String catalog,
            String target,
            DalObscuraAuth auth,
            DalObscuraExecutorAuthProvider executorAuthProvider) {
        this.uri = uri;
        this.catalog = catalog;
        this.target = target;
        this.auth = auth;
        this.executorAuthProvider = executorAuthProvider;
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

    public DalObscuraExecutorAuthProvider executorAuthProvider() {
        return executorAuthProvider;
    }
}
